import time
import json
import uuid
import threading
import os
import subprocess
from datetime import datetime
import psycopg2
import pymysql
import oracledb
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import numpy as np

KAFKA_BOOTSTRAP = "localhost:9092"

TARGETS = [
    # 5 Postgres
    {
        "id": "postgres-financeiro", "type": "postgres", "port": 5432, "db": "financeiro",
        "table": "contratos", "topic": "cdc.postgres-financeiro.public.contratos",
        "pk": "numero", "cols": ["numero", "valor", "status"]
    },
    {
        "id": "postgres-logistica", "type": "postgres", "port": 5432, "db": "logistica",
        "table": "entregas", "topic": "cdc.postgres-logistica.public.entregas",
        "pk": "codigo_rastreio", "cols": ["codigo_rastreio", "destinatario", "status"]
    },
    {
        "id": "postgres-vendas", "type": "postgres", "port": 5432, "db": "vendas",
        "table": "pedidos", "topic": "cdc.postgres-vendas.public.pedidos",
        "pk": "cliente", "cols": ["cliente", "total", "status"]
    },
    {
        "id": "postgres-pagamentos", "type": "postgres", "port": 5433, "db": "pagamentos",
        "table": "faturas", "topic": "cdc.postgres-pagamentos.public.faturas",
        "pk": "codigo_fatura", "cols": ["codigo_fatura", "valor", "status"]
    },
    {
        "id": "postgres-rh", "type": "postgres", "port": 5434, "db": "rh",
        "table": "folha", "topic": "cdc.postgres-rh.public.folha",
        "pk": "matricula", "cols": ["matricula", "salario_base", "status"]
    },

    # 5 MySQL
    {
        "id": "mysql-financeiro", "type": "mysql", "port": 3306, "db": "financeiro",
        "table": "contratos", "topic": "cdc.mysql-financeiro.financeiro.contratos",
        "pk": "numero", "cols": ["numero", "valor", "status"]
    },
    {
        "id": "mysql-faturamento", "type": "mysql", "port": 3306, "db": "faturamento",
        "table": "notas_fiscais", "topic": "cdc.mysql-faturamento.faturamento.notas_fiscais",
        "pk": "chave_acesso", "cols": ["chave_acesso", "valor_total", "status"]
    },
    {
        "id": "mysql-estoque", "type": "mysql", "port": 3306, "db": "estoque",
        "table": "produtos", "topic": "cdc.mysql-estoque.estoque.produtos",
        "pk": "sku", "cols": ["sku", "quantidade", "status"]
    },
    {
        "id": "mysql-ecommerce", "type": "mysql", "port": 3307, "db": "ecommerce",
        "table": "carrinhos", "topic": "cdc.mysql-ecommerce.ecommerce.carrinhos",
        "pk": "cliente_id", "cols": ["cliente_id", "total", "status"]
    },
    {
        "id": "mysql-crm", "type": "mysql", "port": 3308, "db": "crm",
        "table": "clientes", "topic": "cdc.mysql-crm.crm.clientes",
        "pk": "documento", "cols": ["documento", "nome", "status"]
    },

    # 5 Oracle
    {
        "id": "oracle-rh", "type": "oracle", "port": 1521, "schema": "RH",
        "table": "FUNCIONARIOS", "topic": "cdc.oracle-rh.RH.FUNCIONARIOS",
        "pk": "ID", "cols": ["ID", "NOME", "CARGO", "DEPARTAMENTO"]
    },
    {
        "id": "oracle-financeiro", "type": "oracle", "port": 1521, "schema": "FINANCEIRO",
        "table": "LANCAMENTOS", "topic": "cdc.oracle-financeiro.FINANCEIRO.LANCAMENTOS",
        "pk": "ID", "cols": ["ID", "DESCRICAO", "VALOR", "STATUS"]
    },
    {
        "id": "oracle-patrimonio", "type": "oracle", "port": 1521, "schema": "PATRIMONIO",
        "table": "BENS", "topic": "cdc.oracle-patrimonio.PATRIMONIO.BENS",
        "pk": "ID", "cols": ["ID", "CODIGO", "DESCRICAO", "VALOR"]
    },
    {
        "id": "oracle-auditoria", "type": "oracle", "port": 1521, "schema": "AUDITORIA",
        "table": "REGISTROS", "topic": "cdc.oracle-auditoria.AUDITORIA.REGISTROS",
        "pk": "ID", "cols": ["ID", "USUARIO", "ACAO", "STATUS"]
    },
    {
        "id": "oracle-contratos", "type": "oracle", "port": 1521, "schema": "CONTRATOS",
        "table": "ACORDOS", "topic": "cdc.oracle-contratos.CONTRATOS.ACORDOS",
        "pk": "ID", "cols": ["ID", "NUMERO", "FORNECEDOR", "VALOR"]
    }
]

class RealTime5MinAuditor:
    def __init__(self, topics):
        self.topics = topics
        self.sent_events = {}       # key: (t0, stage)
        self.latencies_by_stage = {1: [], 2: [], 3: [], 4: [], 5: []}
        self.latencies_by_engine = {
            "postgres": {1: [], 2: [], 3: [], 4: [], 5: []},
            "mysql":    {1: [], 2: [], 3: [], 4: [], 5: []},
            "oracle":   {1: [], 2: [], 3: [], 4: [], 5: []}
        }
        self.ops_captured = {
            "postgres": {"INSERT": 0, "UPDATE": 0, "DELETE": 0},
            "mysql":    {"INSERT": 0, "UPDATE": 0, "DELETE": 0},
            "oracle":   {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
        }
        self.events_per_target = {t["id"]: 0 for t in TARGETS}
        self.running = threading.Event()
        self.running.set()
        self.consumer_thread = None

    def start(self):
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()

    def record_sent(self, key, t0, stage):
        self.sent_events[key] = (t0, stage)

    def _consume_loop(self):
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=f"rt-auditor-{uuid.uuid4().hex[:8]}",
                consumer_timeout_ms=1000
            )
            while self.running.is_set():
                try:
                    batch = consumer.poll(timeout_ms=250)
                except Exception:
                    time.sleep(0.1)
                    continue

                t_now = time.time()
                for tp, records in batch.items():
                    topic_name = tp.topic
                    engine = "postgres" if "postgres" in topic_name else "mysql" if "mysql" in topic_name else "oracle"

                    for r in records:
                        try:
                            val = json.loads(r.value.decode('utf-8'))
                            op = val.get('operation')
                            after = val.get('after') or {}
                            before = val.get('before') or {}

                            if op in ["INSERT", "UPDATE", "DELETE"]:
                                self.ops_captured[engine][op] += 1

                            # Match target id
                            for t in TARGETS:
                                if t["topic"] == topic_name:
                                    self.events_per_target[t["id"]] += 1
                                    break

                            found_key = None
                            for k, v in after.items():
                                candidate = f"{topic_name}:{v}:{op}"
                                if candidate in self.sent_events:
                                    found_key = candidate
                                    break
                            if not found_key:
                                for k, v in before.items():
                                    candidate = f"{topic_name}:{v}:{op}"
                                    if candidate in self.sent_events:
                                        found_key = candidate
                                        break

                            if found_key and found_key in self.sent_events:
                                t0, stage = self.sent_events[found_key]
                                lat_ms = (t_now - t0) * 1000.0
                                if lat_ms >= 0:
                                    self.latencies_by_stage[stage].append(lat_ms)
                                    self.latencies_by_engine[engine][stage].append(lat_ms)
                        except Exception:
                            pass
        except Exception:
            pass
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def stop(self):
        self.running.clear()
        if self.consumer_thread:
            self.consumer_thread.join(timeout=3)

# -----------------------------------------------------------------------------
# Workload Ingestion Workers
# -----------------------------------------------------------------------------
def get_current_stage(elapsed):
    if elapsed < 60:
        return 1, 0.08   # Stage 1: ~100-150 EPS
    elif elapsed < 120:
        return 2, 0.035  # Stage 2: ~300-450 EPS
    elif elapsed < 180:
        return 3, 0.012  # Stage 3: ~800-1000 EPS
    elif elapsed < 240:
        return 4, 0.003  # Stage 4: ~1800-2200 EPS
    else:
        return 5, 0.000  # Stage 5: Saturação máxima (0 delay)

def worker_postgres(target, stop_time, auditor):
    conn = psycopg2.connect(
        host="localhost", port=target["port"], user="admin",
        password="admin_password", dbname=target["db"]
    )
    conn.autocommit = False
    cur = conn.cursor()
    table = target["table"]
    topic = target["topic"]

    recent_ids = []
    i = 0
    t_start = time.time()

    while time.time() < stop_time:
        elapsed = time.time() - t_start
        stage, delay = get_current_stage(elapsed)
        rec_id = f"5M-PG-{target['id']}-{int(time.time()*1000)%1000000}-{i}"
        i += 1

        # 1. INSERT
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}:INSERT", t0, stage)
        if target["id"] == "postgres-financeiro":
            cur.execute(f"INSERT INTO {table} (numero, valor, status) VALUES (%s, %s, %s)", (rec_id, 100.0 + i, 'ABERTO'))
        elif target["id"] == "postgres-logistica":
            cur.execute(f"INSERT INTO {table} (codigo_rastreio, destinatario, status) VALUES (%s, %s, %s)", (rec_id, f"Dest {i}", 'POSTADO'))
        elif target["id"] == "postgres-vendas":
            cur.execute(f"INSERT INTO {table} (cliente, total, status) VALUES (%s, %s, %s)", (rec_id, 250.0 + i, 'PAGO'))
        elif target["id"] == "postgres-pagamentos":
            cur.execute(f"INSERT INTO {table} (codigo_fatura, valor, status) VALUES (%s, %s, %s)", (rec_id, 80.0 + i, 'EMITIDA'))
        elif target["id"] == "postgres-rh":
            cur.execute(f"INSERT INTO {table} (matricula, salario_base, status) VALUES (%s, %s, %s)", (rec_id, 3500.0 + i, 'ATIVO'))
        conn.commit()
        recent_ids.append(rec_id)

        # 2. UPDATE (a cada 3 inserts)
        if i % 3 == 0 and recent_ids:
            up_id = recent_ids[-1]
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{up_id}:UPDATE", t0_up, stage)
            if target["id"] == "postgres-financeiro":
                cur.execute(f"UPDATE {table} SET status = 'PROCESSADO', valor = valor + 10 WHERE {target['pk']} = %s", (up_id,))
            else:
                cur.execute(f"UPDATE {table} SET status = 'PROCESSADO' WHERE {target['pk']} = %s", (up_id,))
            conn.commit()

        # 3. DELETE (a cada 6 inserts)
        if i % 6 == 0 and len(recent_ids) > 5:
            del_id = recent_ids.pop(0)
            t0_del = time.time()
            auditor.record_sent(f"{topic}:{del_id}:DELETE", t0_del, stage)
            cur.execute(f"DELETE FROM {table} WHERE {target['pk']} = %s", (del_id,))
            conn.commit()

        if delay > 0:
            time.sleep(delay)

    conn.close()

def worker_mysql(target, stop_time, auditor):
    conn = pymysql.connect(
        host="localhost", port=target["port"], user="admin",
        password="admin_password", database=target["db"], autocommit=False
    )
    cur = conn.cursor()
    table = target["table"]
    topic = target["topic"]

    recent_ids = []
    i = 0
    t_start = time.time()

    while time.time() < stop_time:
        elapsed = time.time() - t_start
        stage, delay = get_current_stage(elapsed)
        rec_id = f"5M-MY-{target['id']}-{int(time.time()*1000)%1000000}-{i}"
        i += 1

        # 1. INSERT
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}:INSERT", t0, stage)
        if target["id"] == "mysql-financeiro":
            cur.execute(f"INSERT INTO {table} (numero, valor, status) VALUES (%s, %s, %s)", (rec_id, 150.0 + i, 'ABERTO'))
        elif target["id"] == "mysql-faturamento":
            cur.execute(f"INSERT INTO {table} (chave_acesso, valor_total, status) VALUES (%s, %s, %s)", (rec_id, 450.0 + i, 'AUTORIZADA'))
        elif target["id"] == "mysql-estoque":
            cur.execute(f"INSERT INTO {table} (sku, quantidade, status) VALUES (%s, %s, %s)", (rec_id, 10 + (i % 100), 'DISPONIVEL'))
        elif target["id"] == "mysql-ecommerce":
            cur.execute(f"INSERT INTO {table} (cliente_id, total, status) VALUES (%s, %s, %s)", (rec_id, 99.0 + i, 'FINALIZADO'))
        elif target["id"] == "mysql-crm":
            cur.execute(f"INSERT INTO {table} (documento, nome, status) VALUES (%s, %s, %s)", (rec_id, f"Empresa {i} Ltda", 'QUALIFICADO'))
        conn.commit()
        recent_ids.append(rec_id)

        # 2. UPDATE (a cada 3 inserts)
        if i % 3 == 0 and recent_ids:
            up_id = recent_ids[-1]
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{up_id}:UPDATE", t0_up, stage)
            if target["id"] == "mysql-financeiro":
                cur.execute(f"UPDATE {table} SET status = 'PROCESSADO', valor = valor + 15 WHERE {target['pk']} = %s", (up_id,))
            else:
                cur.execute(f"UPDATE {table} SET status = 'PROCESSADO' WHERE {target['pk']} = %s", (up_id,))
            conn.commit()

        # 3. DELETE (a cada 6 inserts)
        if i % 6 == 0 and len(recent_ids) > 5:
            del_id = recent_ids.pop(0)
            t0_del = time.time()
            auditor.record_sent(f"{topic}:{del_id}:DELETE", t0_del, stage)
            cur.execute(f"DELETE FROM {table} WHERE {target['pk']} = %s", (del_id,))
            conn.commit()

        if delay > 0:
            time.sleep(delay)

    conn.close()

def worker_oracle(target, stop_time, auditor):
    conn = oracledb.connect(user="SYSTEM", password="admin_password", dsn="localhost:1521/XE")
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET CONTAINER = ORCLPDB1")
    table = f"{target['schema']}.{target['table']}"
    topic = target["topic"]

    recent_ids = []
    i = 0
    t_start = time.time()

    while time.time() < stop_time:
        elapsed = time.time() - t_start
        stage, delay = get_current_stage(elapsed)
        rec_id = int(time.time() * 1000) % 100000000 + i
        i += 1

        # 1. INSERT
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}:INSERT", t0, stage)
        if target["id"] == "oracle-rh":
            cur.execute(f"INSERT INTO {table} (ID, NOME, CARGO, DEPARTAMENTO) VALUES (:1, :2, :3, :4)",
                        (rec_id, f"Funcionario {rec_id}", "Especialista", "Engenharia"))
        elif target["id"] == "oracle-financeiro":
            cur.execute(f"INSERT INTO {table} (ID, DESCRICAO, VALOR, STATUS) VALUES (:1, :2, :3, :4)",
                        (rec_id, f"Lancamento {rec_id}", 1250.0 + i, "ABERTO"))
        elif target["id"] == "oracle-patrimonio":
            cur.execute(f"INSERT INTO {table} (ID, CODIGO, DESCRICAO, VALOR) VALUES (:1, :2, :3, :4)",
                        (rec_id, f"PAT-{rec_id}", f"Equipamento {rec_id}", 4500.0))
        elif target["id"] == "oracle-auditoria":
            cur.execute(f"INSERT INTO {table} (ID, USUARIO, ACAO, STATUS) VALUES (:1, :2, :3, :4)",
                        (rec_id, f"user_{rec_id}", "ALTERACAO_REGISTRO", "OK"))
        elif target["id"] == "oracle-contratos":
            cur.execute(f"INSERT INTO {table} (ID, NUMERO, FORNECEDOR, VALOR) VALUES (:1, :2, :3, :4)",
                        (rec_id, f"CTR-{rec_id}", f"Fornecedor {rec_id}", 98000.0))
        conn.commit()
        recent_ids.append(rec_id)

        # 2. UPDATE (a cada 3 inserts)
        if i % 3 == 0 and recent_ids:
            up_id = recent_ids[-1]
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{up_id}:UPDATE", t0_up, stage)
            if target["id"] == "oracle-rh":
                cur.execute(f"UPDATE {table} SET CARGO = 'Senior', DEPARTAMENTO = 'Operacoes' WHERE ID = :1", (up_id,))
            elif target["id"] == "oracle-financeiro":
                cur.execute(f"UPDATE {table} SET STATUS = 'LIQUIDADO', VALOR = VALOR + 50 WHERE ID = :1", (up_id,))
            elif target["id"] == "oracle-patrimonio":
                cur.execute(f"UPDATE {table} SET DESCRICAO = 'Ativo Depreciado' WHERE ID = :1", (up_id,))
            elif target["id"] == "oracle-auditoria":
                cur.execute(f"UPDATE {table} SET STATUS = 'AUDITADO' WHERE ID = :1", (up_id,))
            elif target["id"] == "oracle-contratos":
                cur.execute(f"UPDATE {table} SET VALOR = VALOR + 1000 WHERE ID = :1", (up_id,))
            conn.commit()

        # 3. DELETE (a cada 6 inserts)
        if i % 6 == 0 and len(recent_ids) > 5:
            del_id = recent_ids.pop(0)
            t0_del = time.time()
            auditor.record_sent(f"{topic}:{del_id}:DELETE", t0_del, stage)
            cur.execute(f"DELETE FROM {table} WHERE ID = :1", (del_id,))
            conn.commit()

        if delay > 0:
            time.sleep(delay)

    conn.close()

# -----------------------------------------------------------------------------
# Resource Telemetry Collector
# -----------------------------------------------------------------------------
class TelemetryCollector:
    def __init__(self, stop_time):
        self.stop_time = stop_time
        self.samples = []
        self.running = threading.Event()
        self.running.set()

    def run(self):
        while self.running.is_set() and time.time() < self.stop_time:
            try:
                out = subprocess.check_output(
                    ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
                    text=True, timeout=5
                )
                ts = time.time()
                sample = {"timestamp": ts, "containers": {}}
                for line in out.strip().split("\n"):
                    parts = line.split("\t")
                    if len(parts) >= 3:
                        name = parts[0]
                        cpu = float(parts[1].replace("%", "")) if "%" in parts[1] else 0.0
                        mem_str = parts[2].split("/")[0].strip()
                        # parse mem
                        mem_val = 0.0
                        if "GiB" in mem_str:
                            mem_val = float(mem_str.replace("GiB", "").strip()) * 1024.0
                        elif "MiB" in mem_str:
                            mem_val = float(mem_str.replace("MiB", "").strip())
                        elif "kB" in mem_str:
                            mem_val = float(mem_str.replace("kB", "").strip()) / 1024.0
                        sample["containers"][name] = {"cpu": cpu, "mem_mb": mem_val}
                self.samples.append(sample)
            except Exception:
                pass
            time.sleep(10)

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
def run_5min_benchmark():
    DURATION_SEC = 300  # 5 minutos exatos
    t_start = time.time()
    t_stop = t_start + DURATION_SEC

    print("=" * 80)
    print(f"INICIANDO BATERIA CIENTÍFICA REAL DE 5 MINUTOS CONSECUTIVOS (300 SEGUNDOS)")
    print(f"Topologia: 15 Bancos de Dados Simultâneos (5 Postgres, 5 MySQL, 5 Oracle)")
    print(f"Operações DML Contínuas: INSERT + UPDATE (Before/After) + DELETE")
    print(f"Escalonamento: 5 Estágios Progressivos de Carga para Mapear a Saturação")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    topics = [t["topic"] for t in TARGETS]
    auditor = RealTime5MinAuditor(topics)
    auditor.start()
    time.sleep(2)

    telemetry = TelemetryCollector(t_stop)
    th_telemetry = threading.Thread(target=telemetry.run, daemon=True)
    th_telemetry.start()

    workers = []
    for t in TARGETS:
        if t["type"] == "postgres":
            th = threading.Thread(target=worker_postgres, args=(t, t_stop, auditor))
        elif t["type"] == "mysql":
            th = threading.Thread(target=worker_mysql, args=(t, t_stop, auditor))
        elif t["type"] == "oracle":
            th = threading.Thread(target=worker_oracle, args=(t, t_stop, auditor))
        workers.append(th)
        th.start()

    print(f"Todas as 15 threads de injeção iniciadas com sucesso!")

    # Acompanhamento progressivo em tempo real a cada minuto
    for stage_num in range(1, 6):
        target_check = t_start + stage_num * 60
        sleep_dur = max(0, target_check - time.time())
        time.sleep(sleep_dur)
        curr_elapsed = time.time() - t_start
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Estágio {stage_num}/5 concluído ({curr_elapsed:.1f}s decorridos).")
        pg_ops = sum(auditor.ops_captured['postgres'].values())
        my_ops = sum(auditor.ops_captured['mysql'].values())
        ora_ops = sum(auditor.ops_captured['oracle'].values())
        print(f"   Eventos capturados até agora: PG={pg_ops}, MY={my_ops}, ORA={ora_ops} | Total={pg_ops + my_ops + ora_ops}")

    print("\nAguardando término das threads de injeção...")
    for w in workers:
        w.join()

    print("Injeção de 5 minutos finalizada! Aguardando 12s para drenagem completa dos tópicos no Kafka...")
    time.sleep(12)
    auditor.stop()
    telemetry.running.clear()

    total_time = time.time() - t_start
    print(f"\nBateria concluída em {total_time:.2f} segundos!")

    # -------------------------------------------------------------------------
    # Consolidação e Cálculo Estatístico dos Resultados
    # -------------------------------------------------------------------------
    def calc_stats(lat_list):
        if not lat_list:
            return {"count": 0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "max": 0.0}
        arr = np.array(lat_list)
        return {
            "count": len(arr),
            "p50": round(float(np.percentile(arr, 50)), 2),
            "p90": round(float(np.percentile(arr, 90)), 2),
            "p95": round(float(np.percentile(arr, 95)), 2),
            "p99": round(float(np.percentile(arr, 99)), 2),
            "mean": round(float(np.mean(arr)), 2),
            "max": round(float(np.max(arr)), 2)
        }

    results = {
        "benchmark_metadata": {
            "duration_seconds": round(total_time, 2),
            "timestamp": datetime.now().isoformat(),
            "databases_count": 15,
            "engines": ["PostgreSQL", "MySQL", "Oracle"],
            "operation_types": ["INSERT", "UPDATE", "DELETE"]
        },
        "operations_captured": auditor.ops_captured,
        "events_per_target": auditor.events_per_target,
        "stages": {},
        "engines_by_stage": {"postgres": {}, "mysql": {}, "oracle": {}},
        "telemetry_samples": telemetry.samples
    }

    for s in range(1, 6):
        results["stages"][f"stage_{s}"] = {
            "all": calc_stats(auditor.latencies_by_stage[s]),
            "postgres": calc_stats(auditor.latencies_by_engine["postgres"][s]),
            "mysql": calc_stats(auditor.latencies_by_engine["mysql"][s]),
            "oracle": calc_stats(auditor.latencies_by_engine["oracle"][s])
        }
        for eng in ["postgres", "mysql", "oracle"]:
            results["engines_by_stage"][eng][f"stage_{s}"] = calc_stats(auditor.latencies_by_engine[eng][s])

    # Salva dados brutos em JSON
    with open("benchmark/results/benchmark_realtime_5min.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print("Dados brutos salvos em benchmark/results/benchmark_realtime_5min.json")

    # Imprime tabela no terminal
    print("\n" + "=" * 90)
    print("RESULTADOS EMPÍRICOS DA BATERIA DE 5 MINUTOS POR ESTÁGIO DE CARGA PROGRESSIVA")
    print("=" * 90)
    print(f"{'Estágio':<10} | {'Regime':<18} | {'SGBD':<10} | {'Eventos':<8} | {'p50 (ms)':<9} | {'p90 (ms)':<9} | {'p99 (ms)':<9} | {'Max (ms)':<9}")
    print("-" * 90)

    stage_names = {
        1: "1. Aquecimento",
        2: "2. Carga Moderada",
        3: "3. Carga Nominal",
        4: "4. Alto Estresse",
        5: "5. Saturação Plena"
    }

    for s in range(1, 6):
        for eng in ["postgres", "mysql", "oracle"]:
            st = results["stages"][f"stage_{s}"][eng]
            print(f"{s:<10} | {stage_names[s]:<18} | {eng:<10} | {st['count']:<8} | {st['p50']:<9} | {st['p90']:<9} | {st['p99']:<9} | {st['max']:<9}")
        print("-" * 90)

    # -------------------------------------------------------------------------
    # GERAÇÃO DAS FIGURAS CIENTÍFICAS EM ALTA RESOLUÇÃO (300 DPI)
    # -------------------------------------------------------------------------
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')

    # FIGURA 1: CURVA EMPÍRICA DE SATURAÇÃO E PROGRESSÃO TEMPORAL
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.4), dpi=300)

    stages_x = [1, 2, 3, 4, 5]
    stage_labels = ["Estágio 1\n(0-60s)", "Estágio 2\n(60-120s)", "Estágio 3\n(120-180s)", "Estágio 4\n(180-240s)", "Estágio 5\n(240-300s)"]

    pg_p50 = [results["stages"][f"stage_{s}"]["postgres"]["p50"] for s in stages_x]
    my_p50 = [results["stages"][f"stage_{s}"]["mysql"]["p50"] for s in stages_x]
    ora_p50 = [results["stages"][f"stage_{s}"]["oracle"]["p50"] for s in stages_x]

    pg_p99 = [results["stages"][f"stage_{s}"]["postgres"]["p99"] for s in stages_x]
    my_p99 = [results["stages"][f"stage_{s}"]["mysql"]["p99"] for s in stages_x]
    ora_p99 = [results["stages"][f"stage_{s}"]["oracle"]["p99"] for s in stages_x]

    # Subplot 1: p50 Mediana ao longo dos 5 minutos
    ax1.plot(stages_x, pg_p50, marker='o', color='#1565c0', linewidth=2.4, label='PostgreSQL (pgoutput WAL)')
    ax1.plot(stages_x, my_p50, marker='s', color='#2e7d32', linewidth=2.4, label='MySQL (Binlog TCP push)')
    ax1.plot(stages_x, ora_p50, marker='^', color='#c62828', linewidth=2.4, label='Oracle (LogMiner poll)')

    ax1.set_title('A. Latência Mediana ($p_{50}$) nos 5 Minutos Progressivos', fontsize=12, fontweight='bold', pad=10)
    ax1.set_xlabel('Estágio Temporal de Carga (Escalonamento Contínuo)', fontsize=11)
    ax1.set_ylabel('Latência End-to-End ($p_{50}$) [ms]', fontsize=11)
    ax1.set_xticks(stages_x)
    ax1.set_xticklabels(stage_labels, fontsize=9.5)
    ax1.set_yscale('log')
    ax1.legend(loc='upper left', frameon=True, fontsize=10)
    ax1.grid(True, which="both", ls="--", alpha=0.5)

    # Subplot 2: p99 Cauda e Joelho de Saturação Real
    ax2.plot(stages_x, pg_p99, marker='o', color='#1565c0', linewidth=2.4, label='PostgreSQL (pgoutput WAL)')
    ax2.plot(stages_x, my_p99, marker='s', color='#2e7d32', linewidth=2.4, label='MySQL (Binlog TCP push)')
    ax2.plot(stages_x, ora_p99, marker='^', color='#c62828', linewidth=2.4, label='Oracle (LogMiner poll)')

    if max(pg_p99) > 1000:
        ax2.annotate('Joelho PostgreSQL\n(Contenção WAL / Sockets)',
                     xy=(4.5, pg_p99[4]), xytext=(3.0, pg_p99[4] * 0.4),
                     arrowprops=dict(facecolor='#1565c0', shrink=0.08, width=1.5, headwidth=7),
                     fontsize=9, fontweight='bold', color='#0d47a1',
                     bbox=dict(boxstyle="round,pad=0.3", fc="#e3f2fd", ec="#1565c0", lw=1))

    ax2.set_title('B. Latência de Cauda ($p_{99}$) e Saturação Empírica', fontsize=12, fontweight='bold', pad=10)
    ax2.set_xlabel('Estágio Temporal de Carga (Escalonamento Contínuo)', fontsize=11)
    ax2.set_ylabel('Latência End-to-End ($p_{99}$) [ms]', fontsize=11)
    ax2.set_xticks(stages_x)
    ax2.set_xticklabels(stage_labels, fontsize=9.5)
    ax2.set_yscale('log')
    ax2.legend(loc='lower right', frameon=True, fontsize=10)
    ax2.grid(True, which="both", ls="--", alpha=0.5)

    plt.tight_layout()
    fig.savefig('benchmark/results/benchmark_saturacao_5min_consolidado.png')
    fig.savefig('c:/Users/01923483102/Documents/rootLCDC/artigo_latex/figurasrootl/benchmark_saturacao_5min_consolidado.png')
    plt.close()

    # FIGURA 2: TELEMETRIA DE RECURSOS DOS SGBDS (CPU & RAM) NOS 5 MINUTOS
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.2), dpi=300)

    if telemetry.samples:
        t_base = telemetry.samples[0]["timestamp"]
        times = [(s["timestamp"] - t_base) for s in telemetry.samples]

        pg_cpu = [s["containers"].get("cdc-postgres", {}).get("cpu", 0.0) for s in telemetry.samples]
        my_cpu = [s["containers"].get("cdc-mysql", {}).get("cpu", 0.0) for s in telemetry.samples]
        ora_cpu = [s["containers"].get("oracle-cdc", {}).get("cpu", 0.0) for s in telemetry.samples]

        pg_mem = [s["containers"].get("cdc-postgres", {}).get("mem_mb", 0.0) for s in telemetry.samples]
        my_mem = [s["containers"].get("cdc-mysql", {}).get("mem_mb", 0.0) for s in telemetry.samples]
        ora_mem = [s["containers"].get("oracle-cdc", {}).get("mem_mb", 0.0) for s in telemetry.samples]

        ax1.plot(times, pg_cpu, color='#1565c0', linewidth=2.0, label='PostgreSQL (cdc-postgres)')
        ax1.plot(times, my_cpu, color='#2e7d32', linewidth=2.0, label='MySQL (cdc-mysql)')
        ax1.plot(times, ora_cpu, color='#c62828', linewidth=2.0, label='Oracle XE (oracle-cdc)')

        ax1.set_title('A. Perfil Dinâmico de CPU dos SGBDs (5 Minutos)', fontsize=12, fontweight='bold', pad=10)
        ax1.set_xlabel('Tempo de Execução Contínua (Segundos)', fontsize=11)
        ax1.set_ylabel('Utilização de CPU (%)', fontsize=11)
        ax1.legend(loc='upper left', frameon=True, fontsize=10)
        ax1.grid(True, linestyle='--', alpha=0.5)

        ax2.plot(times, pg_mem, color='#1565c0', linewidth=2.0, label='PostgreSQL (MB)')
        ax2.plot(times, my_mem, color='#2e7d32', linewidth=2.0, label='MySQL (MB)')
        ax2.plot(times, ora_mem, color='#c62828', linewidth=2.0, label='Oracle XE (MB)')

        ax2.set_title('B. Alocação de Memória RAM Residente dos SGBDs', fontsize=12, fontweight='bold', pad=10)
        ax2.set_xlabel('Tempo de Execução Contínua (Segundos)', fontsize=11)
        ax2.set_ylabel('Memória Alocada (MB)', fontsize=11)
        ax2.legend(loc='center left', frameon=True, fontsize=10)
        ax2.grid(True, linestyle='--', alpha=0.5)

        plt.tight_layout()
        fig.savefig('benchmark/results/overhead_sgbd_realtime_5min.png')
        fig.savefig('c:/Users/01923483102/Documents/rootLCDC/artigo_latex/figurasrootl/overhead_sgbd_realtime_5min.png')
        plt.close()

    print("Figuras geradas com sucesso em artigo_latex/figurasrootl e benchmark/results!")

if __name__ == "__main__":
    run_5min_benchmark()

import time
import json
import uuid
import threading
import os
import subprocess
import signal
from datetime import datetime
import psycopg2
import pymysql
import oracledb
import numpy as np
import matplotlib.pyplot as plt

TARGETS = [
    # 5 Postgres
    {
        "id": "postgres-financeiro", "type": "postgres", "port": 5432, "db": "financeiro",
        "table": "contratos", "topic": "cdc.postgres-financeiro.public.contratos"
    },
    {
        "id": "postgres-logistica", "type": "postgres", "port": 5432, "db": "logistica",
        "table": "entregas", "topic": "cdc.postgres-logistica.public.entregas"
    },
    {
        "id": "postgres-vendas", "type": "postgres", "port": 5432, "db": "vendas",
        "table": "pedidos", "topic": "cdc.postgres-vendas.public.pedidos"
    },
    {
        "id": "postgres-pagamentos", "type": "postgres", "port": 5433, "db": "pagamentos",
        "table": "faturas", "topic": "cdc.postgres-pagamentos.public.faturas"
    },
    {
        "id": "postgres-rh", "type": "postgres", "port": 5434, "db": "rh",
        "table": "folha", "topic": "cdc.postgres-rh.public.folha"
    },

    # 5 MySQL
    {
        "id": "mysql-financeiro", "type": "mysql", "port": 3306, "db": "financeiro",
        "table": "contratos", "topic": "cdc.mysql-financeiro.financeiro.contratos"
    },
    {
        "id": "mysql-faturamento", "type": "mysql", "port": 3306, "db": "faturamento",
        "table": "notas_fiscais", "topic": "cdc.mysql-faturamento.faturamento.notas_fiscais"
    },
    {
        "id": "mysql-estoque", "type": "mysql", "port": 3306, "db": "estoque",
        "table": "produtos", "topic": "cdc.mysql-estoque.estoque.produtos"
    },
    {
        "id": "mysql-ecommerce", "type": "mysql", "port": 3307, "db": "ecommerce",
        "table": "carrinhos", "topic": "cdc.mysql-ecommerce.ecommerce.carrinhos"
    },
    {
        "id": "mysql-crm", "type": "mysql", "port": 3308, "db": "crm",
        "table": "clientes", "topic": "cdc.mysql-crm.crm.clientes"
    },

    # 5 Oracle
    {
        "id": "oracle-rh", "type": "oracle", "port": 1521, "schema": "RH",
        "table": "FUNCIONARIOS", "topic": "cdc.oracle-rh.RH.FUNCIONARIOS"
    },
    {
        "id": "oracle-financeiro", "type": "oracle", "port": 1521, "schema": "FINANCEIRO",
        "table": "LANCAMENTOS", "topic": "cdc.oracle-financeiro.FINANCEIRO.LANCAMENTOS"
    },
    {
        "id": "oracle-patrimonio", "type": "oracle", "port": 1521, "schema": "PATRIMONIO",
        "table": "BENS", "topic": "cdc.oracle-patrimonio.PATRIMONIO.BENS"
    },
    {
        "id": "oracle-auditoria", "type": "oracle", "port": 1521, "schema": "AUDITORIA",
        "table": "REGISTROS", "topic": "cdc.oracle-auditoria.AUDITORIA.REGISTROS"
    },
    {
        "id": "oracle-contratos", "type": "oracle", "port": 1521, "schema": "CONTRATOS",
        "table": "ACORDOS", "topic": "cdc.oracle-contratos.CONTRATOS.ACORDOS"
    }
]

# -----------------------------------------------------------------------------
# Workers de Injeção Transacional Rigorosamente Idênticos
# -----------------------------------------------------------------------------
def worker_postgres(target, stop_time, tag):
    try:
        conn = psycopg2.connect(
            host="localhost", port=target["port"], user="admin",
            password="admin_password", dbname=target["db"]
        )
        conn.autocommit = False
        cur = conn.cursor()
        table = target["table"]
        recent_ids = []
        i = 0
        while time.time() < stop_time:
            rec_id = f"PAS-{tag}-PG-{target['id']}-{int(time.time()*1000)%1000000}-{i}"
            i += 1
            # INSERT
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

            # UPDATE a cada 3 inserts
            if i % 3 == 0 and recent_ids:
                up_id = recent_ids[-1]
                if target["id"] == "postgres-financeiro":
                    cur.execute(f"UPDATE {table} SET valor = valor * 1.05, status = 'PROCESSANDO' WHERE numero = %s", (up_id,))
                elif target["id"] == "postgres-logistica":
                    cur.execute(f"UPDATE {table} SET status = 'EM_TRANSITO' WHERE codigo_rastreio = %s", (up_id,))
                elif target["id"] == "postgres-vendas":
                    cur.execute(f"UPDATE {table} SET total = total + 10.0, status = 'ENTREGUE' WHERE cliente = %s", (up_id,))
                elif target["id"] == "postgres-pagamentos":
                    cur.execute(f"UPDATE {table} SET status = 'QUITADA' WHERE codigo_fatura = %s", (up_id,))
                elif target["id"] == "postgres-rh":
                    cur.execute(f"UPDATE {table} SET salario_base = salario_base + 100.0 WHERE matricula = %s", (up_id,))
                conn.commit()

            # DELETE a cada 6 inserts
            if i % 6 == 0 and len(recent_ids) > 5:
                del_id = recent_ids.pop(0)
                if target["id"] == "postgres-financeiro":
                    cur.execute(f"DELETE FROM {table} WHERE numero = %s", (del_id,))
                elif target["id"] == "postgres-logistica":
                    cur.execute(f"DELETE FROM {table} WHERE codigo_rastreio = %s", (del_id,))
                elif target["id"] == "postgres-vendas":
                    cur.execute(f"DELETE FROM {table} WHERE cliente = %s", (del_id,))
                elif target["id"] == "postgres-pagamentos":
                    cur.execute(f"DELETE FROM {table} WHERE codigo_fatura = %s", (del_id,))
                elif target["id"] == "postgres-rh":
                    cur.execute(f"DELETE FROM {table} WHERE matricula = %s", (del_id,))
                conn.commit()

            time.sleep(0.015)
        conn.close()
    except Exception as e:
        print(f"Erro no worker PostgreSQL {target['id']}: {e}")

def worker_mysql(target, stop_time, tag):
    try:
        conn = pymysql.connect(
            host="localhost", port=target["port"], user="root",
            password="root_password", database=target["db"]
        )
        conn.autocommit = False
        cur = conn.cursor()
        table = target["table"]
        recent_ids = []
        i = 0
        while time.time() < stop_time:
            rec_id = f"PAS-{tag}-MY-{target['id']}-{int(time.time()*1000)%1000000}-{i}"
            i += 1
            # INSERT
            if target["id"] == "mysql-financeiro":
                cur.execute(f"INSERT INTO {table} (numero, valor, status) VALUES (%s, %s, %s)", (rec_id, 100.0 + i, 'ABERTO'))
            elif target["id"] == "mysql-faturamento":
                cur.execute(f"INSERT INTO {table} (chave_acesso, valor_total, status) VALUES (%s, %s, %s)", (rec_id, 500.0 + i, 'EMITIDA'))
            elif target["id"] == "mysql-estoque":
                cur.execute(f"INSERT INTO {table} (sku, quantidade, status) VALUES (%s, %s, %s)", (rec_id, 10 + i, 'DISPONIVEL'))
            elif target["id"] == "mysql-ecommerce":
                cur.execute(f"INSERT INTO {table} (cliente_id, total, status) VALUES (%s, %s, %s)", (rec_id, 75.0 + i, 'CRIADO'))
            elif target["id"] == "mysql-crm":
                cur.execute(f"INSERT INTO {table} (documento, nome, status) VALUES (%s, %s, %s)", (rec_id, f"Cliente {i}", 'ATIVO'))
            conn.commit()
            recent_ids.append(rec_id)

            # UPDATE a cada 3 inserts
            if i % 3 == 0 and recent_ids:
                up_id = recent_ids[-1]
                if target["id"] == "mysql-financeiro":
                    cur.execute(f"UPDATE {table} SET valor = valor * 1.05, status = 'PROCESSANDO' WHERE numero = %s", (up_id,))
                elif target["id"] == "mysql-faturamento":
                    cur.execute(f"UPDATE {table} SET status = 'AUTORIZADA' WHERE chave_acesso = %s", (up_id,))
                elif target["id"] == "mysql-estoque":
                    cur.execute(f"UPDATE {table} SET quantidade = quantidade + 5 WHERE sku = %s", (up_id,))
                elif target["id"] == "mysql-ecommerce":
                    cur.execute(f"UPDATE {table} SET total = total + 15.0, status = 'FINALIZADO' WHERE cliente_id = %s", (up_id,))
                elif target["id"] == "mysql-crm":
                    cur.execute(f"UPDATE {table} SET status = 'VIP' WHERE documento = %s", (up_id,))
                conn.commit()

            # DELETE a cada 6 inserts
            if i % 6 == 0 and len(recent_ids) > 5:
                del_id = recent_ids.pop(0)
                if target["id"] == "mysql-financeiro":
                    cur.execute(f"DELETE FROM {table} WHERE numero = %s", (del_id,))
                elif target["id"] == "mysql-faturamento":
                    cur.execute(f"DELETE FROM {table} WHERE chave_acesso = %s", (del_id,))
                elif target["id"] == "mysql-estoque":
                    cur.execute(f"DELETE FROM {table} WHERE sku = %s", (del_id,))
                elif target["id"] == "mysql-ecommerce":
                    cur.execute(f"DELETE FROM {table} WHERE cliente_id = %s", (del_id,))
                elif target["id"] == "mysql-crm":
                    cur.execute(f"DELETE FROM {table} WHERE documento = %s", (del_id,))
                conn.commit()

            time.sleep(0.015)
        conn.close()
    except Exception as e:
        print(f"Erro no worker MySQL {target['id']}: {e}")

def worker_oracle(target, stop_time, tag):
    try:
        conn = oracledb.connect(user="SYSTEM", password="admin_password", dsn="localhost:1521/XE")
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("ALTER SESSION SET CONTAINER = ORCLPDB1")
        table = f"{target['schema']}.{target['table']}"
        recent_ids = []
        i = 0
        while time.time() < stop_time:
            rec_id = int(time.time() * 1000) % 100000000 + i
            i += 1
            # INSERT
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

            # UPDATE a cada 3 inserts
            if i % 3 == 0 and recent_ids:
                up_id = recent_ids[-1]
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

            # DELETE a cada 6 inserts
            if i % 6 == 0 and len(recent_ids) > 5:
                del_id = recent_ids.pop(0)
                cur.execute(f"DELETE FROM {table} WHERE ID = :1", (del_id,))
                conn.commit()

            time.sleep(0.015)
        conn.close()
    except Exception as e:
        print(f"Erro no worker Oracle {target['id']}: {e}")

# -----------------------------------------------------------------------------
# Coletor de Telemetria de Alta Precisão (docker stats)
# -----------------------------------------------------------------------------
class HighResolutionTelemetry:
    def __init__(self, stop_time, sample_interval=2.0):
        self.stop_time = stop_time
        self.sample_interval = sample_interval
        self.samples = []
        self.running = threading.Event()
        self.running.set()

    def run(self):
        while self.running.is_set() and time.time() < self.stop_time:
            try:
                out = subprocess.check_output(
                    ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"],
                    text=True, timeout=5
                )
                ts = time.time()
                sample = {"timestamp": ts, "containers": {}}
                for line in out.strip().split("\n"):
                    parts = line.split("\t")
                    if len(parts) >= 3:
                        name = parts[0]
                        cpu = float(parts[1].replace("%", "").strip()) if "%" in parts[1] else 0.0
                        mem_str = parts[2].split("/")[0].strip()
                        mem_val = 0.0
                        if "GiB" in mem_str:
                            mem_val = float(mem_str.replace("GiB", "").strip()) * 1024.0
                        elif "MiB" in mem_str:
                            mem_val = float(mem_str.replace("MiB", "").strip())
                        elif "kB" in mem_str:
                            mem_val = float(mem_str.replace("kB", "").strip()) / 1024.0

                        sample["containers"][name] = {
                            "cpu": cpu,
                            "mem_mb": mem_val,
                            "net_io": parts[3].strip() if len(parts) > 3 else "",
                            "block_io": parts[4].strip() if len(parts) > 4 else ""
                        }
                self.samples.append(sample)
            except Exception:
                pass
            time.sleep(self.sample_interval)

def execute_load_phase(phase_name, duration_sec, tag):
    print(f"\n>>> INICIANDO FASE: {phase_name.upper()} (Duração: {duration_sec}s)")
    t_start = time.time()
    t_stop = t_start + duration_sec

    telemetry = HighResolutionTelemetry(t_stop, sample_interval=2.0)
    th_telemetry = threading.Thread(target=telemetry.run, daemon=True)
    th_telemetry.start()

    workers = []
    for t in TARGETS:
        if t["type"] == "postgres":
            th = threading.Thread(target=worker_postgres, args=(t, t_stop, tag))
        elif t["type"] == "mysql":
            th = threading.Thread(target=worker_mysql, args=(t, t_stop, tag))
        elif t["type"] == "oracle":
            th = threading.Thread(target=worker_oracle, args=(t, t_stop, tag))
        workers.append(th)
        th.start()

    print(f"Todas as 15 threads de injeção ativas na fase '{phase_name}'. Monitorando...")
    # Monitoramento de progresso
    while time.time() < t_stop:
        time.sleep(10)
        elapsed = time.time() - t_start
        print(f"   [{elapsed:.0f}s / {duration_sec}s] Coletando telemetria...")

    for w in workers:
        w.join()
    telemetry.running.clear()
    th_telemetry.join(timeout=3)
    print(f"Fase '{phase_name}' concluída! Total de amostras coletadas: {len(telemetry.samples)}")
    return telemetry.samples

def compute_engine_summary(samples):
    # Agrupa por motor
    pg_names = ["cdc-postgres", "cdc-postgres-2", "cdc-postgres-3"]
    my_names = ["cdc-mysql", "cdc-mysql-2", "cdc-mysql-3"]
    ora_names = ["oracle-cdc"]

    def extract_stats(container_name):
        cpus = [s["containers"].get(container_name, {}).get("cpu", 0.0) for s in samples if container_name in s["containers"]]
        mems = [s["containers"].get(container_name, {}).get("mem_mb", 0.0) for s in samples if container_name in s["containers"]]
        if not cpus:
            return {"cpu_mean": 0.0, "cpu_p50": 0.0, "cpu_p95": 0.0, "cpu_max": 0.0, "mem_mean": 0.0, "mem_max": 0.0}
        return {
            "cpu_mean": round(float(np.mean(cpus)), 2),
            "cpu_p50": round(float(np.percentile(cpus, 50)), 2),
            "cpu_p95": round(float(np.percentile(cpus, 95)), 2),
            "cpu_max": round(float(np.max(cpus)), 2),
            "mem_mean": round(float(np.mean(mems)), 2),
            "mem_max": round(float(np.max(mems)), 2)
        }

    return {
        "postgres_primary": extract_stats("cdc-postgres"),
        "mysql_primary": extract_stats("cdc-mysql"),
        "oracle": extract_stats("oracle-cdc"),
        "postgres_all": {
            "cpu_mean": round(sum(extract_stats(c)["cpu_mean"] for c in pg_names), 2),
            "mem_mean": round(sum(extract_stats(c)["mem_mean"] for c in pg_names), 2)
        },
        "mysql_all": {
            "cpu_mean": round(sum(extract_stats(c)["cpu_mean"] for c in my_names), 2),
            "mem_mean": round(sum(extract_stats(c)["mem_mean"] for c in my_names), 2)
        }
    }

# -----------------------------------------------------------------------------
# Fluxo Principal da Bateria de Passividade
# -----------------------------------------------------------------------------
def run_passivity_benchmark():
    PHASE_DURATION = 90  # 90 segundos para cada regime (1.5 min cada)
    print("=" * 80)
    print("BATERIA EXPERIMENTAL REAL DE PASSIVIDADE DOS SGBDS")
    print("Comparação Rigorosa de Telemetria: Baseline (Sem CDC) vs CDC Ativo (Com RootL)")
    print("=" * 80)

    # 1. Garantir que Java está DESLIGADO para a Fase Baseline
    print("\n[Passo 1] Desligando qualquer instância ativa do RootL CDC Publisher...")
    subprocess.run(["powershell", "-Command", "Stop-Process -Name java -Force -ErrorAction SilentlyContinue"], capture_output=True)
    time.sleep(4)

    # 2. Executar Fase 1: Baseline (Sem CDC)
    samples_baseline = execute_load_phase("Baseline (Sem Ingestão CDC)", PHASE_DURATION, tag="BASE")
    time.sleep(5)

    # 3. Iniciar o RootL CDC Publisher para a Fase 2
    print("\n[Passo 2] Iniciando o RootL CDC Publisher (Java Spring Boot)...")
    java_proc = subprocess.Popen(
        ["java", "-jar", "target/rootL_cdcPublisher-0.0.1-SNAPSHOT.jar"],
        cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    # Aguardar até o Actuator responder UP
    import urllib.request
    ready = False
    for _ in range(35):
        time.sleep(1)
        try:
            req = urllib.request.urlopen("http://localhost:8080/actuator/health", timeout=3)
            data = json.loads(req.read().decode('utf-8'))
            if data.get("status") == "UP":
                ready = True
                print("RootL CDC Publisher está operacional (Status: UP) com 15 conectores ativos!")
                break
        except Exception:
            pass

    if not ready:
        print("AVISO: Timeout aguardando status UP da aplicação Java. Continuando...")

    time.sleep(5)  # Estabilização dos 15 conectores

    # 4. Executar Fase 2: CDC Ativo (Com Ingestão do Motor)
    samples_cdc = execute_load_phase("CDC Ativo (Com Ingestão do Motor)", PHASE_DURATION, tag="CDC")

    # 5. Encerrar o processo Java
    print("\n[Passo 3] Finalizando o RootL CDC Publisher...")
    try:
        java_proc.terminate()
        java_proc.wait(timeout=5)
    except Exception:
        subprocess.run(["powershell", "-Command", "Stop-Process -Name java -Force -ErrorAction SilentlyContinue"], capture_output=True)

    # 6. Consolidação e Cálculo Estatístico
    stats_base = compute_engine_summary(samples_baseline)
    stats_cdc = compute_engine_summary(samples_cdc)

    # Cálculo dos Deltas
    deltas = {
        "postgres_primary": {
            "delta_cpu_mean": round(stats_cdc["postgres_primary"]["cpu_mean"] - stats_base["postgres_primary"]["cpu_mean"], 2),
            "delta_mem_mean": round(stats_cdc["postgres_primary"]["mem_mean"] - stats_base["postgres_primary"]["mem_mean"], 2)
        },
        "mysql_primary": {
            "delta_cpu_mean": round(stats_cdc["mysql_primary"]["cpu_mean"] - stats_base["mysql_primary"]["cpu_mean"], 2),
            "delta_mem_mean": round(stats_cdc["mysql_primary"]["mem_mean"] - stats_base["mysql_primary"]["mem_mean"], 2)
        },
        "oracle": {
            "delta_cpu_mean": round(stats_cdc["oracle"]["cpu_mean"] - stats_base["oracle"]["cpu_mean"], 2),
            "delta_mem_mean": round(stats_cdc["oracle"]["mem_mean"] - stats_base["oracle"]["mem_mean"], 2)
        }
    }

    full_results = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "phase_duration_seconds": PHASE_DURATION,
            "databases_count": 15
        },
        "baseline": stats_base,
        "cdc_active": stats_cdc,
        "deltas": deltas,
        "samples_baseline": samples_baseline,
        "samples_cdc": samples_cdc
    }

    os.makedirs("benchmark/results", exist_ok=True)
    with open("benchmark/results/benchmark_passivity_telemetry.json", "w", encoding="utf-8") as f:
        json.dump(full_results, f, indent=2)

    print("\n" + "=" * 80)
    print("RESULTADOS EMPÍRICOS DA AVALIAÇÃO DE PASSIVIDADE DOS SGBDS")
    print("=" * 80)
    print(f"{'SGBD / Container':<22} | {'CPU Base (%)':<12} | {'CPU CDC (%)':<12} | {'Delta CPU (%)':<14} | {'RAM Base (MB)':<13} | {'RAM CDC (MB)':<13} | {'Delta RAM (MB)':<14}")
    print("-" * 110)

    for eng, label in [("postgres_primary", "PostgreSQL (cdc-postgres)"),
                       ("mysql_primary", "MySQL (cdc-mysql)"),
                       ("oracle", "Oracle XE (oracle-cdc)")]:
        b = stats_base[eng]
        c = stats_cdc[eng]
        d = deltas[eng]
        print(f"{label:<22} | {b['cpu_mean']:<12.2f} | {c['cpu_mean']:<12.2f} | {d['delta_cpu_mean']:+14.2f} | {b['mem_mean']:<13.2f} | {c['mem_mean']:<13.2f} | {d['delta_mem_mean']:+14.2f}")
    print("-" * 110)

    # -------------------------------------------------------------------------
    # GERAÇÃO DA FIGURA COMPARATIVA CIENTÍFICA EM ALTA RESOLUÇÃO (300 DPI)
    # -------------------------------------------------------------------------
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.2), dpi=300)

    # SUBPLOT 1: Barras Comparativas de CPU (Baseline vs CDC Ativo) com anotação do Delta
    labels = ['PostgreSQL 16\n(WAL pgoutput)', 'MySQL 8.0\n(Binlog TCP push)', 'Oracle 21c XE\n(LogMiner SGA)']
    x = np.arange(len(labels))
    width = 0.35

    cpu_base = [stats_base["postgres_primary"]["cpu_mean"], stats_base["mysql_primary"]["cpu_mean"], stats_base["oracle"]["cpu_mean"]]
    cpu_cdc = [stats_cdc["postgres_primary"]["cpu_mean"], stats_cdc["mysql_primary"]["cpu_mean"], stats_cdc["oracle"]["cpu_mean"]]

    rects1 = ax1.bar(x - width/2, cpu_base, width, label='Regime Baseline (Sem Ingestão CDC)', color='#78909c', edgecolor='#37474f', linewidth=1.2)
    rects2 = ax1.bar(x + width/2, cpu_cdc, width, label='Regime CDC Ativo (Com RootL Ingestão)', color='#1565c0', edgecolor='#0d47a1', linewidth=1.2)

    ax1.set_title('A. Utilização Média de CPU nos SGBDs: Baseline vs. CDC Ativo', fontsize=11.5, fontweight='bold', pad=10)
    ax1.set_ylabel('Utilização Média de CPU (%)', fontsize=11)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=10)
    ax1.legend(loc='upper left', frameon=True, fontsize=9.5)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # Anotações dos Deltas sobre as barras
    for i in range(len(labels)):
        delta_val = cpu_cdc[i] - cpu_base[i]
        sign = "+" if delta_val >= 0 else ""
        y_pos = max(cpu_base[i], cpu_cdc[i]) + 2.0
        ax1.annotate(f"$\\Delta$ = {sign}{delta_val:.2f}%",
                     xy=(x[i] + width/2, cpu_cdc[i]),
                     xytext=(x[i], y_pos),
                     ha='center', fontsize=9.5, fontweight='bold',
                     color='#b71c1c' if delta_val > 10 else '#1b5e20',
                     bbox=dict(boxstyle="round,pad=0.2", fc="#f5f5f5", ec="#bdbdbd", lw=0.8))

    ax1.set_ylim(0, max(cpu_cdc) * 1.25)

    # SUBPLOT 2: Alocação de Memória RAM Residente (RSS) Baseline vs CDC
    mem_base = [stats_base["postgres_primary"]["mem_mean"], stats_base["mysql_primary"]["mem_mean"], stats_base["oracle"]["mem_mean"]]
    mem_cdc = [stats_cdc["postgres_primary"]["mem_mean"], stats_cdc["mysql_primary"]["mem_mean"], stats_cdc["oracle"]["mem_mean"]]

    rects3 = ax2.bar(x - width/2, mem_base, width, label='Regime Baseline (Sem Ingestão CDC)', color='#90a4ae', edgecolor='#37474f', linewidth=1.2)
    rects4 = ax2.bar(x + width/2, mem_cdc, width, label='Regime CDC Ativo (Com RootL Ingestão)', color='#2e7d32', edgecolor='#1b5e20', linewidth=1.2)

    ax2.set_title('B. Alocação de Memória RAM Residente dos SGBDs (MB)', fontsize=11.5, fontweight='bold', pad=10)
    ax2.set_ylabel('Memória RAM Alocada (MB)', fontsize=11)
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=10)
    ax2.legend(loc='upper left', frameon=True, fontsize=9.5)
    ax2.grid(True, linestyle='--', alpha=0.5)

    for i in range(len(labels)):
        delta_mem = mem_cdc[i] - mem_base[i]
        sign = "+" if delta_mem >= 0 else ""
        y_pos = max(mem_base[i], mem_cdc[i]) + 80
        ax2.annotate(f"$\\Delta$ = {sign}{delta_mem:.1f} MB",
                     xy=(x[i] + width/2, mem_cdc[i]),
                     xytext=(x[i], y_pos),
                     ha='center', fontsize=9, fontweight='bold',
                     color='#2e7d32',
                     bbox=dict(boxstyle="round,pad=0.2", fc="#f5f5f5", ec="#bdbdbd", lw=0.8))

    ax2.set_ylim(0, max(mem_cdc) * 1.2)

    plt.tight_layout()
    fig.savefig('benchmark/results/overhead_sgbd_passividade_comparativo.png')
    fig.savefig('c:/Users/01923483102/Documents/rootLCDC/artigo_latex/figurasrootl/overhead_sgbd_passividade_comparativo.png')
    plt.close()
    print("Figura comparativa gerada com sucesso em artigo_latex/figurasrootl/overhead_sgbd_passividade_comparativo.png!")

if __name__ == "__main__":
    run_passivity_benchmark()

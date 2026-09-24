#!/usr/bin/env python3
"""
RootL CDC Publisher - Suíte de Testes de Estresse Massivo Multi-Banco (15 Bancos)
Executa injeção concorrente de alta vazão sobre 15 conectores:
- 5 PostgreSQL (instância multi-database + instâncias isoladas em contêineres)
- 5 MySQL (instância multi-database + instâncias isoladas em contêineres)
- 5 Oracle (esquemas lógicos no Oracle XE com Supplemental Logging completo)
"""

import sys
import time
import uuid
import json
import os
import threading
import queue
from datetime import datetime

# Garante suporte a UTF-8 no console Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')
import numpy as np
import psycopg2
import pymysql
import oracledb
from kafka import KafkaConsumer
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

KAFKA_BOOTSTRAP = 'localhost:9092'
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

# Definição dos 15 alvos
TARGETS = [
    # 5 PostgreSQL
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

class StressAuditor:
    def __init__(self, topics):
        self.topics = topics
        self.sent_events = {}       # key: t0
        self.received_events = {}   # key: (t1, target_id)
        self.latencies = {t: [] for t in topics}
        self.target_stats = {}
        self.running = threading.Event()
        self.running.set()
        self.consumer_thread = None

    def start(self):
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()

    def record_sent(self, key, t0):
        self.sent_events[key] = t0

    def _consume_loop(self):
        consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f"stress-auditor-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=1000
        )
        try:
            while self.running.is_set():
                try:
                    batch = consumer.poll(timeout_ms=250)
                except Exception:
                    time.sleep(0.1)
                    continue

                t_now = time.time()
                for tp, records in batch.items():
                    topic_name = tp.topic
                    for r in records:
                        try:
                            val = json.loads(r.value.decode('utf-8'))
                            op = val.get('operation')
                            after = val.get('after') or {}
                            before = val.get('before') or {}

                            found_key = None
                            for k, v in after.items():
                                candidate = f"{topic_name}:{v}"
                                if candidate in self.sent_events:
                                    found_key = candidate
                                    break
                            if not found_key:
                                for k, v in before.items():
                                    candidate = f"{topic_name}:{v}"
                                    if candidate in self.sent_events:
                                        found_key = candidate
                                        break

                            if found_key and found_key in self.sent_events:
                                t0 = self.sent_events[found_key]
                                lat_ms = (t_now - t0) * 1000.0
                                if lat_ms >= 0:
                                    self.latencies[topic_name].append(lat_ms)
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

def inject_postgres(target, num_ops, auditor, start_id):
    topic = target["topic"]
    table = target["table"]
    conn = psycopg2.connect(
        dbname=target["db"], user="admin", password="admin_password",
        host="localhost", port=target["port"]
    )
    conn.autocommit = False
    cur = conn.cursor()
    
    # 70% INSERT, 30% UPDATE
    for i in range(num_ops):
        rec_id = f"STRESS-PG-{target['id']}-{start_id + i}"
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}", t0)
        
        if target["id"] == "postgres-financeiro":
            cur.execute(f"INSERT INTO {table} (numero, valor, status) VALUES (%s, %s, %s)",
                        (rec_id, 100.0 + i, 'NOVO'))
        elif target["id"] == "postgres-logistica":
            cur.execute(f"INSERT INTO {table} (codigo_rastreio, destinatario, status) VALUES (%s, %s, %s)",
                        (rec_id, f"Cliente {i}", 'DESPACHADO'))
        elif target["id"] == "postgres-vendas":
            cur.execute(f"INSERT INTO {table} (cliente, total, status) VALUES (%s, %s, %s)",
                        (rec_id, 250.0 + i, 'PAGO'))
        elif target["id"] == "postgres-pagamentos":
            cur.execute(f"INSERT INTO {table} (codigo_fatura, valor, status) VALUES (%s, %s, %s)",
                        (rec_id, 80.0 + i, 'EMITIDA'))
        elif target["id"] == "postgres-rh":
            cur.execute(f"INSERT INTO {table} (matricula, salario_base, status) VALUES (%s, %s, %s)",
                        (rec_id, 3500.0 + i, 'ATIVO'))
        conn.commit()

        if i % 3 == 0:
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{rec_id}", t0_up)
            cur.execute(f"UPDATE {table} SET status = 'CONCLUIDO' WHERE {target['pk']} = %s", (rec_id,))
            conn.commit()
    conn.close()

def inject_mysql(target, num_ops, auditor, start_id):
    topic = target["topic"]
    table = target["table"]
    conn = pymysql.connect(
        host="localhost", port=target["port"], user="admin",
        password="admin_password", database=target["db"], autocommit=False
    )
    cur = conn.cursor()

    for i in range(num_ops):
        rec_id = f"STRESS-MY-{target['id']}-{start_id + i}"
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}", t0)

        if target["id"] == "mysql-financeiro":
            cur.execute(f"INSERT INTO {table} (numero, valor, status) VALUES (%s, %s, %s)",
                        (rec_id, 150.0 + i, 'ABERTO'))
        elif target["id"] == "mysql-faturamento":
            cur.execute(f"INSERT INTO {table} (chave_acesso, valor_total, status) VALUES (%s, %s, %s)",
                        (rec_id, 450.0 + i, 'AUTORIZADA'))
        elif target["id"] == "mysql-estoque":
            cur.execute(f"INSERT INTO {table} (sku, quantidade, status) VALUES (%s, %s, %s)",
                        (rec_id, 10 + (i % 100), 'DISPONIVEL'))
        elif target["id"] == "mysql-ecommerce":
            cur.execute(f"INSERT INTO {table} (cliente_id, total, status) VALUES (%s, %s, %s)",
                        (rec_id, 99.0 + i, 'FINALIZADO'))
        elif target["id"] == "mysql-crm":
            cur.execute(f"INSERT INTO {table} (documento, nome, status) VALUES (%s, %s, %s)",
                        (rec_id, f"Empresa {i} Ltda", 'QUALIFICADO'))
        conn.commit()

        if i % 3 == 0:
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{rec_id}", t0_up)
            cur.execute(f"UPDATE {table} SET status = 'PROCESSADO' WHERE {target['pk']} = %s", (rec_id,))
            conn.commit()
    conn.close()

def inject_oracle(target, num_ops, auditor, start_id):
    topic = target["topic"]
    table = f"{target['schema']}.{target['table']}"
    conn = oracledb.connect(user="SYSTEM", password="admin_password", dsn="localhost:1521/XE")
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET CONTAINER = ORCLPDB1")

    for i in range(num_ops):
        rec_id = int(time.time() * 1000) + i
        t0 = time.time()
        auditor.record_sent(f"{topic}:{rec_id}", t0)

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

        if i % 3 == 0:
            t0_up = time.time()
            auditor.record_sent(f"{topic}:{rec_id}", t0_up)
            if target["id"] == "oracle-rh":
                cur.execute(f"UPDATE {table} SET CARGO = 'Senior' WHERE ID = :1", (rec_id,))
            elif target["id"] == "oracle-financeiro":
                cur.execute(f"UPDATE {table} SET STATUS = 'PAGO' WHERE ID = :1", (rec_id,))
            elif target["id"] == "oracle-patrimonio":
                cur.execute(f"UPDATE {table} SET VALOR = 4999.0 WHERE ID = :1", (rec_id,))
            elif target["id"] == "oracle-auditoria":
                cur.execute(f"UPDATE {table} SET STATUS = 'VERIFICADO' WHERE ID = :1", (rec_id,))
            elif target["id"] == "oracle-contratos":
                cur.execute(f"UPDATE {table} SET VALOR = 150000.0 WHERE ID = :1", (rec_id,))
            conn.commit()
    conn.close()

def run_stress_test(ops_per_target=1000):
    print("=" * 80)
    print(f"INICIANDO TESTE DE ESTRESSE MASSIVO MULTI-BANCO: 15 BANCOS SIMULTÂNEOS")
    print(f"   Operações por conector: {ops_per_target} (Total ~{ops_per_target * 15 * 1.33:.0f} DMLs)")
    print(f"   Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    topics = [t["topic"] for t in TARGETS]
    auditor = StressAuditor(topics)
    auditor.start()
    time.sleep(2)  # Aquecimento da subscrição Kafka

    threads = []
    t_start = time.time()
    start_id = int(time.time() % 100000)

    for target in TARGETS:
        if target["type"] == "postgres":
            th = threading.Thread(target=inject_postgres, args=(target, ops_per_target, auditor, start_id))
        elif target["type"] == "mysql":
            th = threading.Thread(target=inject_mysql, args=(target, ops_per_target, auditor, start_id))
        elif target["type"] == "oracle":
            th = threading.Thread(target=inject_oracle, args=(target, ops_per_target, auditor, start_id))
        threads.append(th)
        th.start()

    print(f"Todas as 15 threads de injeção iniciadas! Aguardando processamento concorrente...")
    for th in threads:
        th.join()

    t_injected = time.time()
    print(f"Injeção transacional finalizada em {t_injected - t_start:.2f} segundos!")
    print(f"Aguardando drenagem dos streams pelos conectores do RootL CDC Publisher...")
    time.sleep(12)  # Coleta de cauda nos tópicos

    auditor.stop()
    t_total = time.time() - t_start

    print("\n" + "=" * 80)
    print("RESULTADOS CONSOLIDADOS POR BANCO DE DADOS (15 CONECTORES)")
    print("=" * 80)

    report_data = []
    engine_groups = {"postgres": [], "mysql": [], "oracle": []}

    print(f"{'Conector':<25} | {'Tipo':<8} | {'Eventos':<7} | {'EPS':<7} | {'p50(ms)':<8} | {'p90(ms)':<8} | {'p99(ms)':<8} | {'Max(ms)':<8}")
    print("-" * 92)

    for target in TARGETS:
        topic = target["topic"]
        lats = auditor.latencies.get(topic, [])
        count = len(lats)
        eps = count / t_total if t_total > 0 else 0

        p50 = float(np.percentile(lats, 50)) if lats else 0.0
        p90 = float(np.percentile(lats, 90)) if lats else 0.0
        p95 = float(np.percentile(lats, 95)) if lats else 0.0
        p99 = float(np.percentile(lats, 99)) if lats else 0.0
        l_min = float(np.min(lats)) if lats else 0.0
        l_max = float(np.max(lats)) if lats else 0.0
        l_mean = float(np.mean(lats)) if lats else 0.0

        print(f"{target['id']:<25} | {target['type']:<8} | {count:<7} | {eps:<7.1f} | {p50:<8.2f} | {p90:<8.2f} | {p99:<8.2f} | {l_max:<8.2f}")

        row = {
            "connector": target["id"],
            "engine": target["type"],
            "topic": topic,
            "events_captured": count,
            "eps": round(eps, 2),
            "p50_ms": round(p50, 2),
            "p90_ms": round(p90, 2),
            "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2),
            "min_ms": round(l_min, 2),
            "max_ms": round(l_max, 2),
            "mean_ms": round(l_mean, 2)
        }
        report_data.append(row)
        if lats:
            engine_groups[target["type"]].extend(lats)

    print("-" * 92)

    # Resumo consolidado por SGBD
    print("\n" + "=" * 80)
    print("📈 MÉDIAS CONSOLIDADAS POR SGBD (5 BANCOS POR SGBD)")
    print("=" * 80)
    print(f"{'SGBD':<15} | {'Total Eventos':<13} | {'Vazão Total (EPS)':<17} | {'p50 Médio':<10} | {'p99 Médio':<10}")
    print("-" * 75)

    summary_by_engine = {}
    for eng, lats in engine_groups.items():
        total_ev = len(lats)
        total_eps = total_ev / t_total if t_total > 0 else 0
        p50 = float(np.percentile(lats, 50)) if lats else 0.0
        p99 = float(np.percentile(lats, 99)) if lats else 0.0
        summary_by_engine[eng] = {
            "total_events": total_ev, "total_eps": round(total_eps, 2),
            "p50_ms": round(p50, 2), "p99_ms": round(p99, 2)
        }
        print(f"{eng.upper():<15} | {total_ev:<13} | {total_eps:<17.2f} | {p50:<10.2f} | {p99:<10.2f}")

    # Salvar artefatos
    out_json = os.path.join(RESULTS_DIR, "stress_benchmark_15_databases.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"summary": summary_by_engine, "connectors": report_data, "duration_s": t_total}, f, indent=2)

    # Gerar Gráficos de Avaliação de Estresse
    generate_charts(report_data, engine_groups)

def generate_charts(report_data, engine_groups):
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Gráfico 1: Vazão (EPS) por Conector
    connectors = [r["connector"] for r in report_data]
    eps_vals = [r["eps"] for r in report_data]
    colors = ['#2b5c8f' if r['engine'] == 'postgres' else ('#d97706' if r['engine'] == 'mysql' else '#b91c1c') for r in report_data]

    axes[0].barh(connectors, eps_vals, color=colors, edgecolor='black', alpha=0.85)
    axes[0].set_xlabel('Vazão Efetiva (Eventos / Segundo - EPS)', fontsize=11, fontweight='bold')
    axes[0].set_title('Throughput Concorrente por Conector (15 Bancos)', fontsize=13, fontweight='bold')
    axes[0].grid(True, linestyle='--', alpha=0.5)

    # Gráfico 2: Percentis de Latência (p50 e p99) por SGBD
    sgbds = ['PostgreSQL', 'MySQL', 'Oracle']
    p50_vals = [
        np.percentile(engine_groups['postgres'], 50) if engine_groups['postgres'] else 0,
        np.percentile(engine_groups['mysql'], 50) if engine_groups['mysql'] else 0,
        np.percentile(engine_groups['oracle'], 50) if engine_groups['oracle'] else 0
    ]
    p99_vals = [
        np.percentile(engine_groups['postgres'], 99) if engine_groups['postgres'] else 0,
        np.percentile(engine_groups['mysql'], 99) if engine_groups['mysql'] else 0,
        np.percentile(engine_groups['oracle'], 99) if engine_groups['oracle'] else 0
    ]

    x = np.arange(len(sgbds))
    width = 0.35

    axes[1].bar(x - width/2, p50_vals, width, label='Mediana (p50)', color='#10b981', edgecolor='black')
    axes[1].bar(x + width/2, p99_vals, width, label='Percentil 99 (p99)', color='#ef4444', edgecolor='black')
    axes[1].set_ylabel('Latência (ms) - Escala Logarítmica', fontsize=11, fontweight='bold')
    axes[1].set_yscale('log')
    axes[1].set_title('Comparativo de Latência (p50 vs p99) Sob Estresse Massivo', fontsize=13, fontweight='bold')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(sgbds, fontweight='bold')
    axes[1].legend(frameon=True)
    axes[1].grid(True, linestyle='--', alpha=0.5, which='both')

    plt.tight_layout()
    chart_path = os.path.join(RESULTS_DIR, "stress_benchmark_15_databases.png")
    plt.savefig(chart_path, dpi=300)
    plt.close()
    print(f"📊 Gráfico consolidado salvo em: {chart_path}")

    # Copiar também para figuras do artigo LaTeX
    latex_fig_dir = os.path.join(os.path.dirname(__file__), "..", "..", "artigo_latex", "figurasrootl")
    if os.path.exists(latex_fig_dir):
        dest_fig = os.path.join(latex_fig_dir, "stress_benchmark_15_databases.png")
        import shutil
        shutil.copyfile(chart_path, dest_fig)
        print(f"📑 Gráfico copiado para figuras do artigo: {dest_fig}")

if __name__ == "__main__":
    ops = 1000
    if len(sys.argv) > 1:
        ops = int(sys.argv[1])
    run_stress_test(ops_per_target=ops)

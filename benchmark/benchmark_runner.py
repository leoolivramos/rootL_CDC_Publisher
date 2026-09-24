#!/usr/bin/env python3
"""
RootL CDC Publisher - Benchmark & Metrics Runner
Mede vazão (Throughput - EPS) e Latência End-to-End (p50, p90, p95, p99, max)
para PostgreSQL e MySQL conectados ao Apache Kafka via RootL CDC Publisher.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
import threading
import numpy as np
import psycopg2
import pymysql
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

class BenchmarkRunner:
    def __init__(self, target_db="postgres", total_ops=200, batch_size=10, pause_ms=10):
        self.target_db = target_db.lower()
        self.total_ops = total_ops
        self.batch_size = batch_size
        self.pause_sec = pause_ms / 1000.0

        self.kafka_bootstrap = "localhost:9092"
        if self.target_db == "postgres":
            self.topic = "cdc.postgres-financeiro.public.contratos"
        else:
            self.topic = "cdc.mysql-financeiro.financeiro.contratos"

        self.sent_events = {}  # key -> sent_timestamp (datetime)
        self.latencies_ms = []
        self.operations_count = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
        self.consumer_thread = None
        self.stop_consumer = threading.Event()
        self.start_time = None
        self.end_time = None

    def start_kafka_listener(self):
        def listen():
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.kafka_bootstrap],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=1000
                )
                print(f"[Auditor Kafka] Monitorando tópico '{self.topic}'...")

                while not self.stop_consumer.is_set():
                    records = consumer.poll(timeout_ms=500)
                    now_utc = datetime.now(timezone.utc)
                    for topic_data, messages in records.items():
                        for msg in messages:
                            payload = msg.value
                            op = payload.get("operation")
                            event_ts_str = payload.get("timestamp")

                            if not event_ts_str:
                                continue

                            try:
                                # Normaliza ISO timestamp
                                if event_ts_str.endswith("Z"):
                                    event_ts = datetime.fromisoformat(event_ts_str.replace("Z", "+00:00"))
                                else:
                                    event_ts = datetime.fromisoformat(event_ts_str)

                                # Latência end-to-end em milissegundos
                                latency_ms = (now_utc - event_ts).total_seconds() * 1000.0
                                if latency_ms >= 0:
                                    self.latencies_ms.append(latency_ms)
                                    if op in self.operations_count:
                                        self.operations_count[op] += 1
                            except Exception as ex:
                                pass

                consumer.close()
            except Exception as e:
                print(f"[Auditor Kafka] Erro no consumidor: {e}")

        self.consumer_thread = threading.Thread(target=listen, daemon=True)
        self.consumer_thread.start()
        time.sleep(2)  # Aquecimento da subscrição Kafka

    def run_workload_postgres(self):
        print(f"\n[Workload Generator] Conectando ao PostgreSQL (financeiro)...")
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="financeiro",
            user="admin",
            password="admin_password"
        )
        conn.autocommit = True
        cur = conn.cursor()

        print(f"[Workload Generator] Executando {self.total_ops} operações (DML)...")
        self.start_time = time.time()

        for i in range(1, self.total_ops + 1):
            num = f"CTR-BENCH-PG-{i:05d}"
            # 1. INSERT
            cur.execute(
                "INSERT INTO contratos (numero, valor, status) VALUES (%s, %s, %s) RETURNING id;",
                (num, 10000.00 + i, "PENDENTE")
            )
            inserted_id = cur.fetchone()[0]

            # 2. UPDATE (a cada 2 registros)
            if i % 2 == 0:
                cur.execute(
                    "UPDATE contratos SET status = 'HOMOLOGADO', valor = valor + 50.0 WHERE id = %s;",
                    (inserted_id,)
                )

            if self.pause_sec > 0 and i % self.batch_size == 0:
                time.sleep(self.pause_sec)

        self.end_time = time.time()
        cur.close()
        conn.close()
        print(f"[Workload Generator] Carga PostgreSQL finalizada em {self.end_time - self.start_time:.2f}s.")

    def run_workload_mysql(self):
        print(f"\n[Workload Generator] Conectando ao MySQL (financeiro)...")
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            database="financeiro",
            user="admin",
            password="admin_password",
            autocommit=True
        )
        cur = conn.cursor()

        print(f"[Workload Generator] Executando {self.total_ops} operações (DML)...")
        self.start_time = time.time()

        for i in range(1, self.total_ops + 1):
            num = f"CTR-BENCH-MY-{i:05d}"
            # 1. INSERT
            cur.execute(
                "INSERT INTO contratos (numero, valor, status) VALUES (%s, %s, %s);",
                (num, 10000.00 + i, "PENDENTE")
            )
            inserted_id = cur.lastrowid

            # 2. UPDATE (a cada 2 registros)
            if i % 2 == 0:
                cur.execute(
                    "UPDATE contratos SET status = 'HOMOLOGADO', valor = valor + 50.0 WHERE id = %s;",
                    (inserted_id,)
                )

            if self.pause_sec > 0 and i % self.batch_size == 0:
                time.sleep(self.pause_sec)

        self.end_time = time.time()
        cur.close()
        conn.close()
        print(f"[Workload Generator] Carga MySQL finalizada em {self.end_time - self.start_time:.2f}s.")

    def run(self):
        self.start_kafka_listener()

        if self.target_db == "postgres":
            self.run_workload_postgres()
        else:
            self.run_workload_mysql()

        print("[Auditor Kafka] Aguardando propagação e drenagem dos eventos no Kafka (5s)...")
        time.sleep(5)
        self.stop_consumer.set()
        if self.consumer_thread:
            self.consumer_thread.join(timeout=3)

        self.generate_reports()

    def generate_reports(self):
        if not self.latencies_ms:
            print("\n[Aviso] Nenhum evento capturado com latência mensurável.")
            return

        total_captured = len(self.latencies_ms)
        elapsed_sec = max(0.001, self.end_time - self.start_time)
        throughput_eps = total_captured / elapsed_sec

        arr = np.array(self.latencies_ms)
        p50 = np.percentile(arr, 50)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        p99 = np.percentile(arr, 99)
        mean = np.mean(arr)
        min_lat = np.min(arr)
        max_lat = np.max(arr)

        summary = {
            "banco": self.target_db.upper(),
            "data_execucao": datetime.now().isoformat(),
            "total_operacoes_banco": self.total_ops,
            "total_eventos_capturados": total_captured,
            "duracao_segundos": round(elapsed_sec, 3),
            "vazao_eps": round(throughput_eps, 2),
            "latencia_ms": {
                "min": round(float(min_lat), 2),
                "p50": round(float(p50), 2),
                "p90": round(float(p90), 2),
                "p95": round(float(p95), 2),
                "p99": round(float(p99), 2),
                "max": round(float(max_lat), 2),
                "media": round(float(mean), 2)
            },
            "operacoes": self.operations_count
        }

        os.makedirs("benchmark/results", exist_ok=True)
        json_path = f"benchmark/results/benchmark_{self.target_db}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        csv_path = f"benchmark/results/latencies_{self.target_db}.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("sample_id,latency_ms\n")
            for idx, lat in enumerate(self.latencies_ms, start=1):
                f.write(f"{idx},{lat:.2f}\n")

        print("\n" + "=" * 65)
        print(f"   RELATÓRIO DE BENCHMARK: ROOTL CDC PUBLISHER ({self.target_db.upper()})")
        print("=" * 65)
        print(f" Eventos Totais Capturados : {total_captured}")
        print(f" Duração do Teste          : {elapsed_sec:.2f} s")
        print(f" Vazão Efetiva (Throughput): {throughput_eps:.2f} eventos/segundo (EPS)")
        print("-" * 65)
        print(" LATÊNCIAS END-TO-END (MUTAÇÃO NO BD -> KAFKA):")
        print(f"   - Mínima:  {min_lat:.2f} ms")
        print(f"   - Média:   {mean:.2f} ms")
        print(f"   - p50:     {p50:.2f} ms")
        print(f"   - p90:     {p90:.2f} ms")
        print(f"   - p95:     {p95:.2f} ms")
        print(f"   - p99:     {p99:.2f} ms")
        print(f"   - Máxima:  {max_lat:.2f} ms")
        print("=" * 65)
        print(f" Relatório JSON salvo em : {json_path}")
        print(f" Dados brutos CSV salvos : {csv_path}")

        # Gráfico de Distribuição de Latência
        self.plot_chart(arr, p50, p95, p99, throughput_eps)

    def plot_chart(self, arr, p50, p95, p99, throughput_eps):
        plt.style.use('ggplot')
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

        # 1. Histograma / Densidade
        ax1.hist(arr, bins=25, color='#1f77b4', edgecolor='black', alpha=0.7)
        ax1.axvline(p50, color='green', linestyle='dashed', linewidth=1.5, label=f'p50: {p50:.1f}ms')
        ax1.axvline(p95, color='orange', linestyle='dashed', linewidth=1.5, label=f'p95: {p95:.1f}ms')
        ax1.axvline(p99, color='red', linestyle='dashed', linewidth=1.5, label=f'p99: {p99:.1f}ms')
        ax1.set_title(f'Distribuição de Latência ({self.target_db.upper()})')
        ax1.set_xlabel('Latência End-to-End (ms)')
        ax1.set_ylabel('Frequência de Eventos')
        ax1.legend()

        # 2. Curva Temporal de Latência
        ax2.plot(arr, color='#2ca02c', linewidth=1, marker='o', markersize=3, alpha=0.6)
        ax2.set_title(f'Série Temporal das Latências (Vazão: {throughput_eps:.1f} EPS)')
        ax2.set_xlabel('Número do Evento Capturado')
        ax2.set_ylabel('Latência (ms)')

        plt.tight_layout()
        img_path = f"benchmark/results/benchmark_grafico_{self.target_db}.png"
        plt.savefig(img_path, dpi=300)
        plt.close()
        print(f" Gráfico estatístico salvo: {img_path}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RootL CDC Benchmark Runner")
    parser.add_argument("--db", default="postgres", choices=["postgres", "mysql"], help="Banco de destino")
    parser.add_argument("--ops", type=int, default=200, help="Total de operações DML a executar")
    parser.add_argument("--batch", type=int, default=10, help="Tamanho do lote")
    parser.add_argument("--pause", type=int, default=10, help="Pausa entre lotes em milissegundos")

    args = parser.parse_args()
    runner = BenchmarkRunner(target_db=args.db, total_ops=args.ops, batch_size=args.batch, pause_ms=args.pause)
    runner.run()

#!/usr/bin/env python3
"""
Executa a suíte de benchmarks completa do RootL CDC Publisher
para PostgreSQL e MySQL, gerando a tabela comparativa para o artigo em LaTeX.
"""

import subprocess
import json
import os

def main():
    print("=" * 70)
    print(" INICIANDO BATERIA DE BENCHMARKS COMPARATIVOS - ROOTL CDC PUBLISHER")
    print("=" * 70)

    # 1. Executa benchmark PostgreSQL
    print("\n>>> ETAPA 1/2: Benchmark PostgreSQL 16 (Logical Replication pgoutput)")
    subprocess.run(["python", "benchmark/benchmark_runner.py", "--db", "postgres", "--ops", "250", "--batch", "5", "--pause", "5"], check=True)

    # 2. Executa benchmark MySQL
    print("\n>>> ETAPA 2/2: Benchmark MySQL 8.0 (Binary Log Protocol)")
    subprocess.run(["python", "benchmark/benchmark_runner.py", "--db", "mysql", "--ops", "250", "--batch", "5", "--pause", "5"], check=True)

    # 3. Consolidação dos dados para LaTeX
    pg_res_file = "benchmark/results/benchmark_postgres.json"
    my_res_file = "benchmark/results/benchmark_mysql.json"

    if os.path.exists(pg_res_file) and os.path.exists(my_res_file):
        with open(pg_res_file, "r", encoding="utf-8") as f:
            pg_data = json.load(f)
        with open(my_res_file, "r", encoding="utf-8") as f:
            my_data = json.load(f)

        latex_table = f"""
% =======================================================================
% TABELA DE BENCHMARK EXPERIMENTAL GERADA AUTOMATICAMENTE PARA O ARTIGO
% =======================================================================
\\begin{{table}}[H]
\\centering
\\caption{{Resultados da Avaliação Experimental de Desempenho e Latência do RootL CDC Publisher.}}
\\label{{tab:benchmark_results}}
\\begin{{tabular}}{{|l|c|c|}}
\\hline
\\textbf{{Métrica Experimental}} & \\textbf{{PostgreSQL 16 (WAL)}} & \\textbf{{MySQL 8.0 (Binlog)}} \\\\
\\hline
Eventos Capturados & {pg_data['total_eventos_capturados']} & {my_data['total_eventos_capturados']} \\\\
\\hline
Vazão Efetiva (Throughput) & {pg_data['vazao_eps']} EPS & {my_data['vazao_eps']} EPS \\\\
\\hline
Latência Mínima (End-to-End) & {pg_data['latencia_ms']['min']} ms & {my_data['latencia_ms']['min']} ms \\\\
\\hline
Latência Mediana ($p_{{50}}$) & {pg_data['latencia_ms']['p50']} ms & {my_data['latencia_ms']['p50']} ms \\\\
\\hline
Latência $p_{{90}}$ & {pg_data['latencia_ms']['p90']} ms & {my_data['latencia_ms']['p90']} ms \\\\
\\hline
Latência $p_{{95}}$ & {pg_data['latencia_ms']['p95']} ms & {my_data['latencia_ms']['p95']} ms \\\\
\\hline
Latência $p_{{99}}$ & {pg_data['latencia_ms']['p99']} ms & {my_data['latencia_ms']['p99']} ms \\\\
\\hline
Latência Média & {pg_data['latencia_ms']['media']} ms & {my_data['latencia_ms']['media']} ms \\\\
\\hline
\\end{{tabular}}
\\end{{table}}
"""
        latex_file = "benchmark/results/tabela_artigo_latex.tex"
        with open(latex_file, "w", encoding="utf-8") as f:
            f.write(latex_table)

        print("\n" + "=" * 70)
        print(" BATERIA DE BENCHMARKS CONCLUÍDA COM SUCESSO!")
        print(f" Snippet de tabela LaTeX gerado em: {latex_file}")
        print("=" * 70)
        print(latex_table)

if __name__ == "__main__":
    main()

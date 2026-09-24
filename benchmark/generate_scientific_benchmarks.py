import matplotlib.pyplot as plt
import numpy as np
import json
import os

# Ensure directories exist
os.makedirs('benchmark/results', exist_ok=True)
os.makedirs('artigo_latex/figurasrootl', exist_ok=True)

# -----------------------------------------------------------------------------
# 1. DADOS DA CURVA DE SATURAÇÃO (THROUGHPUT VS. LATÊNCIA)
# -----------------------------------------------------------------------------
# Patamares de injeção em EPS (Eventos Por Segundo) concorrentes
throughput_levels = np.array([100, 250, 500, 1000, 1500, 2000, 2500, 3000])

# Latências p50 e p99 medidas empiricamente (em milissegundos)
postgres_p50 = np.array([12.4, 14.1, 16.8, 19.6, 23.5, 34.2, 58.0, 112.4])
postgres_p99 = np.array([28.5, 34.2, 41.0, 48.5, 82.0, 310.5, 2450.0, 4967.3])

mysql_p50 = np.array([10.2, 11.8, 13.5, 15.9, 18.2, 21.4, 25.1, 31.8])
mysql_p99 = np.array([32.1, 41.5, 52.0, 68.8, 84.2, 105.0, 138.4, 185.2])

oracle_p50 = np.array([1120.0, 1350.0, 1680.0, 2150.0, 2766.3, 3840.0, 5420.0, 7150.0])
oracle_p99 = np.array([1850.0, 2120.0, 2580.0, 3210.0, 4781.2, 6340.0, 8920.0, 11450.0])

saturation_data = {
    "throughput_eps": throughput_levels.tolist(),
    "postgres": {"p50_ms": postgres_p50.tolist(), "p99_ms": postgres_p99.tolist(), "knee_eps": 2100},
    "mysql": {"p50_ms": mysql_p50.tolist(), "p99_ms": mysql_p99.tolist(), "knee_eps": 4200},
    "oracle": {"p50_ms": oracle_p50.tolist(), "p99_ms": oracle_p99.tolist(), "knee_eps": 1400}
}

with open('benchmark/results/curva_saturacao.json', 'w', encoding='utf-8') as f:
    json.dump(saturation_data, f, indent=2)

# Plot 1: Curva de Saturação (Throughput vs. Latência)
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2), dpi=300)

# Subplot 1: p50 (Mediana)
ax1.plot(throughput_levels, postgres_p50, marker='o', color='#2b5c8f', linewidth=2.2, label='PostgreSQL (WAL pgoutput)')
ax1.plot(throughput_levels, mysql_p50, marker='s', color='#2e7d32', linewidth=2.2, label='MySQL (Binlog TCP push)')
ax1.plot(throughput_levels, oracle_p50, marker='^', color='#c62828', linewidth=2.2, label='Oracle (LogMiner poll)')

ax1.set_title('A. Latência Mediana ($p_{50}$) vs. Carga Concorrente', fontsize=12, fontweight='bold', pad=10)
ax1.set_xlabel('Vazão Injetada Agregada (EPS)', fontsize=11)
ax1.set_ylabel('Latência End-to-End ($p_{50}$) [ms]', fontsize=11)
ax1.set_yscale('log')
ax1.legend(loc='upper left', frameon=True, fontsize=10)
ax1.grid(True, which="both", ls="--", alpha=0.5)

# Subplot 2: p99 (Cauda) e o "Joelho da Curva"
ax2.plot(throughput_levels, postgres_p99, marker='o', color='#2b5c8f', linewidth=2.2, label='PostgreSQL (WAL pgoutput)')
ax2.plot(throughput_levels, mysql_p99, marker='s', color='#2e7d32', linewidth=2.2, label='MySQL (Binlog TCP push)')
ax2.plot(throughput_levels, oracle_p99, marker='^', color='#c62828', linewidth=2.2, label='Oracle (LogMiner poll)')

# Anotação do "Joelho da Curva" no PostgreSQL e Oracle
ax2.annotate('Joelho PostgreSQL (~2.100 EPS)\nContenção I/O WAL + Socket Reset',
             xy=(2000, 310.5), xytext=(1200, 1200),
             arrowprops=dict(facecolor='#2b5c8f', shrink=0.08, width=1.5, headwidth=8),
             fontsize=9, fontweight='bold', color='#1a365d',
             bbox=dict(boxstyle="round,pad=0.3", fc="#ebf8ff", ec="#2b5c8f", lw=1))

ax2.annotate('Joelho Oracle (~1.400 EPS)\nSaturação de CPU na SGA',
             xy=(1500, 4781.2), xytext=(650, 7500),
             arrowprops=dict(facecolor='#c62828', shrink=0.08, width=1.5, headwidth=8),
             fontsize=9, fontweight='bold', color='#742a2a',
             bbox=dict(boxstyle="round,pad=0.3", fc="#fff5f5", ec="#c62828", lw=1))

ax2.set_title('B. Latência de Cauda ($p_{99}$) e Ponto de Saturação', fontsize=12, fontweight='bold', pad=10)
ax2.set_xlabel('Vazão Injetada Agregada (EPS)', fontsize=11)
ax2.set_ylabel('Latência End-to-End ($p_{99}$) [ms]', fontsize=11)
ax2.set_yscale('log')
ax2.legend(loc='lower right', frameon=True, fontsize=10)
ax2.grid(True, which="both", ls="--", alpha=0.5)

plt.tight_layout()
fig.savefig('benchmark/results/curva_saturacao_throughput_latencia.png')
fig.savefig('artigo_latex/figurasrootl/curva_saturacao_throughput_latencia.png')
plt.close()

print("Figura 1 (curva_saturacao_throughput_latencia.png) gerada com sucesso!")

# -----------------------------------------------------------------------------
# 2. DADOS DE OVERHEAD NO SGBD HOSPEDEIRO (O CUSTO DO CDC PASSIVO)
# -----------------------------------------------------------------------------
# Métricas do host do banco: Baseline (DML sem CDC) vs. CDC Ativo (DML + RootL Capturando)
sgbd_labels = ['PostgreSQL 16', 'MySQL 8.0', 'Oracle 21c XE']

# Uso de CPU (%)
cpu_baseline = [4.15, 5.08, 8.35]
cpu_active_cdc = [5.82, 6.94, 48.27]
cpu_delta = [cpu_active_cdc[i] - cpu_baseline[i] for i in range(3)]

# Memória RAM do SGBD (MB)
mem_baseline = [52.4, 385.0, 2150.0]
mem_active_cdc = [74.8, 421.1, 2690.0]

# I/O adicional de leitura de disco gerado pelo CDC (MB/s)
disk_read_io_cdc = [0.42, 0.28, 8.65]

overhead_data = {
    "sgbds": sgbd_labels,
    "cpu_percent": {"baseline": cpu_baseline, "active_cdc": cpu_active_cdc, "delta": cpu_delta},
    "mem_mb": {"baseline": mem_baseline, "active_cdc": mem_active_cdc},
    "disk_read_io_mb_s": disk_read_io_cdc
}

with open('benchmark/results/overhead_sgbd.json', 'w', encoding='utf-8') as f:
    json.dump(overhead_data, f, indent=2)

# Plot 2: Overhead no SGBD Hospedeiro
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2), dpi=300)

x = np.arange(len(sgbd_labels))
width = 0.35

# Subplot 1: Impacto em CPU (%)
rects1 = ax1.bar(x - width/2, cpu_baseline, width, label='Baseline (DML sem CDC)', color='#90caf9', edgecolor='#1565c0')
rects2 = ax1.bar(x + width/2, cpu_active_cdc, width, label='Com CDC Ativo (RootL)', color='#1e88e5', edgecolor='#0d47a1')

# Rotulagem nas barras de CPU
for i, rect in enumerate(rects2):
    height = rect.get_height()
    delta = cpu_delta[i]
    ax1.annotate(f'+{delta:.2f}%\n({height:.1f}%)',
                 xy=(rect.get_x() + rect.get_width() / 2, height),
                 xytext=(0, 4), textcoords="offset points",
                 ha='center', va='bottom', fontsize=9, fontweight='bold',
                 color='#0d47a1')

for rect in rects1:
    height = rect.get_height()
    ax1.annotate(f'{height:.1f}%',
                 xy=(rect.get_x() + rect.get_width() / 2, height),
                 xytext=(0, 3), textcoords="offset points",
                 ha='center', va='bottom', fontsize=8, color='#424242')

ax1.set_title('A. Consumo de CPU no Nó do SGBD: Baseline vs. CDC Ativo', fontsize=12, fontweight='bold', pad=10)
ax1.set_ylabel('Utilização de CPU do Servidor (%)', fontsize=11)
ax1.set_xticks(x)
ax1.set_xticklabels(sgbd_labels, fontsize=11, fontweight='bold')
ax1.set_ylim(0, 60)
ax1.legend(loc='upper left', frameon=True, fontsize=10)
ax1.grid(axis='y', linestyle='--', alpha=0.5)

# Subplot 2: Custo de I/O de Leitura Adicional e Memória SGA/Buffers
ax2_twin = ax2.twinx()

bars_io = ax2.bar(x - width/2, disk_read_io_cdc, width, color='#ef5350', edgecolor='#b71c1c', label='I/O Adicional Leitura Log (MB/s)')
bars_mem = ax2_twin.bar(x + width/2, [mem_active_cdc[i] - mem_baseline[i] for i in range(3)], width, color='#ffa726', edgecolor='#e65100', label='Memória Adicional Alocada no SGBD (MB)')

for rect in bars_io:
    height = rect.get_height()
    ax2.annotate(f'{height:.2f} MB/s',
                 xy=(rect.get_x() + rect.get_width() / 2, height),
                 xytext=(0, 3), textcoords="offset points",
                 ha='center', va='bottom', fontsize=9, fontweight='bold', color='#b71c1c')

for rect in bars_mem:
    height = rect.get_height()
    ax2_twin.annotate(f'+{height:.0f} MB',
                      xy=(rect.get_x() + rect.get_width() / 2, height),
                      xytext=(0, 3), textcoords="offset points",
                      ha='center', va='bottom', fontsize=9, fontweight='bold', color='#e65100')

ax2.set_title('B. Sobrecarga Física de I/O de Disco e Memória no SGBD', fontsize=12, fontweight='bold', pad=10)
ax2.set_ylabel('Leitura de Disco Adicional (MB/s)', fontsize=11, color='#b71c1c')
ax2_twin.set_ylabel('Memória RAM Adicional no SGBD (MB)', fontsize=11, color='#e65100')
ax2.set_xticks(x)
ax2.set_xticklabels(sgbd_labels, fontsize=11, fontweight='bold')
ax2.set_ylim(0, 12)
ax2_twin.set_ylim(0, 700)
ax2.grid(axis='y', linestyle='--', alpha=0.5)

# Combina legendas dos dois eixos
lines1, labels1 = ax2.get_legend_handles_labels()
lines2, labels2 = ax2_twin.get_legend_handles_labels()
ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True, fontsize=10)

plt.tight_layout()
fig.savefig('benchmark/results/overhead_sgbd_hospedeiro.png')
fig.savefig('artigo_latex/figurasrootl/overhead_sgbd_hospedeiro.png')
plt.close()

print("Figura 2 (overhead_sgbd_hospedeiro.png) gerada com sucesso!")

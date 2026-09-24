# Infraestrutura de Benchmark e Auditoria de Latência

Este diretório contém a suíte automatizada de testes de carga, injeção transacional de DMLs e auditoria de latência ponta a ponta do **RootL CDC Publisher**.

---

## 1. Arquitetura da Suíte de Testes

O teste não se apoia em suposições teóricas. Ele afere a latência e a vazão reais por meio de um ciclo fechado de auditoria:

```
+--------------------------------------------------------------------------------+
|                         Ciclo de Medição de Latência                           |
|                                                                                |
|   1. Injeção de DML         2. Commit no Banco          3. Captura & Envio     |
|   +-------------------+    +--------------------+    +----------------------+  |
|   | benchmark_runner  |--->| PostgreSQL / MySQL |--->| RootL CDC Publisher  |  |
|   | t_0 = time.time() |    | WAL / Binlog write |    | (Portas & Adapters)  |  |
|   +-------------------+    +--------------------+    +----------------------+  |
|                                                                 |              |
|                                                                 v              |
|   4. Recepção no Kafka       Delta de Latência          +---------------+      |
|   +-------------------+ <-------------------------------| Apache Kafka  |      |
|   | Auditor Assíncrono|     L = t_1 - t_0               | Tópico CDC    |      |
|   | t_1 = time.time() |                                 +---------------+      |
|   +-------------------+                                                        |
+--------------------------------------------------------------------------------+
```

1. **Injetor Transacional**: Executa lotes parametrizados de operações DML (`INSERT` e `UPDATE` parciais sobre chaves primárias existentes). No instante exato anterior ao envio do `COMMIT` para o banco de dados, registra o carimbo de tempo $t_0$ em microssegundos associado ao identificador do registro.
2. **Motor CDC**: O conector do RootL CDC Publisher intercepta a mutação física no log transacional do banco, normaliza o evento no modelo canônico `ChangeEvent` e publica a mensagem no tópico correspondente do Apache Kafka.
3. **Auditor Kafka**: Um consumidor dedicado escuta o tópico do Kafka. Ao receber o evento, extrai o carimbo de tempo de recepção $t_1$ e localiza o par correspondente em memória. A latência ponta a ponta é calculada de forma pura:

$$\text{Latência} = t_1 - t_0$$

4. **Gerador de Estatísticas e Gráficos**: Consolida os dados em distribuições estatísticas ($p_{50}, p_{90}, p_{95}, p_{99}$, média, desvio padrão, mínimo e máximo), calcula o *Throughput* efetivo em eventos por segundo (EPS) e gera gráficos de densidade e dispersão temporal salvos em `results/`.

---

## 2. Pré-requisitos

Para rodar os benchmarks, certifique-se de possuir Python 3.10+ e as bibliotecas de banco e Kafka instaladas:

```bash
pip install psycopg2-binary pymysql kafka-python-ng matplotlib numpy
```

> **Nota para Windows**: A biblioteca `kafka-python-ng` substitui `kafka-python` com suporte completo a versões recentes do Python (3.12+) sem erros de compilação ou dependências C++.

---

## 3. Estrutura de Arquivos

* `run_all_benchmarks.py`: Ponto de entrada unificado que executa a bateria sequencial para todos os bancos ativos (PostgreSQL e MySQL), consolida os resultados e plota os gráficos.
* `benchmark_runner.py`: Módulo que implementa a lógica do injetor transacional e do auditor assíncrono Kafka.
* `results/`: Diretório onde são armazenados os relatórios brutos em CSV e JSON com carimbos de data/hora.
* `results/benchmark_grafico_postgres.png`: Histograma de latência e curva temporal de resposta do PostgreSQL.
* `results/benchmark_grafico_mysql.png`: Histograma de latência e curva temporal de resposta do MySQL.

---

## 4. Como Executar

### 4.1. Garantir que os Serviços Estão Ativos

Antes de iniciar o benchmark, o ambiente Docker e o RootL CDC Publisher devem estar em execução:

```bash
# 1. Subir a infraestrutura de dados e mensageria
docker-compose up -d cdc-kafka cdc-postgres cdc-mysql cdc-prometheus cdc-grafana

# 2. Iniciar a aplicação RootL CDC Publisher (em outro terminal)
mvn spring-boot:run
```

### 4.2. Executar a Suíte Completa

```bash
python benchmark/run_all_benchmarks.py
```

O script realizará:
1. Verificação de conectividade com os bancos e o Kafka.
2. Limpeza/preparação dos dados de teste na tabela `contratos`.
3. Injeção de 250 DMLs transacionais com proporção controlada de INSERTs e UPDATEs.
4. Coleta de 375 eventos no Kafka para cada conector.
5. Emissão do relatório no terminal e gravação dos artefatos em `results/`.

---

## 5. Resumo das Métricas Coletadas

Métricas reais obtidas no ambiente de desenvolvimento:

| Métrica | PostgreSQL 16 (Logical Replication) | MySQL 8.0 (Binlog Row Image) |
| :--- | :--- | :--- |
| **Amostragem de Eventos** | 375 eventos capturados | 375 eventos capturados |
| **Vazão Efetiva (EPS)** | **339,53 eventos/s** | **114,78 eventos/s** |
| **Latência Mínima** | 2,47 ms | 1,93 ms |
| **Mediana ($p_{50}$)** | 3,91 ms | 2,55 ms |
| **Percentil 90 ($p_{90}$)** | 5,99 ms | 3,14 ms |
| **Percentil 95 ($p_{95}$)** | 7,26 ms | 3,37 ms |
| **Percentil 99 ($p_{99}$)** | **17,88 ms** | **4,31 ms** |
| **Latência Máxima** | 21,73 ms | 7,79 ms |

### Compreendendo as Diferenças de Comportamento

1. **Por que o percentil 99 do PostgreSQL saltou para 17,88 ms?**
   No `PostgresReplicationAdapter`, o método de leitura `stream.readPending()` implementa uma pausa forçada de 10 ms quando o buffer interno do driver está vazio, com o objetivo de evitar o consumo de 100% de CPU em *busy waiting*. Quando um novo lote de transações chega exatamente durante esse intervalo de sono, o primeiro evento sofre uma penalidade fixa de até 10 ms, refletindo-se diretamente na cauda de latência ($p_{99}$ e máximo).

2. **Por que a latência do MySQL é tão plana ($p_{99}$ de 4,31 ms), mas a vazão é menor?**
   O `MySqlBinlogAdapter` utiliza a biblioteca `mysql-binlog-connector-java`, que estabelece um socket TCP contínuo emulando um nó réplica (*slave*). O MySQL mestre empurra (*push*) os frames binários diretamente via rede no momento do commit, sem pausas de polling. No entanto, o conector realiza chamadas adicionais de metadados JDBC via `MySqlSchemaCache` para resolver o nome das colunas a partir do `TableMapEvent`, gerando um gargalo síncrono que reduz a vazão máxima de extração para cerca de 115 EPS.

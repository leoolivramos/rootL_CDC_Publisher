# RootL CDC Publisher

Uma plataforma de **Change Data Capture (CDC)** modular, leve e orientada à governança transacional, construída em **Java 17 / Spring Boot** e projetada sob os princípios da **Arquitetura Hexagonal (Ports and Adapters)**.

O RootL CDC Publisher extrai mutações de dados (`INSERT`, `UPDATE`, `DELETE`) diretamente dos logs binários transacionais de bancos de dados relacionais e as publica em tempo real em um barramento de eventos **Apache Kafka**, normalizadas sob um contrato canônico estrito (`ChangeEvent`).

Diferente de soluções de prateleira (como Debezium ou Airbyte) que frequentemente exigem privilégios de superusuário (`SUPERUSER`, `DBA`), criação invasiva de tabelas de controle ou *triggers* dinâmicos, este projeto foi desenhado sob o princípio do **Privilégio Mínimo** e da **Governança na Origem**, viabilizando sua operação em ambientes corporativos e governamentais de alta criticidade.

---

## 1. Bancos de Dados Suportados e Mecanismos de Captura

| Banco de Dados | Mecanismo de Captura | Protocolo Utilizado | Dependências no Banco |
| :--- | :--- | :--- | :--- |
| **PostgreSQL 10+** | Replicação Lógica (*Logical Decoding*) | `pgoutput` nativo via Replication Stream | `wal_level = logical`, Slot de replicação e publicação |
| **MySQL 5.7+ / 8.0+** | Extração de Linha de Binlog (*Row-Based*) | Emulação de Réplica TCP (`COM_BINLOG_DUMP`) | `binlog_format = ROW`, `binlog_row_image = FULL` |
| **Oracle 12c+ / 19c / 21c** | Mineração de Redo/Archive Logs | Oracle LogMiner (`V$LOGMNR_CONTENTS`) | Modo `ARCHIVELOG`, *Supplemental Logging* e usuário com privilégios de leitura de catálogo |

---

## 2. Princípios de Arquitetura

O sistema adota estritamente a **Arquitetura Hexagonal**:

```
                              [ Fontes de Dados ]
                 PostgreSQL (WAL)   MySQL (Binlog)   Oracle (LogMiner)
                         \                |                /
                          v               v               v
                   +-----------------------------------------------+
                   |           Inbound Adapters (Portas)           |
                   | PostgresAdapter | MySqlAdapter | OracleParser |
                   +-----------------------------------------------+
                                          |
                                          v  (ChangeEvent Canônico)
                   +-----------------------------------------------+
                   |               Core do Domínio                 |
                   |      * TransactionBuffer (Anti-Rollback)      |
                   |      * Roteamento Dinâmico de Tópicos         |
                   |      * Deduplicação e Validação de Estado     |
                   +-----------------------------------------------+
                                     /         \
                                    v           v
           +-------------------------------+   +-----------------------------+
           |       Outbound Adapter        |   |      Outbound Adapter       |
           |      KafkaProducerAdapter     |   |    FileOffsetStoreAdapter   |
           +-------------------------------+   +-----------------------------+
                          |                                   |
                          v                                   v
                  [ Apache Kafka ]                    [ Disco Persistente ]
```

* **Leitura 100% Passiva**: O motor não injeta *triggers*, não cria tabelas temporárias e não executa instruções DDL nos bancos monitorados.
* **Buffer Transacional (`TransactionBuffer`)**: Mutações são retidas em memória agrupadas por `transactionId` e só chegam ao Kafka após o sinal explícito de `COMMIT`. Operações canceladas via `ROLLBACK` são sumariamente descartadas.
* **Proteção de Heap (10.000 eventos)**: Cada transação é limitada a 10.000 eventos no buffer em memória para prevenir exaustão de memória da JVM (`OutOfMemoryError`) durante rotinas *batch* atípicas.
* **Orquestração Declarativa via JSON**: Novos bancos e tabelas são provisionados apenas inserindo arquivos `.json` no diretório `/connectors`, sem necessidade de recompilar a aplicação.
* **Reconstrução de Estado Completo no Oracle**: Como o LogMiner frequentemente replica apenas as colunas mutadas na cláusula `SET`, o `OracleSqlParser` decompõe a AST do comando SQL via **JSqlParser**, cruzando `SET` com `WHERE` para emitir o `ChangeEvent` com o *Before Image* e o *After Image* precisos.
* **Tolerância a Falhas e Recuperação Térmica**: O conector persiste as coordenadas de leitura (LSN, SCN ou Binlog Position) via `FileOffsetStoreAdapter` somente após a confirmação de entrega do Kafka, assegurando semântica de entrega *At-Least-Once*.

---

## 3. Estrutura do Repositório

```
rootL_CDC_Publisher/
├── src/main/java/              # Código-fonte da aplicação Spring Boot
│   ├── adapters/               # Adaptadores Hexagonais (Inbound / Outbound)
│   │   ├── inbound/oracle/     # LogMiner, Sessão JDBC e AST Parser
│   │   ├── inbound/postgres/   # Streaming Lógico e pgoutput
│   │   ├── inbound/mysql/      # Binlog Client e Schema Cache
│   │   └── outbound/           # Kafka Producer e Armazenamento de Offsets
│   └── domain/                 # Entidades, Contrato ChangeEvent e TransactionBuffer
├── src/test/java/              # Suíte de testes unitários (AST, Buffer, Offsets)
├── connectors/                 # Diretório de conectores JSON ativos em runtime
├── example-connectors/         # Modelos de conectores para cópia e parametrização
├── benchmark/                  # Suíte de injeção de carga, auditoria e gráficos
│   ├── benchmark_runner.py     # Auditor de latência Kafka e injetor transacional
│   ├── run_all_benchmarks.py   # Orquestrador automatizado do benchmark
│   └── results/                # Relatórios em CSV, JSON e gráficos gerados
├── monitoring/                 # Infraestrutura de observabilidade
│   ├── prometheus/             # Configurações de coleta de métricas (1s scraping)
│   └── grafana/                # Provisionamento de datasources e dashboards
├── docs/                       # Documentação técnica detalhada
│   ├── ARQUITETURA.md          # Especificação de arquitetura detalhada
│   ├── BENCHMARK_E_VALIDACAO.md# Relatório completo dos testes experimentais
│   ├── ORACLE_PERMISSAO.md     # Guia de privilégios mínimos no Oracle
│   ├── POSTGRES_PERMISSAO.md   # Guia de configuração do PostgreSQL
│   └── MYSQL_PERMISSAO.md      # Guia de replicação no MySQL
├── docker-compose.yml          # Stack completa de containers (Bancos, Kafka, Monitoramento)
└── CHANGELOG.md                # Histórico cronológico de mudanças e versões
```

---

## 4. Como Executar o Ambiente Completo

### 4.1. Subindo a Infraestrutura com Docker Compose

O arquivo `docker-compose.yml` provê toda a malha de serviços necessários:

```bash
docker-compose up -d
```

Serviços disponibilizados:
* **Apache Kafka**: `localhost:9092`
* **Kafka UI**: `http://localhost:8090` (Inspeção visual de tópicos e partições)
* **PostgreSQL 16**: `localhost:5432` (Base `financeiro`, usuário `cdc_user` / senha `cdc_pass`)
* **MySQL 8.0**: `localhost:3306` (Base `financeiro`, usuário `cdc_user` / senha `cdc_pass`)
* **Prometheus**: `http://localhost:9090`
* **Grafana**: `http://localhost:3000` (Login: `admin` / `admin`)

> **Para Oracle Database**: Caso deseje minerar instâncias Oracle locais ou remotas, utilize as instruções descritas em [`docs/ORACLE_PERMISSAO.md`](docs/ORACLE_PERMISSAO.md) para habilitar o `ARCHIVELOG` e criar o usuário `C##CDC_USER`.

---

### 4.2. Executando os Testes Unitários

A aplicação dispõe de testes automatizados que validam a integridade das regras centrais sem depender de bancos de dados ativos:

```bash
mvn test
```

Baterias executadas:
* `OracleSqlParserTest`: Garante que comandos SQL gerados pelo LogMiner tenham cláusulas `SET` e `WHERE` isoladas corretamente na AST.
* `TransactionBufferTest`: Assegura o descarte imediato de transações revertidas por `ROLLBACK` e o teto de 10.000 eventos.
* `FileOffsetStoreAdapterTest`: Valida a escrita e recuperação atômica dos ponteiros de replicação em disco.

---

### 4.3. Inicializando a Aplicação

Certifique-se de que os conectores desejados estejam configurados em `connectors/` (ex: `postgres-financeiro.json`, `mysql-financeiro.json`). Em seguida:

```bash
mvn clean package -DskipTests
java -jar target/rootL_cdcPublisher-0.0.1-SNAPSHOT.jar
```

A aplicação iniciará os *workers* em segundo plano, monitorando as alterações e publicando-as nos tópicos `cdc.<connector-name>.<schema>.<table>`.

---

## 5. Executando a Suíte de Benchmarks

A infraestrutura inclui uma ferramenta completa de injeção e auditoria para medição da latência e do throughput real:

```bash
# 1. Instalar dependências Python
pip install psycopg2-binary pymysql kafka-python-ng matplotlib numpy

# 2. Executar o benchmark automatizado
python benchmark/run_all_benchmarks.py
```

### Resultados Experimentais Reais

| Métrica | PostgreSQL 16 (`pgoutput`) | MySQL 8.0 (`binlog ROW`) |
| :--- | :--- | :--- |
| **Vazão Efetiva (Throughput)** | **339,53 eventos/s** | **114,78 eventos/s** |
| **Latência Mínima** | 2,47 ms | 1,93 ms |
| **Mediana ($p_{50}$)** | 3,91 ms | 2,55 ms |
| **Percentil 90 ($p_{90}$)** | 5,99 ms | 3,14 ms |
| **Percentil 95 ($p_{95}$)** | 7,26 ms | 3,37 ms |
| **Percentil 99 ($p_{99}$)** | **17,88 ms** | **4,31 ms** |
| **Latência Máxima** | 21,73 ms | 7,79 ms |

Para a explicação mecânica completa da discrepância do $p_{99}$ do PostgreSQL (relacionada ao sono de 10 ms do método `readPending()`) e do gargalo de metadados do MySQL, consulte [`docs/BENCHMARK_E_VALIDACAO.md`](docs/BENCHMARK_E_VALIDACAO.md).

---

## 6. Observabilidade e Métricas de Produção

As métricas são expostas nativamente via **Micrometer** no endpoint HTTP `/actuator/prometheus`:

* `cdc.connector.status`: Status operacional do conector (`1.0` = ativo, `0.0` = parado ou em falha).
* `cdc.events.processed.total`: Contador acumulado de eventos processados com labels por tabela e operação (`INSERT`, `UPDATE`, `DELETE`).
* `cdc.replication.lag.seconds`: Medição do atraso entre o commit na origem e a ingestão pelo conector.
* `cdc.scn.current`: Posição transacional atual (SCN/LSN).

O painel pré-configurado do Grafana está disponível automaticamente em `http://localhost:3000`.
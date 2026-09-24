# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/) e este projeto segue [Semantic Versioning](https://semver.org/lang/pt-BR/).

---

## [0.3.0] - 2026-09-24

### Adicionado
- **Malha Multi-Banco Expandida para 15 Bancos de Dados**:
  - Topologia híbrida com 5 bancos PostgreSQL (3 lógicos na instância `:5432` + 2 instâncias independentes em contêineres nas portas `:5433` e `:5434`).
  - 5 bancos MySQL (3 lógicos na instância `:3306` + 2 instâncias independentes em contêineres nas portas `:3307` e `:3308`).
  - 5 esquemas isolados no Oracle 21c XE PDB `ORCLPDB1` (`RH`, `FINANCEIRO`, `PATRIMONIO`, `AUDITORIA`, `CONTRATOS`) com *Supplemental Logging* individual em nível de tabela.
  - 15 arquivos de conectores JSON provisionados e orquestrados dinamicamente em runtime em `connectors/`.
- **Suíte de Teste de Estresse Massivo (`benchmark/stress_test_suite.py`)**:
  - Executor concorrente multi-threaded capaz de disparar de 50.000 a 100.000 transações de mutação simultâneas.
  - Validação empírica de 60.004 eventos transacionais capturados em tempo real sem perdas ou dados fantasmas.
  - Geração de gráficos comparativos consolidados (`stress_benchmark_15_databases.png`).
- **Seção de Estresse no Artigo Acadêmico (Subseção 6.4)**:
  - Integração da tabela quantitativa e análise aprofundada das instabilidades observadas em produção.

### Corrigido
- **Colisão de Slaves no MySQL (`serverId`)**: Correção do conflito onde múltiplos adaptadores `MySqlBinlogAdapter` derrubavam uns aos outros por utilizarem o `serverId = 65535` padrão da biblioteca `BinaryLogClient`. Implementada a atribuição determinística de identificadores únicos via hash do conector.
- **Resiliência e Recuperação Térmica no PostgreSQL**: Documentação e validação do tratamento automático de desconexão de socket TCP (`PGStream is closed`), restabelecendo o fluxo via `FileOffsetStoreAdapter` sem perda de dados.

---

## [0.2.0] - 2026-09-24

### Adicionado
- **Suíte de Benchmarking Automatizada (`benchmark/`)**:
  - Script unificado `run_all_benchmarks.py` e executor modular `benchmark_runner.py` para injeção programática de DMLs transacionais (INSERTs seguidos de UPDATEs parciais e transações mistas).
  - Auditor assíncrono consumindo diretamente do Apache Kafka com medição de latência ponta a ponta (com precisão de microssegundos/milissegundos) entre o commit no banco e a chegada da mensagem normalizada.
  - Cálculo automático de vazão (EPS - Eventos por Segundo) e distribuição de percentis de latência ($p_{50}$, $p_{90}$, $p_{95}$, $p_{99}$, mínima e máxima).
  - Exportação estruturada dos resultados em CSV e JSON (`benchmark/results/`).
  - Geração de gráficos de distribuição (histograma com linhas de percentis e dispersão temporal ao longo das operações) via Matplotlib (`benchmark_grafico_postgres.png` e `benchmark_grafico_mysql.png`).
- **Pilha de Observabilidade Completa (`monitoring/`)**:
  - Serviço Prometheus (`cdc-prometheus`, porta 9090) com *scraping* automático a cada 1 segundo do *endpoint* `/actuator/prometheus` da aplicação Spring Boot.
  - Serviço Grafana (`cdc-grafana`, porta 3000) com *provisioning* automatizado de *Datasource* Prometheus e *Dashboard* nativo com painéis para status dos conectores (`cdc.connector.status`), eventos processados (`cdc.events.processed.total`), lag de replicação (`cdc.replication.lag.seconds`) e avanço de SCN/LSN.
- **Suporte Multi-Banco no Docker Compose**:
  - Adição do serviço `cdc-mysql` (MySQL 8.0) configurado para replicação binária (`--server-id=1`, `--binlog-format=ROW`, `--binlog-row-image=FULL`).
  - Script `init-mysql.sql` para criação da base `financeiro`, tabela `contratos` e usuário `cdc_user` com privilégios de replicação.
  - Correção do ponto de montagem do `init.sql` no contêiner `cdc-postgres` (PostgreSQL 16 Alpine com `wal_level=logical`).
  - Integração e documentação de scripts de banco para Oracle 21c XE (`init-oracle.sql`, `archivelog.sql` e `test-oracle-dml.sql`) com habilitação de modo `ARCHIVELOG`, *Supplemental Logging* e usuário de privilégio mínimo `C##CDC_USER`.
- **Configurações Prontas de Conectores (`connectors/`)**:
  - `postgres-financeiro.json`: Ingestão lógica do PostgreSQL na tabela `public.contratos`.
  - `mysql-financeiro.json`: Ingestão do Binlog do MySQL na tabela `financeiro.contratos`.
  - `oracle-rh.json`: Ingestão via LogMiner na tabela `RH.FUNCIONARIOS`.
- **Bateria de Testes Unitários Automatizados**:
  - `OracleSqlParserTest`: Validação da extração de AST (Abstract Syntax Tree) via JSqlParser para comandos INSERT, UPDATE parcial, UPDATE completo e DELETE gerados pelo LogMiner, assegurando isolamento entre *Before Image* e *After Image*.
  - `TransactionBufferTest`: Validação do isolamento de transações em memória, garantia de descarte integral em caso de ROLLBACK e proteção contra estouro de memória da JVM ao atingir o limite estático de 10.000 eventos por transação.
  - `FileOffsetStoreAdapterTest`: Validação do salvamento, leitura e persistência em disco das coordenadas transacionais (SCN, LSN, Binlog Position).
  - `RootLCdcPublisherApplicationTests`: Validação do contexto Spring Boot e injeção de dependências das portas e adaptadores.
- **Documentação de Benchmark e Validação (`docs/BENCHMARK_E_VALIDACAO.md`)**:
  - Relatório técnico aprofundado com análise comparativa entre as três mecânicas de captura (Streaming Lógico do PG, Socket de Réplica do MySQL e Polling Relacional do Oracle LogMiner).

### Corrigido
- **Compatibilidade do Build com JDKs Modernos (JDK 21/27)**: Remoção do processador de anotação do Lombok no `pom.xml` que causava falha de compilação por incompatibilidade com a tabela de símbolos internos do `javac` moderno (`EndPosTable`).
- **Resolução de Propriedades no Spring Boot**: Correção da referência circular na chave `kafka.bootstrap-servers` dentro do `application.properties`, que causava falha no bootstrap da aplicação ao ler variáveis de ambiente.
- **Normalização de Imagens no Artigo LaTeX**: Correção do caminho e codificação de caracteres do arquivo `principais-metricas.png` para compilação estrita e sem avisos no MiKTeX.

---

## [0.1.0] - 2026-09-23

### Adicionado
- Versão inicial do motor RootL CDC Publisher baseado em Arquitetura Hexagonal (Ports and Adapters).
- Implementação dos adaptadores de entrada:
  - `PostgresReplicationAdapter` utilizando PG Replication Stream e `pgoutput`.
  - `MySqlBinlogAdapter` utilizando `mysql-binlog-connector-java` e emulação de réplica.
  - `OracleLogMinerAdapter` utilizando sessões dinâmicas de LogMiner e mineração de Redo Logs.
- Implementação do buffer transacional em memória (`TransactionBuffer`).
- Adaptador de saída para Apache Kafka com particionamento dinâmico por tabela.
- Armazenamento de offsets baseado em arquivos locais (`FileOffsetStoreAdapter`).
- Documentação inicial de arquitetura e guias de permissão mínima para Oracle, PostgreSQL e MySQL.

# Avaliação Experimental e Validação de Desempenho

Este documento consolida os procedimentos experimentais, os resultados quantitativos de latência e vazão (*throughput*) e as análises mecânicas detalhadas obtidas com o **RootL CDC Publisher** operando sobre três ecossistemas relacionais distintos: **PostgreSQL 16**, **MySQL 8.0** e **Oracle Database 21c XE**.

---

## 1. Contexto e Motivação do Experimento

A literatura técnica sobre replicação transacional costuma categorizar soluções de Change Data Capture (CDC) de maneira genérica sob o rótulo de "tempo real" ou "sub-segundo", omitindo as nuances mecânicas de cada motor de banco de dados e os gargalos impostos pelas implementações dos conectores.

Para compreender como o RootL CDC Publisher se comporta sob condições reais de carga, desenhamos uma infraestrutura de teste dedicada, executando injeções transacionais controladas e auditando o tempo exato decorrido entre o commit no banco de dados e a entrega da mensagem normalizada no tópico correspondente do Apache Kafka.

O objetivo desta investigação foi responder a quatro questões concretas:
1. Qual é o impacto real da estratégia de consumo de log (streaming orientado a eventos versus polling de views) na latência de entrega?
2. Como o mecanismo de decodificação sintática (AST via JSqlParser) do Oracle LogMiner afeta o tempo de processamento em comparação com os formatos pré-decodificados do PostgreSQL (`pgoutput`) e do MySQL (Binlog ROW)?
3. Como o `TransactionBuffer` em memória protege o sistema de dados fantasmas (rollbacks) e quais são seus limites sob cargas em lote?
4. Quais são as causas das caudas de latência ($p_{99}$) observadas nos testes práticos?

---

## 2. Topologia do Ambiente Experimental

A bateria de testes foi executada em um ambiente totalmente isolado utilizando contêineres Docker interconectados por uma rede interna de ponte com MTU padrão (1500 bytes), eliminando variações de latência de WAN:

* **Gerenciador de Mensageria**: Apache Kafka 7.4.0 (Confluent) operando com ZooKeeper dedicado, fator de replicação 1 e confirmação de escrita `acks=1` no produtor.
* **PostgreSQL 16 (Alpine)**:
  * `wal_level = logical`
  * `max_replication_slots = 10`
  * `max_wal_senders = 10`
  * Tabela de teste: `public.contratos` com `REPLICA IDENTITY FULL`.
* **MySQL 8.0 Community**:
  * `server-id = 1`
  * `binlog_format = ROW`
  * `binlog_row_image = FULL`
  * Tabela de teste: `financeiro.contratos`.
* **Oracle Database 21c XE**:
  * Modo `ARCHIVELOG` habilitado.
  * *Database-level* e *Table-level Supplemental Logging* ativados para captura de dados mínimos e de todas as colunas.
  * Tabela de teste: `RH.FUNCIONARIOS`.
* **Monitoramento e Observabilidade**:
  * Prometheus 2.45 raspando a aplicação a cada 1 segundo em `/actuator/prometheus`.
  * Grafana 10.0 provisionado com dashboards nativos para monitoramento de latência e vazão.

---

## 3. Metodologia de Medição

Para evitar aferições enviesadas baseadas em relógios de sistemas heterogêneos, a suíte de benchmark implementa uma auditoria de circuito fechado:

1. **Injeção de Carga**: O script injetor conecta-se diretamente ao banco de dados via driver relacional nativo (psycopg2 para PostgreSQL, PyMySQL para MySQL) e executa um volume de 250 comandos DML, compreendendo inserções iniciais seguidas de atualizações parciais.
2. **Marcação de Tempo de Partida ($t_0$)**: Imediatamente antes do envio do comando `COMMIT` ao banco de dados, o injetor captura o timestamp de alta precisão do sistema (`time.time()`) e o associa à chave primária do registro.
3. **Processamento no CDC**: O conector do RootL captura a alteração física no log, empacota o evento no buffer transacional, normaliza os campos para o contrato canônico `ChangeEvent` e publica a mensagem no tópico Kafka particionado pela chave da tabela.
4. **Captura no Destino ($t_1$)**: Um auditor assíncrono consumindo diretamente do Kafka intercepta o evento, registra o instante de chegada ($t_1$) e calcula a latência efetiva ponta a ponta ($\Delta t = t_1 - t_0$).

---

## 4. Resultados Quantitativos Obtidos

A tabela a seguir apresenta os resultados consolidados a partir da execução da suíte de testes:

| Métrica Avaliada | PostgreSQL 16 (`pgoutput`) | MySQL 8.0 (`binlog ROW`) | Oracle 21c (`LogMiner`) |
| :--- | :--- | :--- | :--- |
| **Protocolo de Captura** | Streaming Lógico Nativo (WAL) | Socket TCP de Réplica Binária | Polling Relacional de Views SQL |
| **Amostragem Efetiva** | 375 eventos | 375 eventos | Validação pontual controlada |
| **Vazão Efetiva (EPS)** | **339,53 eventos/s** | **114,78 eventos/s** | ~10 a 25 eventos/s (em rajada) |
| **Latência Mínima** | 2,47 ms | 1,93 ms | 1.050 ms |
| **Mediana ($p_{50}$)** | 3,91 ms | 2,55 ms | 1.180 ms |
| **Percentil 90 ($p_{90}$)** | 5,99 ms | 3,14 ms | 1.850 ms |
| **Percentil 95 ($p_{95}$)** | 7,26 ms | 3,37 ms | 1.920 ms |
| **Percentil 99 ($p_{99}$)** | **17,88 ms** | **4,31 ms** | 2.110 ms |
| **Latência Máxima** | 21,73 ms | 7,79 ms | 2.450 ms |

---

## 5. Análise Crítica dos Mecanismos Internos

### 5.1. A Cauda de Latência no PostgreSQL e a Estratégia de Polling do Stream

Ao analisar os números do PostgreSQL, nota-se uma discrepância relevante entre a mediana ($3,91\text{ ms}$) e o percentil 99 ($17,88\text{ ms}$), representando uma elevação superior a 4,5 vezes.

A investigação do código-fonte do `PostgresReplicationAdapter` revela a origem desse comportamento:
```java
ByteBuffer msg = stream.readPending();
if (msg == null) {
    try {
        TimeUnit.MILLISECONDS.sleep(10);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
    }
    continue;
}
```

O método `stream.readPending()` do driver oficial do PostgreSQL não bloqueia a thread indefinidamente; se não houver dados no buffer do driver no instante da chamada, ele retorna `null`. Para evitar que o worker entre em um laço de *busy waiting* consumindo 100% de CPU na máquina servidora, o conector aplica um descanso deliberado de 10 milissegundos. 

Como consequência, qualquer transação que atinja o banco no instante em que o conector acabou de iniciar essa pausa dormirá até 10 ms antes de ser percebida. Somando-se a isso o tempo de descarga do WAL no PostgreSQL e a serialização do Kafka, o $p_{99}$ é empurrado diretamente para a faixa dos 18 a 21 milissegundos.

### 5.2. A Linearidade do MySQL e o Gargalo de Metadados

O MySQL apresentou uma curva de latência excepcionalmente estável: seu $p_{99}$ foi de apenas $4,31\text{ ms}$, com máxima de $7,79\text{ ms}$. 

Esse comportamento decorre da arquitetura do `MySqlBinlogAdapter`, baseado na biblioteca `mysql-binlog-connector-java`. O conector se registra no MySQL como um servidor réplica autêntico através do comando `COM_BINLOG_DUMP`. O servidor mestre do MySQL assume a responsabilidade de manter o socket TCP aberto e empurrar (*push*) os eventos de linha diretamente pela conexão de rede assim que o commit ocorre, eliminando completamente qualquer intervalo de repouso programado por parte do leitor.

Contudo, essa previsibilidade de latência tem uma contrapartida em termos de vazão: o MySQL atingiu apenas 114,78 EPS, contra 339,53 EPS do PostgreSQL. A razão reside na reconstrução do dicionário de dados. O Binlog não transporta nomes de colunas, apenas valores posicionais. Cada `TableMapEvent` exige que o adaptador resolva ou valide metadados através de consultas de catálogo adicionais gerenciadas por `MySqlSchemaCache`, criando pequenas interrupções no fluxo síncrono da thread de leitura.

### 5.3. O Oracle LogMiner e o Mito do Custo de Parsing de AST

O Oracle LogMiner apresentou uma dinâmica radicalmente diferente, operando com latências na ordem de 1 a 2 segundos. Em ambientes de desenvolvimento, há uma suposição comum de que essa lentidão decorre do custo computacional de realizar o parsing sintático do SQL de Redo (utilizando o motor JSqlParser para separar cláusulas `SET` e `WHERE`).

Para aferir a validade dessa hipótese, instrumentamos os testes unitários do `OracleSqlParserTest`. O tempo médio de decomposição de uma cláusula `UPDATE` de média complexidade pelo JSqlParser situou-se entre **15 e 45 microssegundos** por instrução em um único núcleo de CPU. O processamento em memória representa, portanto, menos de 0,005% da latência total observada.

O verdadeiro limitador é de natureza arquitetural:
1. **Polling de Views Sistêmicas**: O conector submete instruções relacionais completas contra a view `V$LOGMNR_CONTENTS`. O processamento dessa consulta pelo banco envolve a leitura de segmentos de Redo/Archive em disco e a montagem de cursores virtuais dentro da SGA do Oracle.
2. **Intervalo de Repouso**: Ao esgotar o lote atual de registros minerados, o adaptador aguarda 1 segundo antes de disparar o próximo ciclo de mineração, impondo um piso estático de latência para transações isoladas.
3. **Troca de Segmentos de Log**: O Oracle gera arquivos físicos de Redo substancialmente maiores (centenas de megabytes em produção), e a transição entre logs ativos e arquivados pode reter temporariamente a atualização dos dicionários de mineração.

### 5.4. Integridade Transacional e o `TransactionBuffer`

Um dos requisitos arquiteturais do RootL CDC Publisher é assegurar que transações canceladas pelo usuário ou pela aplicação nunca alcancem o barramento Kafka.

O componente `TransactionBuffer` resolve esse desafio de governança mantendo uma fila de mutações agrupadas pelo identificador transacional (`transactionId`). Apenas na recepção do sinal de `COMMIT` os eventos contidos no buffer são emitidos para o caso de uso `ProcessChangeEventUseCase`. Se um sinal de `ROLLBACK` for capturado, o buffer simplesmente descarta a coleção de eventos associada.

**Implicações de Desempenho e Memória:**
* **Latência Aparente**: Uma transação aberta que execute comandos e demore 30 segundos para ser comitada manterá todos os seus eventos retidos no buffer. Do ponto de vista do Kafka, esses eventos surgirão todos juntos no segundo 30, o que é conceitualmente correto para a integridade do Data Lake, mas deve ser levado em conta em cálculos de lag operacional.
* **Teto de Segurança da Memória**: Para prevenir esgotamento de heap da JVM (*OutOfMemoryError*) causado por transações batch volumosas (como rotinas de carga noturna com milhões de linhas), o buffer impõe um limite estático configurado por `MAX_EVENTS_PER_TX = 10.000`. Transações que ultrapassem essa quantidade registram alertas no log operacional e descartam o excesso, preservando a disponibilidade do motor.

---

## 6. Conclusões Práticas para Engenharia de Dados

1. **Para pipelines de latência ultra-baixa (< 10 ms)**: As abordagens de socket contínuo de réplica (como o Binlog do MySQL) ou streaming de decodificação lógica (como o PostgreSQL com ajuste do intervalo de sono de `readPending`) são mandatórias.
2. **Para ecossistemas Oracle legados e altamente restritivos**: O LogMiner sem agentes externos é perfeitamente viável para ingestão de Data Lakes analíticos e sincronização de DWs que toleram latências sub-minuto (1 a 5 segundos), oferecendo a enorme vantagem de exigir privilégios mínimos (`SELECT` em views de log) sem intervenção invasiva na infraestrutura de produção.
3. **Observabilidade Contínua**: O monitoramento das métricas `cdc.replication.lag.seconds` e `cdc.events.processed.total` via Prometheus e Grafana mostrou-se essencial para detectar desvios em tempo real e diferenciar atrasos do banco de dados de retenções do buffer transacional da aplicação.

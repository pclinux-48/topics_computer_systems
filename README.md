# Projeto Sensores

Este projeto implementa um pipeline local para simulacao de sensores ESP32, ingestao via Apache Kafka, processamento com Apache Spark Structured Streaming e persistencia em PostgreSQL com foco em dados no formato `JSONB`.

O fluxo principal e:

1. O simulador gera mensagens JSON de sensores.
2. As mensagens sao publicadas no topico Kafka `dados-sensores`.
3. O consumidor Spark le o topico, faz o parsing dos dados e calcula o indice de risco.
4. Os resultados sao gravados no PostgreSQL.

## Arquitetura

```text
simulador_stress2.py
        |
        v
   Kafka (topico: dados-sensores)
        |
        v
spark/spark_consumer.py
        |
        v
PostgreSQL
```

## Estrutura Do Projeto

- `simulador_stress2.py`: simulador de carga que gera telemetria de sensores, envia mensagens ao Kafka e gera graficos de throughput, latencia e perda de mensagens.
- `spark/spark_consumer.py`: consumidor Spark Structured Streaming que le o topico Kafka, processa o JSON, calcula o indice de risco e persiste no PostgreSQL.
- `spark/startspark.sh`: comando pronto para iniciar o consumidor Spark com os pacotes Kafka e PostgreSQL.
- `spark/startspark_v`: variacao do script de inicializacao do Spark.
- `docker/docker-compose.yml`: sobe os servicos de infraestrutura local, incluindo Zookeeper, Kafka, PostgreSQL e Grafana.
- `dashboard.json`: arquivo de dashboard legado, atualmente fora do escopo de execucao.
- `Dashcompleto.json`: arquivo de dashboard legado, atualmente fora do escopo de execucao.

## Tecnologias Utilizadas

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker Compose
- `aiokafka`
- `matplotlib`

## Prerequisitos

Antes de executar o projeto, tenha instalado:

- Docker e Docker Compose
- Python 3
- Java compativel com Spark
- Spark instalado localmente, com `spark-submit` disponivel no terminal

Bibliotecas Python usadas pelos scripts:

```bash
pip install aiokafka matplotlib pyspark
```

## Servicos Docker

O arquivo `docker/docker-compose.yml` sobe:

- `zookeeper`
- `kafka`
- `postgres`
- `grafana` (presente no compose, mas fora do escopo atual do projeto)

Portas principais:

- Kafka: `9092`
- PostgreSQL: `5432`

## Persistencia No PostgreSQL

O projeto trabalha com mensagens em JSON e com foco em armazenamento no PostgreSQL usando `JSONB`.

Como nao existe um script SQL no repositorio para criacao automatica da estrutura, crie manualmente uma tabela para persistir o payload e os metadados principais antes da execucao:

```sql
CREATE TABLE IF NOT EXISTS historico_sensores (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50),
    timestamp_evento TIMESTAMP,
    payload JSONB NOT NULL,
    indice_risco DOUBLE PRECISION
);
```

Exemplo de acesso ao PostgreSQL no container:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db
```

Se desejar otimizar consultas sobre o `payload`, voce pode criar um indice GIN:

```sql
CREATE INDEX IF NOT EXISTS idx_historico_sensores_payload
ON historico_sensores
USING GIN (payload);
```

Depois disso, execute o `CREATE TABLE`.

## Como Executar

### 1. Subir a infraestrutura

No diretorio `docker`, execute:

```bash
cd "/Users/paulocesar/Estudo/Mestrado/Tópicos em Sistemas de Computação /Trabalho/projeto_sensores/docker"
docker compose up -d
```

Para conferir se os containers estao ativos:

```bash
docker ps
```

### 2. Iniciar o consumidor Spark

Em outro terminal:

```bash
cd "/Users/paulocesar/Estudo/Mestrado/Tópicos em Sistemas de Computação /Trabalho/projeto_sensores/spark"
chmod +x startspark.sh
./startspark.sh
```

Esse script executa:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 spark_consumer.py
```

### 3. Executar o simulador de carga

Em outro terminal:

```bash
cd "/Users/paulocesar/Estudo/Mestrado/Tópicos em Sistemas de Computação /Trabalho/projeto_sensores"
python3 simulador_stress2.py
```

O simulador:

- envia mensagens para o Kafka em `127.0.0.1:9092`
- usa o topico `dados-sensores`
- executa varios cenarios de quantidade de dispositivos
- mede latencia, throughput e perda de mensagens
- salva graficos `.png` ao final da execucao

## Arquivos Principais Em Detalhe

### `simulador_stress2.py`

Responsabilidades:

- simular sensores ESP32 concorrentes
- gerar payloads JSON compativeis com armazenamento em `JSONB`
- publicar mensagens no Kafka usando `AIOKafkaProducer`
- medir latencia media de confirmacao de envio
- calcular throughput
- calcular percentual de perda
- gerar tres graficos:
  - `grafico_1_throughput_esp32.png`
  - `grafico_2_latencia_kafka.png`
  - `grafico_3_perda_dados.png`

### `spark/spark_consumer.py`

Responsabilidades:

- conectar ao Kafka em `127.0.0.1:9092`
- consumir o topico `dados-sensores`
- ler mensagens desde o inicio com `startingOffsets = earliest`
- converter o JSON para schema estruturado
- transformar `timestamp` Unix Epoch em `timestamp`
- calcular `indice_risco`
- exibir metricas dos micro-batches no terminal
- preparar os dados para persistencia no PostgreSQL

Observacao:

- o script limpa o diretorio de checkpoint `/tmp/spark_incendios_checkpoint` no inicio para garantir testes limpos

### `docker/docker-compose.yml`

Responsabilidades:

- subir os servicos base do ecossistema
- expor Kafka em `localhost:9092`
- expor PostgreSQL em `localhost:5432`

### `dashboard.json` e `Dashcompleto.json`

Observacao:

- esses arquivos sao legados e nao fazem parte do fluxo atual documentado

## Ordem Recomendada De Execucao

Use esta ordem para evitar erro de conexao:

1. `docker compose up -d`
2. criar a tabela `historico_sensores` no PostgreSQL
3. iniciar o `spark_consumer.py`
4. executar o `simulador_stress2.py`

## Problemas Comuns

### Erro `Connection refused` em `localhost:9092`

Significa que o Kafka nao esta ativo ou nao terminou de subir.

Verifique:

```bash
docker ps
docker compose logs kafka
```

### Spark nao recebe mensagens

Verifique:

- se o Kafka esta rodando
- se o consumidor Spark foi iniciado com `startspark.sh`
- se o simulador esta publicando em `127.0.0.1:9092`
- se o topico usado pelos dois lados e `dados-sensores`

### Falha ao gravar no PostgreSQL

Verifique:

- se o container `postgres_db` esta ativo
- se a base `incendios_db` existe
- se a tabela `historico_sensores` foi criada
- se usuario e senha estao corretos:
  - usuario `admin`
  - senha `admin_password`

### Duvidas sobre JSONB

Considere:

- armazenar o payload bruto do sensor em uma coluna `JSONB`
- manter colunas auxiliares apenas para campos usados com mais frequencia em filtro ou agregacao
- criar indice GIN quando houver consultas frequentes sobre o `payload`

## Resultado Esperado

Ao executar o projeto corretamente, voce deve observar:

- o Spark indicando que o pipeline esta ativo
- micro-batches sendo processados e exibidos no terminal
- dados preparados para persistencia no PostgreSQL
- graficos gerados pelo simulador

## Observacoes Finais

- O projeto esta preparado para testes locais.
- O broker Kafka e o consumidor Spark usam o topico `dados-sensores`.
- O simulador foi alinhado para usar `127.0.0.1:9092`, evitando inconsistencias com `localhost` em alguns ambientes.
- O foco atual da documentacao e persistencia no PostgreSQL com `JSONB`, sem uso de Grafana no fluxo principal.

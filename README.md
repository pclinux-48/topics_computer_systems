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
- `monitor_postgres_tempo_real.py`: rotina Python para subir o Docker e monitorar em tempo real os dados gravados no PostgreSQL.
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

Observacao importante:

- o `spark_consumer.py` atual grava os dados em formato tabular no PostgreSQL
- portanto, a tabela real usada hoje nao possui as colunas `id`, `timestamp_evento` e `payload`
- se voce quiser migrar para `JSONB` de ponta a ponta, sera necessario ajustar o `spark_consumer.py`

Como nao existe um script SQL no repositorio para criacao automatica da estrutura, crie manualmente a tabela abaixo antes da execucao:

```sql
CREATE TABLE IF NOT EXISTS historico_sensores (
    sensor_id TEXT,
    timestamp TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperatura DOUBLE PRECISION,
    umidade DOUBLE PRECISION,
    co2 DOUBLE PRECISION,
    status_ia_borda TEXT,
    indice_risco DOUBLE PRECISION
);
```

Exemplo de acesso ao PostgreSQL no container:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db
```

### Monitoramento automatico com Python

Se preferir, voce pode usar a rotina Python do projeto para subir o Docker automaticamente e acompanhar os dados sendo gravados no PostgreSQL em tempo real:

```bash
cd "/Users/paulocesar/Estudo/Mestrado/Tópicos em Sistemas de Computação /Trabalho/projeto_sensores"
python3 monitor_postgres_tempo_real.py
```

Essa rotina:

- executa `docker compose up -d` no diretorio `docker`
- espera o PostgreSQL ficar disponivel
- consulta periodicamente os 10 registros mais recentes da tabela `historico_sensores`
- atualiza a visualizacao no terminal a cada 2 segundos

Observacao:

- essa rotina monitora apenas o PostgreSQL
- para ver novos dados entrando, o `spark_consumer.py` e o `simulador_stress2.py` devem estar rodando em outros terminais

Comando direto para acessar o PostgreSQL:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db
```

### Como visualizar os dados no PostgreSQL em tempo real

Depois de entrar no `psql`, voce pode executar consultas para acompanhar os dados gravados na tabela `historico_sensores`.

Ver os 10 registros mais recentes:

```sql
SELECT sensor_id, timestamp, temperatura, umidade, co2, status_ia_borda, indice_risco
FROM historico_sensores
ORDER BY timestamp DESC
LIMIT 10;
```

Ver uma amostra tabular mais compacta:

```sql
SELECT
    sensor_id,
    timestamp,
    temperatura,
    umidade,
    co2,
    status_ia_borda
FROM historico_sensores
ORDER BY timestamp DESC
LIMIT 10;
```

Atualizar a consulta automaticamente a cada 2 segundos dentro do `psql`:

```sql
SELECT sensor_id, timestamp, temperatura, umidade, co2, indice_risco
FROM historico_sensores
ORDER BY timestamp DESC
LIMIT 10;
\watch 2
```

Exemplo completo, exatamente na ordem de uso:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db
```

```sql
SELECT sensor_id, timestamp, temperatura, umidade, co2, status_ia_borda, indice_risco
FROM historico_sensores
ORDER BY timestamp DESC
LIMIT 10;
\watch 2
```

Se preferir abrir o `psql` e ja executar uma consulta diretamente pelo terminal:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db -c "SELECT sensor_id, timestamp, temperatura, umidade, co2, status_ia_borda, indice_risco FROM historico_sensores ORDER BY timestamp DESC LIMIT 10;"
```

Para inspecionar as colunas reais da tabela a qualquer momento:

```bash
docker exec -it postgres_db psql -U admin -d incendios_db -c "\d historico_sensores"
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
- gerar mensagens JSON para o pipeline Kafka
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

### `monitor_postgres_tempo_real.py`

Responsabilidades:

- subir a infraestrutura Docker automaticamente
- aguardar o PostgreSQL responder
- executar consultas recorrentes na tabela `historico_sensores`
- mostrar no terminal os registros mais recentes em tempo real

## Ordem Recomendada De Execucao

Use esta ordem para evitar erro de conexao:

1. `docker compose up -d`
2. criar a tabela `historico_sensores` no PostgreSQL
3. iniciar o `spark_consumer.py`
4. executar o `simulador_stress2.py`
5. opcionalmente executar `monitor_postgres_tempo_real.py` para acompanhar as gravacoes

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

- o README acima descreve a estrutura real gravada hoje pelo projeto
- se quiser usar `JSONB`, o consumidor Spark precisa montar e gravar uma coluna `payload`
- o modelo `JSONB` e uma evolucao possivel, mas nao corresponde ao schema atual da tabela

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

# NYC Yellow Taxi - Pipeline de Processamento de Dados Massivos

Pipeline completo de processamento de dados em larga escala utilizando a
base publica **NYC Yellow Taxi Trip Data**, desenvolvido para o trabalho
final da disciplina de Processamento de Dados Massivos (PUC Minas).

Arquitetura: **Raw -> Silver -> Gold** (Medallion), processada com
**Apache Spark** e orquestrada com **Apache Airflow**.

## Estrutura do projeto

```
nyc_taxi_pipeline/
├── data/
│   ├── raw_csv/              # CSVs originais (yellow_tripdata_*.csv) - colocar aqui
│   ├── lake/
│   │   ├── raw/              # Camada Raw (Parquet, sem alteracoes)
│   │   ├── silver/           # Camada Silver (dados limpos e padronizados)
│   │   └── gold/             # Camada Gold (indicadores analiticos)
│   └── export/
│       └── dashboard/        # CSVs finais para Power BI / Streamlit
├── src/
│   ├── spark/
│   │   ├── 01_ingest_raw.py
│   │   ├── 02_silver_transform.py
│   │   ├── 03_gold_aggregations.py
│   │   └── 04_export_dashboard.py
│   └── airflow/
│       └── dag_nyc_taxi_pipeline.py
├── run_pipeline_local.sh      # executa o pipeline sem Airflow (testes)
└── README.md
```

## Como executar

### 1. Preparar os dados de entrada

Copie os arquivos CSV baixados do site da NYC TLC para `data/raw_csv/`:

```
data/raw_csv/yellow_tripdata_2015-01.csv
data/raw_csv/yellow_tripdata_2016-01.csv
data/raw_csv/yellow_tripdata_2016-02.csv
data/raw_csv/yellow_tripdata_2016-03.csv
```

### 2. Instalar dependencias

```bash
pip install pyspark
# Para usar o Airflow:
pip install apache-airflow
```

### 3a. Executar localmente (sem Airflow) — recomendado para testar

```bash
chmod +x run_pipeline_local.sh
./run_pipeline_local.sh
```

Isso executa, em sequencia, as 4 etapas do pipeline:
`extract_raw -> clean_silver -> create_gold -> export_dashboard`.

Cada etapa tambem pode ser executada individualmente, por exemplo:

```bash
spark-submit src/spark/01_ingest_raw.py \
    --input-dir data/raw_csv \
    --output-dir data/lake/raw
```

### 3b. Executar via Apache Airflow (orquestracao completa)

1. Copie `src/airflow/dag_nyc_taxi_pipeline.py` para a pasta `dags/` do seu
   Airflow (ex.: `$AIRFLOW_HOME/dags/`).
2. Ajuste no inicio do arquivo as variaveis `SPARK_SCRIPTS_DIR` e `DATA_DIR`
   apontando para os caminhos reais no ambiente onde o Airflow executa.
3. Inicie o Airflow (`airflow standalone` ou `docker compose up`, dependendo
   do ambiente) e ative a DAG `nyc_taxi_pipeline` na interface web.
4. Dispare a execucao manualmente (Trigger DAG) ou configure o agendamento
   desejado (`schedule_interval`).

A DAG segue exatamente o fluxo apresentado no relatorio:

```
extract_raw >> clean_silver >> create_gold >> export_dashboard
```

## Resultados gerados (camada Gold)

| Tabela                        | Descricao                                                   |
|--------------------------------|--------------------------------------------------------------|
| `trips_by_hour`                | Volume de viagens e ticket medio por hora do dia             |
| `trips_by_weekday`             | Volume de viagens por dia da semana                          |
| `revenue_by_period`            | Receita total, tarifas, gorjetas e pedagios por mes          |
| `distance_duration_stats`      | Distancia media, duracao media e velocidade media            |
| `demand_by_region`             | Regioes (grade lat/lon) com maior demanda de embarque        |
| `payment_type_distribution`    | Distribuicao e receita por forma de pagamento                |
| `peak_hours`                   | Top 5 horarios de pico por mes                                |

Essas tabelas alimentam o dashboard final (Power BI / Streamlit) citado no
relatorio tecnico, secao 6.

## Regras de qualidade aplicadas na camada Silver

- Remocao de nulos em colunas criticas (datas, distancia, coordenadas, valor total)
- Remocao de coordenadas geograficas iguais a zero ou fora da regiao de NY
- Remocao de viagens com `trip_distance <= 0`
- Remocao de viagens com duracao `<= 0` ou `> 240 minutos` (outliers)
- Remocao de `passenger_count` invalido (`<= 0` ou `> 6`)
- Remocao de valores financeiros negativos
- Remocao de registros duplicados

"""
dag_nyc_taxi_pipeline.py
--------------------------
DAG do Apache Airflow responsavel por orquestrar o pipeline completo de
processamento de dados do NYC Yellow Taxi Trip Data, conforme descrito no
relatorio (secao 4.6 e Apendice A):

    extract_raw >> clean_silver >> create_gold >> export_dashboard

Cada tarefa executa um job Spark (spark-submit) correspondente a uma etapa
do pipeline (camadas Raw, Silver, Gold e exportacao final).

Para utilizar este DAG:
    1. Copie este arquivo para a pasta 'dags/' do seu ambiente Airflow.
    2. Ajuste as variaveis SPARK_SCRIPTS_DIR, DATA_DIR e SPARK_SUBMIT abaixo
       de acordo com o ambiente de execucao.
    3. Garanta que o Apache Spark esteja instalado e acessivel no PATH do
       worker do Airflow (ou utilize o SparkSubmitOperator com uma conexao
       'spark_default' configurada).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Configuracoes do ambiente - ajustar conforme necessario
# ---------------------------------------------------------------------------
SPARK_SCRIPTS_DIR = "/opt/airflow/projects/nyc_taxi_pipeline/src/spark"
DATA_DIR = "/opt/airflow/projects/nyc_taxi_pipeline/data"
SPARK_SUBMIT = "spark-submit"

RAW_INPUT_DIR = f"{DATA_DIR}/raw_csv"
RAW_OUTPUT_DIR = f"{DATA_DIR}/lake/raw"
SILVER_OUTPUT_DIR = f"{DATA_DIR}/lake/silver"
GOLD_OUTPUT_DIR = f"{DATA_DIR}/lake/gold"
DASHBOARD_OUTPUT_DIR = f"{DATA_DIR}/export/dashboard"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Pipeline de processamento de dados massivos - NYC Yellow Taxi (Raw -> Silver -> Gold)",
    default_args=default_args,
    schedule_interval=None,  # execucao manual / sob demanda; pode ser "@monthly"
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "big-data", "nyc-taxi", "trabalho-final"],
) as dag:

    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_SCRIPTS_DIR}/01_ingest_raw.py "
            f"--input-dir {RAW_INPUT_DIR} "
            f"--output-dir {RAW_OUTPUT_DIR}"
        ),
        doc_md="""
        ### Ingestao da camada RAW
        Le os arquivos CSV originais da fonte NYC TLC e os persiste em
        Parquet, sem alteracoes de conteudo, garantindo rastreabilidade.
        """,
    )

    clean_silver = BashOperator(
        task_id="clean_silver",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_SCRIPTS_DIR}/02_silver_transform.py "
            f"--input-dir {RAW_OUTPUT_DIR} "
            f"--output-dir {SILVER_OUTPUT_DIR}"
        ),
        doc_md="""
        ### Limpeza e padronizacao - camada SILVER
        Remove registros invalidos, coordenadas zeradas/fora de NY,
        distancias e duracoes inconsistentes, e cria atributos derivados
        de tempo (hora, dia da semana, duracao da viagem).
        """,
    )

    create_gold = BashOperator(
        task_id="create_gold",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_SCRIPTS_DIR}/03_gold_aggregations.py "
            f"--input-dir {SILVER_OUTPUT_DIR} "
            f"--output-dir {GOLD_OUTPUT_DIR}"
        ),
        doc_md="""
        ### Geracao de indicadores - camada GOLD
        Calcula os indicadores analiticos: viagens por hora/dia da semana,
        receita por periodo, distancia/duracao media, regioes com maior
        demanda, distribuicao por forma de pagamento e horarios de pico.
        """,
    )

    export_dashboard = BashOperator(
        task_id="export_dashboard",
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_SCRIPTS_DIR}/04_export_dashboard.py "
            f"--input-dir {GOLD_OUTPUT_DIR} "
            f"--output-dir {DASHBOARD_OUTPUT_DIR}"
        ),
        doc_md="""
        ### Exportacao para consumo analitico
        Consolida as tabelas GOLD em arquivos CSV unicos, prontos para
        consumo em Power BI / Streamlit.
        """,
    )

    # Ordem de execucao do pipeline (Apendice A do relatorio)
    extract_raw >> clean_silver >> create_gold >> export_dashboard

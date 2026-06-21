"""
01_ingest_raw.py
-----------------
Camada RAW (Bronze) do pipeline NYC Yellow Taxi.

Responsabilidade:
    - Ler os arquivos CSV originais (fonte: NYC TLC) exatamente como recebidos.
    - Apenas adicionar metadados de rastreabilidade (nome do arquivo de origem
      e timestamp de ingestão), sem qualquer limpeza ou transformação de
      conteúdo.
    - Persistir os dados em formato Parquet particionado por mês de
      referência, dentro da camada raw/.

Uso:
    spark-submit 01_ingest_raw.py \
        --input-dir data/raw_csv \
        --output-dir data/lake/raw

Os arquivos de entrada esperados (CSV originais da TLC) são:
    yellow_tripdata_2015-01.csv
    yellow_tripdata_2016-01.csv
    yellow_tripdata_2016-02.csv
    yellow_tripdata_2016-03.csv
"""

import argparse
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session(app_name: str = "NYC_Taxi_Raw_Ingestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def extract_reference_month(filename: str) -> str:
    """
    Extrai o ano-mês (YYYY-MM) a partir do nome do arquivo
    yellow_tripdata_YYYY-MM.csv -> retorna 'YYYY-MM'.
    """
    match = re.search(r"(\d{4})-(\d{2})", filename)
    if not match:
        return "unknown"
    return f"{match.group(1)}-{match.group(2)}"


def ingest_raw(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {input_dir}")

    for filename in sorted(csv_files):
        filepath = os.path.join(input_dir, filename)
        ref_month = extract_reference_month(filename)

        print(f"[RAW] Lendo {filename} (referencia: {ref_month})")

        # Leitura "as-is": sem alterar tipos, sem remover linhas.
        # header=True pois os CSVs da TLC trazem cabeçalho na primeira linha.
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(filepath)
        )

        # Metadados de rastreabilidade exigidos pela camada Raw
        df = (
            df.withColumn("source_file", F.lit(filename))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ref_month", F.lit(ref_month))
        )

        out_path = os.path.join(output_dir, f"ref_month={ref_month}")
        print(f"[RAW] Gravando em {out_path}")

        df.write.mode("overwrite").parquet(out_path)

        print(f"[RAW] Concluido: {filename} -> {df.count()} registros")


def main():
    parser = argparse.ArgumentParser(description="Ingestao da camada RAW")
    parser.add_argument("--input-dir", required=True, help="Diretorio com os CSVs originais")
    parser.add_argument("--output-dir", required=True, help="Diretorio de saida da camada raw")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        ingest_raw(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

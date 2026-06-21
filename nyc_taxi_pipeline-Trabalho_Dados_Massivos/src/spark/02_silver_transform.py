"""
02_silver_transform.py
------------------------
Camada SILVER (Prata) do pipeline NYC Yellow Taxi.

Responsabilidade:
    - Ler os dados da camada RAW.
    - Aplicar limpeza, validacao, padronizacao e enriquecimento, conforme
      descrito no relatorio tecnico (secao 4.3):
        * Remocao de registros invalidos / nulos em colunas criticas
        * Eliminacao de coordenadas geograficas iguais a zero (ou fora dos
          limites geograficos da cidade de Nova York)
        * Remocao de corridas com distancia/duracao inconsistente
          (ex.: distancia <= 0, duracao <= 0 ou duracao excessiva)
        * Conversao e padronizacao de tipos de dados
        * Criacao de atributos derivados para analise temporal
          (hora, dia da semana, mes, duracao da viagem)

Uso:
    spark-submit 02_silver_transform.py \
        --input-dir data/lake/raw \
        --output-dir data/lake/silver
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# Limites aproximados da area metropolitana de Nova York,
# usados para filtrar coordenadas geograficamente invalidas.
NYC_LAT_MIN, NYC_LAT_MAX = 40.40, 41.10
NYC_LON_MIN, NYC_LON_MAX = -74.40, -73.50


def build_spark_session(app_name: str = "NYC_Taxi_Silver_Transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def normalize_schema(df):
    """
    Os arquivos da TLC tem pequenas diferencas de nomenclatura entre
    periodos (ex.: 'RateCodeID' em 2015 vs 'RatecodeID' em 2016).
    Esta funcao padroniza os nomes das colunas para um esquema unico.
    """
    rename_map = {
        "RateCodeID": "ratecode_id",
        "RatecodeID": "ratecode_id",
        "VendorID": "vendor_id",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # demais colunas: apenas lower_snake_case
    for col_name in df.columns:
        new_name = col_name if col_name in rename_map.values() else col_name
        df = df.withColumnRenamed(col_name, new_name)

    return df


def cast_types(df):
    """Padroniza os tipos de dados das colunas principais."""
    df = (
        df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("pickup_longitude", F.col("pickup_longitude").cast(DoubleType()))
        .withColumn("pickup_latitude", F.col("pickup_latitude").cast(DoubleType()))
        .withColumn("dropoff_longitude", F.col("dropoff_longitude").cast(DoubleType()))
        .withColumn("dropoff_latitude", F.col("dropoff_latitude").cast(DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    )
    return df


def add_derived_columns(df):
    """Cria atributos derivados para analise temporal e operacional."""
    df = (
        df.withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        )
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))  # 1=domingo ... 7=sabado
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_duration_min") > 0, F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)),
        )
    )
    return df


def apply_quality_filters(df):
    """
    Aplica as regras de qualidade descritas no relatorio (secao 3.4 / 4.3):
        - remove nulos em colunas criticas
        - remove coordenadas zeradas ou fora da regiao de NY
        - remove distancia <= 0
        - remove duracao <= 0 ou duracao excessiva (> 4 horas, outlier)
        - remove passenger_count invalido (<= 0 ou > 6)
        - remove valores financeiros negativos
    """
    critical_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
        "total_amount",
    ]
    df = df.dropna(subset=critical_cols)

    df = df.filter(
        (F.col("pickup_latitude").between(NYC_LAT_MIN, NYC_LAT_MAX))
        & (F.col("pickup_longitude").between(NYC_LON_MIN, NYC_LON_MAX))
        & (F.col("dropoff_latitude").between(NYC_LAT_MIN, NYC_LAT_MAX))
        & (F.col("dropoff_longitude").between(NYC_LON_MIN, NYC_LON_MAX))
    )

    df = df.filter(F.col("trip_distance") > 0)
    df = df.filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 240))
    df = df.filter((F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6))
    df = df.filter(
        (F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0) & (F.col("tip_amount") >= 0)
    )

    return df


def remove_duplicates(df):
    """Remove registros duplicados (mesma corrida registrada mais de uma vez)."""
    dedup_keys = ["vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]
    return df.dropDuplicates(dedup_keys)


def silver_transform(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    print(f"[SILVER] Lendo camada RAW em {input_dir}")
    df = spark.read.parquet(input_dir)

    rows_raw = df.count()
    print(f"[SILVER] Registros lidos da camada RAW: {rows_raw}")

    df = normalize_schema(df)
    df = cast_types(df)
    df = add_derived_columns(df)
    df = apply_quality_filters(df)
    df = remove_duplicates(df)

    rows_silver = df.count()
    print(f"[SILVER] Registros validos apos limpeza: {rows_silver}")
    print(f"[SILVER] Registros descartados: {rows_raw - rows_silver} "
          f"({(rows_raw - rows_silver) / rows_raw:.2%})")

    print(f"[SILVER] Gravando camada SILVER em {output_dir}")
    (
        df.write.mode("overwrite")
        .partitionBy("pickup_month")
        .parquet(output_dir)
    )
    print("[SILVER] Concluido.")


def main():
    parser = argparse.ArgumentParser(description="Transformacao da camada SILVER")
    parser.add_argument("--input-dir", required=True, help="Diretorio da camada raw")
    parser.add_argument("--output-dir", required=True, help="Diretorio de saida da camada silver")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        silver_transform(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

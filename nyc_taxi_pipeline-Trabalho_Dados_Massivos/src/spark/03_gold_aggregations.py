"""
03_gold_aggregations.py
-------------------------
Camada GOLD (Ouro) do pipeline NYC Yellow Taxi.

Responsabilidade:
    - Ler os dados tratados da camada SILVER.
    - Gerar os indicadores analiticos descritos no relatorio (secoes 4.4 e 7):
        * Viagens por hora do dia
        * Viagens por dia da semana
        * Receita total por periodo (mes)
        * Distancia media percorrida
        * Duracao media das viagens
        * Regioes (grade lat/lon) com maior demanda
        * Formas de pagamento mais utilizadas
        * Indicadores de arrecadacao e gorjetas

    Cada indicador e salvo como uma tabela Parquet independente dentro da
    camada gold/, pronta para consumo por ferramentas de BI (Power BI /
    Streamlit), conforme citado no relatorio (secao 6).

Uso:
    spark-submit 03_gold_aggregations.py \
        --input-dir data/lake/silver \
        --output-dir data/lake/gold
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PAYMENT_TYPE_LABELS = {
    1: "Cartao de credito",
    2: "Dinheiro",
    3: "Sem cobranca",
    4: "Disputa",
    5: "Desconhecido",
    6: "Viagem anulada",
}

WEEKDAY_LABELS = {
    1: "Domingo",
    2: "Segunda-feira",
    3: "Terca-feira",
    4: "Quarta-feira",
    5: "Quinta-feira",
    6: "Sexta-feira",
    7: "Sabado",
}


def build_spark_session(app_name: str = "NYC_Taxi_Gold_Aggregations") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def map_labels(df, source_col, mapping, target_col):
    mapping_expr = F.create_map([F.lit(x) for pair in mapping.items() for x in pair])
    return df.withColumn(target_col, mapping_expr.getItem(F.col(source_col)))


def trips_by_hour(df):
    """Demanda por horario do dia (padrao de horarios de pico)."""
    return (
        df.groupBy("pickup_month", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy("pickup_month", "pickup_hour")
    )


def trips_by_weekday(df):
    """Distribuicao de viagens ao longo da semana."""
    result = (
        df.groupBy("pickup_month", "pickup_dayofweek")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_month", "pickup_dayofweek")
    )
    result = map_labels(result, "pickup_dayofweek", WEEKDAY_LABELS, "weekday_name")
    return result


def revenue_by_period(df):
    """Receita total e indicadores financeiros por mes de referencia."""
    return (
        df.groupBy("pickup_month")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("tolls_amount"), 2).alias("total_tolls"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy("pickup_month")
    )


def distance_duration_stats(df):
    """Distancia media percorrida e duracao media das viagens."""
    return (
        df.groupBy("pickup_month")
        .agg(
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance_miles"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
        )
        .orderBy("pickup_month")
    )


def demand_by_region(df, grid_precision=2):
    """
    Regioes com maior demanda. Como a base nao traz IDs de zona (taxi_zone),
    a regiao e aproximada por uma grade de coordenadas de embarque
    (latitude/longitude arredondadas), conforme citado no relatorio
    (secao 3.3 - atributos geograficos).
    """
    df_region = df.withColumn(
        "pickup_lat_grid", F.round("pickup_latitude", grid_precision)
    ).withColumn("pickup_lon_grid", F.round("pickup_longitude", grid_precision))

    return (
        df_region.groupBy("pickup_month", "pickup_lat_grid", "pickup_lon_grid")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy(F.desc("trip_count"))
    )


def payment_type_distribution(df):
    """Comportamento dos meios de pagamento utilizados pelos passageiros."""
    result = (
        df.groupBy("pickup_month", "payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_month", "payment_type")
    )
    result = map_labels(result, "payment_type", PAYMENT_TYPE_LABELS, "payment_type_label")
    return result


def peak_hours(df):
    """Identificacao dos horarios de pico (top horarios por volume de viagens)."""
    return (
        df.groupBy("pickup_month", "pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("pickup_month").orderBy(F.desc("trip_count"))
            ),
        )
        .filter(F.col("rank") <= 5)
        .orderBy("pickup_month", "rank")
    )


def gold_aggregations(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    print(f"[GOLD] Lendo camada SILVER em {input_dir}")
    df = spark.read.parquet(input_dir).cache()
    print(f"[GOLD] Registros disponiveis: {df.count()}")

    tables = {
        "trips_by_hour": trips_by_hour(df),
        "trips_by_weekday": trips_by_weekday(df),
        "revenue_by_period": revenue_by_period(df),
        "distance_duration_stats": distance_duration_stats(df),
        "demand_by_region": demand_by_region(df),
        "payment_type_distribution": payment_type_distribution(df),
        "peak_hours": peak_hours(df),
    }

    for table_name, table_df in tables.items():
        out_path = f"{output_dir}/{table_name}"
        print(f"[GOLD] Gravando tabela '{table_name}' em {out_path}")
        table_df.write.mode("overwrite").parquet(out_path)

        # Tambem exporta uma copia em CSV (uma so particao) para facilitar
        # o consumo direto em ferramentas de BI / dashboards.
        table_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            f"{out_path}_csv"
        )

    print("[GOLD] Concluido. Tabelas geradas:", list(tables.keys()))


def main():
    parser = argparse.ArgumentParser(description="Geracao da camada GOLD")
    parser.add_argument("--input-dir", required=True, help="Diretorio da camada silver")
    parser.add_argument("--output-dir", required=True, help="Diretorio de saida da camada gold")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        gold_aggregations(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

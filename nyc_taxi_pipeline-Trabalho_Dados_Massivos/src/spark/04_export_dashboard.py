"""
04_export_dashboard.py
-------------------------
Etapa final do pipeline: consolida as tabelas da camada GOLD em um unico
diretorio de exportacao (CSV), pronto para ser consumido por ferramentas de
visualizacao como Power BI ou Streamlit, conforme citado no relatorio
(secao 6 - Tecnologias).

Uso:
    spark-submit 04_export_dashboard.py \
        --input-dir data/lake/gold \
        --output-dir data/export/dashboard
"""

import argparse
import os
import shutil

from pyspark.sql import SparkSession

GOLD_TABLES = [
    "trips_by_hour",
    "trips_by_weekday",
    "revenue_by_period",
    "distance_duration_stats",
    "demand_by_region",
    "payment_type_distribution",
    "peak_hours",
]


def build_spark_session(app_name: str = "NYC_Taxi_Export_Dashboard") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def export_dashboard(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    for table_name in GOLD_TABLES:
        table_path = os.path.join(input_dir, table_name)
        if not os.path.exists(table_path):
            print(f"[EXPORT] AVISO: tabela {table_name} nao encontrada em {table_path}, ignorando.")
            continue

        print(f"[EXPORT] Exportando tabela '{table_name}'")
        df = spark.read.parquet(table_path)

        tmp_path = os.path.join(output_dir, f"_tmp_{table_name}")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_path)

        # Move o unico arquivo part-*.csv gerado para um nome amigavel
        final_csv = os.path.join(output_dir, f"{table_name}.csv")
        for fname in os.listdir(tmp_path):
            if fname.startswith("part-") and fname.endswith(".csv"):
                shutil.move(os.path.join(tmp_path, fname), final_csv)
        shutil.rmtree(tmp_path, ignore_errors=True)

        print(f"[EXPORT] Tabela '{table_name}' exportada em {final_csv}")

    print(f"[EXPORT] Exportacao concluida em {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Exportacao das tabelas GOLD para dashboard")
    parser.add_argument("--input-dir", required=True, help="Diretorio da camada gold")
    parser.add_argument("--output-dir", required=True, help="Diretorio de exportacao final")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        export_dashboard(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

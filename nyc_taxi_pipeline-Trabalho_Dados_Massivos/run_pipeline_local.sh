#!/usr/bin/env bash
###############################################################################
# run_pipeline_local.sh
#
# Executa o pipeline completo localmente, sem Airflow, na mesma ordem definida
# na DAG (extract_raw >> clean_silver >> create_gold >> export_dashboard).
# Util para testes, depuracao e para gerar os resultados usados no relatorio
# antes de configurar o ambiente Airflow.
#
# Uso:
#   ./run_pipeline_local.sh
#
# Pre-requisitos:
#   - Apache Spark instalado (spark-submit disponivel no PATH), ou
#     'pip install pyspark' em um ambiente local.
#   - Os arquivos CSV originais devem estar em data/raw_csv/
###############################################################################
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_DIR="$PROJECT_DIR/src/spark"
DATA_DIR="$PROJECT_DIR/data"

RAW_INPUT_DIR="$DATA_DIR/raw_csv"
RAW_OUTPUT_DIR="$DATA_DIR/lake/raw"
SILVER_OUTPUT_DIR="$DATA_DIR/lake/silver"
GOLD_OUTPUT_DIR="$DATA_DIR/lake/gold"
DASHBOARD_OUTPUT_DIR="$DATA_DIR/export/dashboard"

echo "=================================================================="
echo " Pipeline NYC Yellow Taxi - Execucao Local"
echo "=================================================================="

echo ""
echo "[1/4] extract_raw"
spark-submit "$SPARK_DIR/01_ingest_raw.py" \
    --input-dir "$RAW_INPUT_DIR" \
    --output-dir "$RAW_OUTPUT_DIR"

echo ""
echo "[2/4] clean_silver"
spark-submit "$SPARK_DIR/02_silver_transform.py" \
    --input-dir "$RAW_OUTPUT_DIR" \
    --output-dir "$SILVER_OUTPUT_DIR"

echo ""
echo "[3/4] create_gold"
spark-submit "$SPARK_DIR/03_gold_aggregations.py" \
    --input-dir "$SILVER_OUTPUT_DIR" \
    --output-dir "$GOLD_OUTPUT_DIR"

echo ""
echo "[4/4] export_dashboard"
spark-submit "$SPARK_DIR/04_export_dashboard.py" \
    --input-dir "$GOLD_OUTPUT_DIR" \
    --output-dir "$DASHBOARD_OUTPUT_DIR"

echo ""
echo "=================================================================="
echo " Pipeline concluido. Resultados em: $DASHBOARD_OUTPUT_DIR"
echo "=================================================================="

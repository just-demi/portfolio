#!/bin/bash
set -e

# Paths
INPUT="src/data/adult.csv"
OUT_DIR="src/out_data_pipeline"
CSV_DIR="$OUT_DIR/csv_out"
PARQUET_DIR="$OUT_DIR/parquet_out"
ARCH_DIR="docs"

# ----------------------
# Run ETL Pipeline
# ----------------------
echo "=== Running ETL Pipeline ==="
python src/pipeline.py \
    --input "$INPUT" \
    --out-dir "$OUT_DIR" \
    --npartitions 4 \
    --expand-factor 5

# ----------------------
# Run Monitor
# ----------------------
echo "=== Running Pipeline Monitor ==="
python src/monitor.py --csv-dir "$CSV_DIR" --parquet-dir "$PARQUET_DIR"

# ----------------------
# Run Tests
# ----------------------
echo "=== Running Tests ==="
pytest -q tests/

# ----------------------
# Generate Architecture Diagram (PNG)
# ----------------------
echo "=== Generating Architecture Diagram ==="
mkdir -p $ARCH_DIR
dot -Tpng $ARCH_DIR/pipeline_flow.dot -o $ARCH_DIR/pipeline_flow.png
echo "Diagram saved at $ARCH_DIR/pipeline_flow.png"

echo "=== Pipeline Complete ==="
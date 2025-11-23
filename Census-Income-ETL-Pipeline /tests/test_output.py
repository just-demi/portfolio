import os
import pandas as pd
import pytest

# ----------------------------
# Setup paths relative to this test file
# ----------------------------
TEST_DIR = os.path.dirname(__file__)                  # tests/
PROJECT_ROOT = os.path.dirname(TEST_DIR)             # project root
SRC_DIR = os.path.join(PROJECT_ROOT, "src")          # src/
CSV_DIR = os.path.join(SRC_DIR, "out_data_pipeline", "csv_out")
PARQUET_DIR = os.path.join(SRC_DIR, "out_data_pipeline", "parquet_out")

# ----------------------------
# Helper to list files
# ----------------------------
def list_files(dir_path, ext):
    if not os.path.exists(dir_path):
        pytest.fail(f"Directory does not exist: {dir_path}")
    return [f for f in os.listdir(dir_path) if f.endswith(ext)]


# ----------------------------
# Tests
# ----------------------------
def test_csv_exists():
    files = list_files(CSV_DIR, ".csv")
    assert len(files) > 0, "No CSV files found"


def test_parquet_exists():
    files = list_files(PARQUET_DIR, ".parquet")
    assert len(files) > 0, "No Parquet files found"


def test_schema_match():
    csv_files = list_files(CSV_DIR, ".csv")
    parquet_files = list_files(PARQUET_DIR, ".parquet")

    # Just compare first CSV & Parquet
    csv_df = pd.read_csv(os.path.join(CSV_DIR, csv_files[0]))
    parquet_df = pd.read_parquet(os.path.join(PARQUET_DIR, parquet_files[0]))

    assert list(csv_df.columns) == list(parquet_df.columns), "CSV & Parquet schemas do not match"


def test_non_empty_csv():
    csv_files = list_files(CSV_DIR, ".csv")
    for f in csv_files:
        df = pd.read_csv(os.path.join(CSV_DIR, f))
        assert len(df) > 0, f"{f} is empty"


def test_non_empty_parquet():
    parquet_files = list_files(PARQUET_DIR, ".parquet")
    for f in parquet_files:
        df = pd.read_parquet(os.path.join(PARQUET_DIR, f))
        assert len(df) > 0, f"{f} is empty"
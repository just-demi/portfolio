#!/usr/bin/env python3
"""
ETL pipeline for Census Income dataset with expansion + mild jitter

Workflow:
- Read CSV
- Expand dataset by replication factor (--expand-factor), then apply mild jitter to numeric columns
- Repartition for batch processing
- Handle missing values (treat "?" as missing; drop small fraction; fill with mode otherwise)
- Encode categorical features:
    - Drop 'education' (keep 'education-num')
    - Drop 'fnlwgt'
    - Binary mapping for gender & income -> int 0/1
    - One-hot (after categorize) for low-cardinality cols -> int 0/1
    - Frequency encode native-country
- Transform numeric features:
    - capital-gain, capital-loss -> log1p then min-max scale
    - age, hours-per-week -> min-max scale
- Save results to partitioned Parquet and partitioned CSV
- Print stage timings and memory usage
"""

import argparse
import time
import os
import psutil
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd

# Use in-process threaded scheduler for local stability
dask.config.set(scheduler="threads")


# ---------------------------
# Helper: Timer context manager
# ---------------------------
def stage_timer(message):
    class Timer:
        def __enter__(self_inner):
            print(f"\n--- Starting: {message} ---")
            self_inner.t0 = time.time()
            return self_inner

        def __exit__(self_inner, exc_type, exc_val, exc_tb):
            elapsed = time.time() - self_inner.t0
            print(f"--- Finished: {message} in {elapsed:.2f} sec ---\n")
    return Timer()


# ---------------------------
# CLI args
# ---------------------------
def parse_args():
    p = argparse.ArgumentParser(description="ETL pipeline with expansion & jitter")
    p.add_argument("--input", required=True, help="Path to input CSV file")
    p.add_argument("--out-dir", required=True, help="Directory to write outputs")
    p.add_argument("--npartitions", type=int, default=4, help="Base number of Dask partitions (scaled by expand factor)")
    p.add_argument("--expand-factor", type=int, default=1, help="Replication factor to expand dataset (1 = no expansion)")
    p.add_argument("--max-partitions", type=int, default=200, help="Cap for total partitions after expansion")
    p.add_argument("--persist", action="store_true", help="Persist intermediate Dask DataFrame (may use memory)")
    return p.parse_args()


# ---------------------------
# Memory helper
# ---------------------------
def measure_mem_mb():
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss / (1024 * 1024)


# ---------------------------
# Jitter function (mild)
# ---------------------------
def apply_mild_jitter(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Applies mild jitter to numeric columns in-place on a pandas DataFrame partition.
    - age: + Normal(0, 1.5), clip [0, 100]
    - hours-per-week: + Normal(0, 2), clip [1, 100]
    - capital-gain / capital-loss: small multiplicative noise (1 + Normal(0, 0.01))
    - fnlwgt is expected to be dropped later; if present we won't jitter it.
    """
    if pdf.empty:
        return pdf

    # age
    if "age" in pdf.columns:
        try:
            noise = np.random.normal(loc=0.0, scale=1.5, size=len(pdf))
            pdf["age"] = pd.to_numeric(pdf["age"], errors="coerce").fillna(0) + noise
            pdf["age"] = pdf["age"].clip(lower=0, upper=100)
        except Exception:
            pass

    # hours-per-week
    if "hours-per-week" in pdf.columns:
        try:
            noise = np.random.normal(loc=0.0, scale=2.0, size=len(pdf))
            pdf["hours-per-week"] = pd.to_numeric(pdf["hours-per-week"], errors="coerce").fillna(0) + noise
            pdf["hours-per-week"] = pdf["hours-per-week"].clip(lower=1, upper=100)
        except Exception:
            pass

    # capital-gain and capital-loss: multiplicative mild jitter
    for col in ("capital-gain", "capital-loss"):
        if col in pdf.columns:
            try:
                vals = pd.to_numeric(pdf[col], errors="coerce").fillna(0)
                # multiplicative factor ~ N(1, 0.01) clipped to [0.9, 1.1]
                factors = np.random.normal(loc=1.0, scale=0.01, size=len(pdf))
                factors = np.clip(factors, 0.9, 1.1)
                pdf[col] = (vals * factors).clip(lower=0)
            except Exception:
                pass

    return pdf


# ---------------------------
# Min-max scaling helpers
# ---------------------------
def minmax_scale_series(series, min_val, max_val):
    # Avoid division by zero
    denom = max_val - min_val
    if denom == 0:
        return series - min_val  # will be zeros
    return (series - min_val) / denom


# ---------------------------
# Main pipeline
# ---------------------------
def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    print("Dask scheduler: threads (in-process).")

    mem_start = measure_mem_mb()
    pipeline_start = time.time()

    # ----------------------------
    # READ CSV
    # ----------------------------
    with stage_timer("Reading CSV into Dask DataFrame"):
        df = dd.read_csv(args.input, assume_missing=True, blocksize="64MB")
        df = df.repartition(npartitions=args.npartitions)
        print(f"Loaded: {df.npartitions} partitions, columns: {list(df.columns)}")

    # ----------------------------
    # EXPAND (replicate) + APPLY JITTER
    # ----------------------------
    expand_factor = max(1, int(args.expand_factor))
    if expand_factor > 1:
        with stage_timer(f"Expanding dataset by factor x{expand_factor} (replicate + mild jitter)"):
            # Create list of references (cheap) and concat - Dask will handle lazily
            df_list = [df] * expand_factor
            df = dd.concat(df_list, interleave_partitions=True)

            # Repartition to increase number of partitions for parallel processing
            target_nparts = min(args.npartitions * expand_factor, args.max_partitions)
            try:
                df = df.repartition(npartitions=target_nparts)
            except Exception:
                # fallback: keep current partitions
                pass

            # Apply jitter per partition (mild)
            df = df.map_partitions(apply_mild_jitter, meta=df._meta)
            print(f"Expanded and jittered DataFrame; partitions: {df.npartitions}")

    else:
        print("No expansion requested (expand-factor=1).")

    # ----------------------------
    # HANDLE MISSING VALUES SMARTLY
    # ----------------------------
    with stage_timer("Handling missing categorical values"):
        missing_cols = ["workclass", "occupation", "native-country"]
        for col in missing_cols:
            if col in df.columns:
                # Treat "?" as missing
                df[col] = df[col].replace("?", None)

                # compute fraction of missing (safe compute)
                missing_frac = df[col].isna().sum().compute() / len(df)
                print(f"Column '{col}' missing fraction: {missing_frac:.4f}")

                if missing_frac < 0.01:
                    print(f"Dropping rows with missing '{col}'")
                    df = df.dropna(subset=[col])
                else:
                    mode_val = df[col].mode().compute()[0]
                    print(f"Filling missing '{col}' with mode: {mode_val}")
                    df[col] = df[col].fillna(mode_val)

    # ----------------------------
    # ENCODE CATEGORICAL COLUMNS
    # ----------------------------
    with stage_timer("Encoding categorical features"):
        # Drop original 'education' and keep 'education-num'
        if "education" in df.columns:
            df = df.drop("education", axis=1)

        # Drop fnlwgt entirely
        if "fnlwgt" in df.columns:
            df = df.drop("fnlwgt", axis=1)

        # Binary mapping to numeric ints
        if "gender" in df.columns:
            df["gender"] = df["gender"].map({"Female": 0, "Male": 1}).astype(int)
        if "income" in df.columns:
            df["income"] = df["income"].map({"<=50K": 0, ">50K": 1}).astype(int)

        # One-hot encoding for low-cardinality columns (categorize then get_dummies)
        one_hot_cols = ["workclass", "marital-status", "occupation", "relationship", "race"]
        existing_one_hot_cols = [c for c in one_hot_cols if c in df.columns]
        if existing_one_hot_cols:
            df[existing_one_hot_cols] = df[existing_one_hot_cols].categorize()
            df = dd.get_dummies(df, columns=existing_one_hot_cols)

            # convert boolean / uint8 to int 0/1 explicitly
            ohe_cols = [c for c in df.columns if any(c.startswith(orig + "_") for orig in existing_one_hot_cols)]
            if ohe_cols:
                df[ohe_cols] = df[ohe_cols].astype(int)

        # Frequency encoding for 'native-country'
        if "native-country" in df.columns:
            country_counts = df["native-country"].value_counts().compute()
            df["native-country"] = df["native-country"].map(lambda x: country_counts.get(x, 0))

    # ----------------------------
    # NUMERIC FEATURE TRANSFORMATIONS
    # ----------------------------
    with stage_timer("Transforming numeric features"):
        # Ensure columns are present and numeric
        numeric_cols = []
        if "age" in df.columns:
            numeric_cols.append("age")
        if "hours-per-week" in df.columns:
            numeric_cols.append("hours-per-week")
        if "capital-gain" in df.columns:
            numeric_cols.append("capital-gain")
        if "capital-loss" in df.columns:
            numeric_cols.append("capital-loss")

        # Apply log1p to capital gain/loss if present
        for c in ("capital-gain", "capital-loss"):
            if c in df.columns:
                df[c] = df[c].map_partitions(lambda s: np.log1p(pd.to_numeric(s, errors="coerce").fillna(0)), meta=(c, "float64"))

        # For age and hours-per-week (and log-transformed capital cols), compute min/max then apply min-max scaling
        # Gather min/max
        to_scale = [c for c in ["age", "hours-per-week", "capital-gain", "capital-loss"] if c in df.columns]
        if to_scale:
            stats_min = df[to_scale].min().compute()
            stats_max = df[to_scale].max().compute()
            stats_min = stats_min.to_dict()
            stats_max = stats_max.to_dict()

            # map_partitions scaling function needs to use the numeric min/max values
            def scale_partition(pdf: pd.DataFrame, mins: dict, maxs: dict):
                for col in to_scale:
                    if col in pdf.columns:
                        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)
                        min_v = mins.get(col, 0)
                        max_v = maxs.get(col, min_v)
                        denom = max_v - min_v
                        if denom == 0:
                            pdf[col] = pdf[col] - min_v
                        else:
                            pdf[col] = (pdf[col] - min_v) / denom
                return pdf

            df = df.map_partitions(scale_partition, mins=stats_min, maxs=stats_max, meta=df._meta)

    # ----------------------------
    # OPTIONAL: persist (if requested)
    # ----------------------------
    if args.persist:
        with stage_timer("Persisting Dask DataFrame in memory"):
            df = df.persist()

    # ----------------------------
    # WRITE OUTPUTS (Parquet + CSV)
    # ----------------------------
    with stage_timer("Writing output to Parquet and CSV (partitioned)"):
        parquet_out = os.path.join(args.out_dir, "parquet_out")
        csv_out = os.path.join(args.out_dir, "csv_out")
        os.makedirs(parquet_out, exist_ok=True)
        os.makedirs(csv_out, exist_ok=True)

        # Parquet (partitioned)
        df.to_parquet(parquet_out, engine="pyarrow", write_index=False)

        # CSV (many files, one per partition)
        df.to_csv(os.path.join(csv_out, "part-*.csv"), index=False, single_file=False)

        print(f"Outputs written to: {parquet_out} (parquet) and {csv_out} (csv)")

    # ----------------------------
    # Final stats
    # ----------------------------
    pipeline_end = time.time()
    mem_end = measure_mem_mb()
    print("\n============== Performance Summary ==============")
    print(f"Total pipeline time: {pipeline_end - pipeline_start:.2f} sec")
    print(f"Memory start: {mem_start:.1f} MB")
    print(f"Memory end:   {mem_end:.1f} MB")
    print(f"Memory delta: {mem_end - mem_start:.1f} MB")
    print("=================================================\n")


if __name__ == "__main__":
    main()
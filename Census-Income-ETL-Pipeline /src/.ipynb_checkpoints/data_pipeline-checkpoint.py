import dask.dataframe as dd

def process_large_csv(input_path, output_path):
    df = dd.read_csv(input_path)
    df = df.dropna()
    df["BMI"] = df["Weight"] / (df["Height"]/100)**2
    df.to_parquet(output_path, engine="pyarrow", overwrite=True)
    print("✅ Pipeline complete:", output_path)

if __name__ == "__main__":
    process_large_csv("../data/sample.csv", "../data/processed/")

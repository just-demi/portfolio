import pandas as pd

def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.fillna(df.median(numeric_only=True))
    return df

if __name__ == "__main__":
    df = load_data("../data/heart.csv")
    clean = clean_data(df)
    print(f"Cleaned shape: {clean.shape}")

from pathlib import Path
import pandas as pd

RAW = Path(__file__).resolve().parents[2] / "data" / "raw"

for path in sorted(RAW.glob("*.csv")):
    # sample large files; full read for small ones
    nrows = 5000 if path.name in {"transaction_data.csv", "causal_data.csv"} else None
    df = pd.read_csv(path, nrows=nrows)
    print(f"=== {path.name} ===")
    print("columns:", list(df.columns))
    print(df.head())
    print("nulls:\n", df.isna().sum())
    print()
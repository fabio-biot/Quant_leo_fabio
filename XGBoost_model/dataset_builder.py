from src.database import get_connection
import pandas as pd

def build_dataset():
    conn = get_connection()
    query = """
        SELECT 
            p.ticker,
            p.date,
            p.close,
            p.volume,
            f.market_cap,
            f.pe_ratio,
            t.target_5d
        FROM prices p
        JOIN fundamentals f 
            ON p.ticker = f.ticker
        JOIN targets t 
            ON p.ticker = t.ticker 
            AND p.date = t.date
        """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    dataset = build_dataset()
    print(dataset.head())
    dataset.to_csv("tables_csv/dataset_features_model.csv", index=False)
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

            fe.ma20,
            fe.ma50,
            fe.momentum_5,
            fe.bb_position,
            fe.rsi_14,
            fe.volatility,

            f.market_cap,
            f.pe_ratio,

            t.target_5d,

            m.interest_rate,
            m.inflation,
            m.vix

        FROM prices p

        JOIN features fe 
            ON p.ticker = fe.ticker 
            AND p.date = fe.date

        JOIN targets t 
            ON p.ticker = t.ticker 
            AND p.date = t.date

        LEFT JOIN (
            SELECT *
            FROM fundamentals f1
            WHERE f1.date = (
                SELECT MAX(f2.date)
                FROM fundamentals f2
                WHERE f2.ticker = f1.ticker
                AND f2.date <= f1.date
            )
        ) f
        ON p.ticker = f.ticker

        LEFT JOIN macro m
            ON date(p.date) = date(m.date)
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df = df.sort_values(["ticker", "date"])
    df = df.dropna(subset=["target_5d"])  # éviter target cassée

    return df


if __name__ == "__main__":
    dataset = build_dataset()
    print(dataset.head())
    dataset.to_csv("tables_csv/dataset_features_model.csv", index=False)
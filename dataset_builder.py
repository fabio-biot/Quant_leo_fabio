import pandas as pd
from src.database import get_connection


def build_dataset():
    conn = get_connection()

    query = """
    SELECT
        f.ticker,
        f.date,

        -- FEATURES
        f.ma20,
        f.ma50,
        f.bb_upper,
        f.bb_lower,
        f.bb_position,
        f.ma20_diff,
        f.momentum_5,
        f.return,
        f.volatility,
        f.rsi_14,
        f.close_lag_1,
        f.close_lag_2,

        -- FUNDAMENTALS
        fu.market_cap,
        fu.pe_ratio,

        -- MACRO
        m.inflation,
        m.interest_rate,
        m.vix,

        -- TARGETS
        t.future_return_5d,
        t.future_return_20d,
        t.target_5d,
        t.target_20d

    FROM features f

    LEFT JOIN fundamentals fu
        ON f.ticker = fu.ticker
        AND f.date = fu.date

    LEFT JOIN targets t
        ON f.ticker = t.ticker
        AND f.date = t.date

    LEFT JOIN macro m
        ON f.date = m.date

    ORDER BY f.ticker, f.date
    """
    df = pd.read_sql_query(query, conn)
    df_csv = df.to_csv("tables_csv/dataset.csv", index=False)
    conn.close()
    return df


if __name__ == "__main__":
    df = build_dataset()

    print(df.head())
    print(df.shape)

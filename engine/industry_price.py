import yfinance as yf
import pandas as pd
from src.database import get_connection


def fetch_industry_prices():
    conn = get_connection()

    query = """
    SELECT DISTINCT industry_etf
    FROM assets
    WHERE industry_etf IS NOT NULL
    """

    tickers_df = pd.read_sql(query, conn)
    rows = []
    for ticker in tickers_df["industry_etf"]:
        try:
            print(f"Processing industry ETF {ticker}")
            data = yf.download(
                ticker,
                period="3y",
                interval="1d",
                auto_adjust=True
            )
            if data.empty:
                continue
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data = data.reset_index()
            for _, row in data.iterrows():
                rows.append({
                    "ticker": ticker,
                    "date": row["Date"].date(),
                    "close": float(row["Close"]),
                    "volume": float(row["Volume"])
                })

        except Exception as e:
            print(f"Industry Error {ticker}: {e}")
    conn.close()
    return pd.DataFrame(rows)


def save_industry_prices(df):
    if df.empty:
        return
    conn = get_connection()
    df.to_sql(
        "industry_prices",
        conn,
        if_exists="append",
        index=False
    )
    df.to_csv(
        "tables_csv/industry_prices.csv",
        mode="a",
        header=False,
        index=False
    )
    conn.close()

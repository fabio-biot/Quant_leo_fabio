import yfinance as yf
import pandas as pd
from src.database import get_connection


def fetch_sector_prices():
    conn = get_connection()
    query = "SELECT DISTINCT sector_etf FROM assets WHERE sector_etf IS NOT NULL"
    tickers_df = pd.read_sql(query, conn)
    print(f"Found {len(tickers_df)} unique sector ETFs")
    print(tickers_df)
    rows = []
    for ticker in tickers_df["sector_etf"]:
        try:
            print(f"Processing sector ETF {ticker}")
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
            data = data[["Date", "Close", "Volume"]]
            data.columns = ["date", "close", "volume"]
            data["ticker"] = ticker
            data = data[["ticker", "date", "close", "volume"]]
            rows.extend(data.to_dict("records"))

        except Exception as e:
            print(f"Sector Error {ticker}: {e}")
    return pd.DataFrame(rows)


def save_sector_prices(df):
    if df.empty: return
    conn = get_connection()
    df.to_sql("sector_prices", conn, if_exists="append", index=False)
    df.to_csv("tables_csv/sector_prices.csv", mode="a", header=False, index=False)
    conn.close()
import yfinance as yf
import pandas as pd
from src.database import get_connection


def fetch_fundamentals(symbol="AAPL"):
    ticker = yf.Ticker(symbol)

    info = ticker.info

    data = {
        "ticker": symbol,
        "date": pd.to_datetime(pd.Timestamp.today().date(),format="%Y-%m-%d"),
        "market_cap": info.get("marketCap"),
        "pe_ratio": info.get("trailingPE")
    }

    return pd.DataFrame([data])


def save_fundamentals(df):
    conn = get_connection()
    df.to_sql("fundamentals", conn, if_exists="append", index=False)
    conn.close()

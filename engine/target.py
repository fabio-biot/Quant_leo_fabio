import yfinance as yf
import pandas as pd
from src.database import get_connection


def compute_targets(df: pd.DataFrame):
    df = df.sort_values(['ticker', 'date'])
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df['future_return_5d'] = df.groupby('ticker')['close'].shift(-5) / df['close'] - 1
    df['future_return_20d'] = df.groupby('ticker')['close'].shift(-20) / df['close'] - 1
    df['target_5d'] = (df['future_return_5d'] > 0).astype(int)
    df['target_20d'] = (df['future_return_20d'] > 0).astype(int)
    df = df.dropna(subset=['future_return_5d', 'future_return_20d'])
    return df[['ticker', 'date', 'future_return_5d', 'future_return_20d', 'target_5d', 'target_20d']]


def fetch_and_compute_targets(ticker: str, start_date: str, end_date: str):
    data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)
    if data.empty:
        return pd.DataFrame()
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    data = data.reset_index()
    data.columns = [c.lower() for c in data.columns]
    if 'ticker' not in data.columns:
        data['ticker'] = ticker
    return compute_targets(data)


def save_targets(df: pd.DataFrame):
    if df.empty:
        return
    conn = get_connection()
    df.to_sql("targets", conn, if_exists="append", index=False)
    df.to_csv("tables_csv/targets.csv", mode="a", header=False, index=False)
    conn.close()


import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from src.database import get_connection


def fetch_fred_series(series_id: str):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def fetch_interest_rate():
    df = fetch_fred_series("FEDFUNDS")
    df.rename(columns={"value": "interest_rate"}, inplace=True)
    return df[["date", "interest_rate"]]


def fetch_vix():
    vix = yf.download("^VIX", start="2020-01-01")
    if isinstance(vix.columns, pd.MultiIndex):
        vix.columns = vix.columns.get_level_values(0)
    vix = vix.reset_index(0)
    vix = vix[["Date", "Close"]]
    vix.rename(columns={"Date": "date", "Close": "vix"}, inplace=True)
    return vix


def fetch_inflation():
    df = fetch_fred_series("CPIAUCSL")
    df.rename(columns={"value": "inflation_index"}, inplace=True)
    df["inflation"] = df["inflation_index"].pct_change(12) * 100
    return df[["date", "inflation"]]


def build_macro_dataset():
    all_dates = pd.date_range(
        start="2020-01-01",
        end=datetime.today(),
        freq="D"
    )
    calendar = pd.DataFrame({"date": all_dates})
    inflation = fetch_inflation()
    rates = fetch_interest_rate()
    vix = fetch_vix()
    df = calendar.merge(inflation, on="date", how="left")
    df = df.merge(rates, on="date", how="left")
    df[["inflation", "interest_rate"]] = (
        df[["inflation", "interest_rate"]]
        .ffill()
    )
    df = df.merge(vix, on="date", how="left")
    df = df.sort_values("date")
    df = df.ffill().bfill()
    return df


def save_macro(df):
    conn = get_connection()
    df.to_sql("macro", conn, if_exists="append", index=False)
    df.to_csv("tables_csv/macro.csv", mode="a", header=False, index=False)
    conn.close()

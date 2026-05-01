import yfinance as yf
import pandas as pd
from src.database import get_connection


def fetch_assets_data(symbol):
    industry_dim = pd.read_csv("dim_tables/industry_dim.csv", sep=";")
    market_dim = pd.read_csv("dim_tables/market_dim.csv", sep=";")
    sector_dim = pd.read_csv("dim_tables/sector_dim.csv", sep=";")

    ticker = yf.Ticker(symbol)
    info = ticker.info
    data = {
        "ticker": symbol,
        "company_name": info.get("longName"),
        "market_sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_country": info.get("country"),
        "exchange": info.get("exchange"),
        "exchange_timezone": info.get("exchangeTimezoneName"),
        "region": info.get("region"),
        "financial_currency": info.get("financialCurrency"),
        "asset_type": info.get("quoteType"),
        "recommendation_key": info.get("recommendationKey"),
        "recommendation_mean": info.get("recommendationMean"),
        "current_price": info.get("currentPrice"),
        "target_high_price": info.get("targetHighPrice"),
        "target_low_price": info.get("targetLowPrice"),
        "target_mean_price": info.get("targetMeanPrice"),
        "target_median_price": info.get("targetMedianPrice"),
        "market_cap": info.get("marketCap"),
        "pe_ratio": info.get("trailingPE"),
        "audit_risk": info.get("auditRisk"),
        "board_risk": info.get("boardRisk"),
        "compensation_risk": info.get("compensationRisk"),
        "shareholder_rights_risk": info.get("shareHolderRightsRisk"),
        "overall_risk": info.get("overallRisk"),
        "updated_at": pd.Timestamp.today().date()
    }
    
    # Simple lookup
    try:
        data["industry_etf"] = industry_dim[industry_dim["industry"] == data["industry"]]["etf"].values[0]
    except: data["industry_etf"] = None
    try:
        data["market_index"] = market_dim[market_dim["market"] == data["market_country"]]["ticker"].values[0]
    except: data["market_index"] = None
    try:
        data["sector_etf"] = sector_dim[sector_dim["sector"] == data["market_sector"]]["etf"].values[0]
    except: data["sector_etf"] = None

    return pd.DataFrame([data])


def save_assets_data(df):
    conn = get_connection()
    df.to_sql("assets", conn, if_exists="append", index=False)
    df.to_csv("tables_csv/assets.csv", mode="a", header=False, index=False)
    conn.close()

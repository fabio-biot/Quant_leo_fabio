import yfinance as yf
import pandas as pd
from src.database import get_connection
import yfinance as yf


def fetch_assets_data(symbol):
    ticker = yf.Ticker(symbol)
    info = ticker.info
    data = {
        "ticker": symbol,

        # Company info
        "company_name": info.get("longName"),
        "market_sector": info.get("sector"),
        "industry": info.get("industry"),

        # Market info
        "market_country": info.get("country"),
        "exchange": info.get("exchange"),
        "exchange_timezone": info.get("exchangeTimezoneName"),
        "region": info.get("region"),
        "financial_currency": info.get("financialCurrency"),
        "asset_type": info.get("quoteType"),

        # Analyst recommendations
        "recommendation_key": info.get("recommendationKey"),
        "recommendation_mean": info.get("recommendationMean"),

        # Prices
        "current_price": info.get("currentPrice"),
        "target_high_price": info.get("targetHighPrice"),
        "target_low_price": info.get("targetLowPrice"),
        "target_mean_price": info.get("targetMeanPrice"),
        "target_median_price": info.get("targetMedianPrice"),

        # Financial metrics
        "market_cap": info.get("marketCap"),
        "pe_ratio": info.get("trailingPE"),

        # Governance / Risk
        "audit_risk": info.get("auditRisk"),
        "board_risk": info.get("boardRisk"),
        "compensation_risk": info.get("compensationRisk"),
        "shareholder_rights_risk": info.get("shareHolderRightsRisk"),
        "overall_risk": info.get("overallRisk"),
        "updated_at": pd.Timestamp.today().date()
    }
    return pd.DataFrame([data])


def save_assets_data(df):
    conn = get_connection()
    df.to_sql("assets", conn, if_exists="append", index=False)
    conn.close()

import pandas as pd
from src.database import get_connection


def build_relative_strength_signals() -> pd.DataFrame:

    conn = get_connection()
    prices = pd.read_sql("SELECT * FROM prices", conn)
    assets = pd.read_sql("SELECT * FROM assets", conn)
    features = pd.read_sql("SELECT * FROM features", conn)

    etf_features_industry = pd.read_sql(
        "SELECT * FROM etf_features_industry",
        conn
    )

    etf_features_sector = pd.read_sql(
        "SELECT * FROM etf_features_sector",
        conn
    )

    etf_features_market = pd.read_sql(
        "SELECT * FROM etf_features_market",
        conn
    )

    prices["date"] = pd.to_datetime(prices["date"])
    features["date"] = pd.to_datetime(features["date"])
    etf_features_sector["date"] = pd.to_datetime(
        etf_features_sector["date"]
    )

    etf_features_industry["date"] = pd.to_datetime(
        etf_features_industry["date"]
    )

    etf_features_market["date"] = pd.to_datetime(
        etf_features_market["date"]
    )

    df = prices.merge(
        assets,
        on="ticker",
        how="left",
        suffixes=("", "_ticker")
    )
    
    df = df.merge(
        features,
        on="key",
        how="left",
        suffixes=("", "_features")
    )
    df["sector_key"] = df["sector_etf"] + df["date"].dt.strftime("%Y-%m-%d")
    df["market_key"] = df["market_etf"] + df["date"].dt.strftime("%Y-%m-%d")
    df["industry_key"] = df["industry_etf"] + df["date"].dt.strftime("%Y-%m-%d")
    df = df.merge(
        etf_features_sector,
        left_on="sector_key",
        right_on="key",
        how="left",
        suffixes=("", "_sector")
    )

    df = df.merge(
        etf_features_industry,
        left_on="industry_key",
        right_on="key",
        how="left",
        suffixes=("", "_industry")
    )

    df = df.merge(
        etf_features_market,
        left_on="market_key",
        right_on="key",
        how="left",
        suffixes=("", "_market")
    )


    
# -----------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------


    df["rs_sector_signal"] = (
        df["momentum_60"] > df["Momentum_60"]
    ).astype(int)

    df["volatility_sector_signal"] = (
        df["volatility"] < df["Volatility"]
    ).astype(int)

    df["trend_sector_signal"] = (
        (df["ma20"] - df["ma50"]) >
        (df["MA20"] - df["MA50"])
    ).astype(int)

    df["rsi_sector_signal"] = (
        df["rsi_14"] > df["RSI_14"]
    ).astype(int)

    df["bb_position_sector_signal"] = (
        df["bb_position"] > df["BB_position"]
    ).astype(int)

    
    # -----------------------------------------------------------------------------------
    # INDUSTRY SIGNALS
    # -----------------------------------------------------------------------------------

    df["rs_industry_signal"] = (
        df["momentum_60"] > df["Momentum_60_industry"]
    ).astype(int)

    df["volatility_industry_signal"] = (
        df["volatility"] < df["Volatility_industry"]
    ).astype(int)

    df["trend_industry_signal"] = (
        (df["ma20"] - df["ma50"]) >
        (df["MA20_industry"] - df["MA50_industry"])
    ).astype(int)

    df["rsi_industry_signal"] = (
        df["rsi_14"] > df["RSI_14_industry"]
    ).astype(int)

    df["bb_position_industry_signal"] = (
        df["bb_position"] > df["BB_position_industry"]
    ).astype(int)
        


    # -----------------------------------------------------------------------------------
    # MARKET SIGNALS
    # -----------------------------------------------------------------------------------

    df["rs_market_signal"] = (
        df["momentum_60"] > df["Momentum_60_market"]
    ).astype(int)

    df["volatility_market_signal"] = (
        df["volatility"] < df["Volatility_market"]
    ).astype(int)

    df["trend_market_signal"] = (
        (df["ma20"] - df["ma50"]) >
        (df["MA20_market"] - df["MA50_market"])
    ).astype(int)

    df["rsi_market_signal"] = (
        df["rsi_14"] > df["RSI_14_market"]
    ).astype(int)

    df["bb_position_market_signal"] = (
        df["bb_position"] > df["BB_position_market"]
    ).astype(int)



    # -----------------------------------------------------------------------------------
    # GLOBAL QUANT SCORES
    # -----------------------------------------------------------------------------------

    signal_columns = [
        # Sector
        "rs_sector_signal",
        "volatility_sector_signal",
        "trend_sector_signal",
        "rsi_sector_signal",
        "bb_position_sector_signal",

        # Industry
        "rs_industry_signal",
        "volatility_industry_signal",
        "trend_industry_signal",
        "rsi_industry_signal",
        "bb_position_industry_signal",

        # Market
        "rs_market_signal",
        "volatility_market_signal",
        "trend_market_signal",
        "rsi_market_signal",
        "bb_position_market_signal"
    ]

    df["quant_score"] = df[signal_columns].sum(axis=1)

    # Score max = 15
    df["final_signal"] = (
        df["quant_score"] >= 10
    ).astype(int)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
# -----------------------------------------------------------------

    final_columns = [
        "key",
        "ticker",
        "date",
        "quant_score",
        "final_signal"] + signal_columns
    
    final_df = df[final_columns]
    final_df.to_csv("tables_csv/relative_strength_signals.csv",index=False)
    final_df.to_sql("relative_strength_signals", conn, if_exists="replace", index=False)

    conn.close()
    return final_df


build_relative_strength_signals()
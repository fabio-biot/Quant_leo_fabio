import pandas as pd
from src.database import get_connection
from sklearn.preprocessing import StandardScaler


def build_relative_strength_signals() -> pd.DataFrame:

    conn = get_connection()

    prices = pd.read_sql("SELECT * FROM prices", conn)
    assets = pd.read_sql("SELECT * FROM assets", conn)
    features = pd.read_sql("SELECT * FROM features", conn)

    etf_features_sector = pd.read_sql(
        "SELECT * FROM etf_features_sector",
        conn
    )

    etf_features_industry = pd.read_sql(
        "SELECT * FROM etf_features_industry",
        conn
    )

    etf_features_market = pd.read_sql(
        "SELECT * FROM etf_features_market",
        conn
    )

    # ------------------------------------------------------------
    # DATES
    # ------------------------------------------------------------

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

    # ------------------------------------------------------------
    # MAIN DF
    # ------------------------------------------------------------

    df = prices.merge(
        assets,
        on="ticker",
        how="left"
    )

    df = df.merge(
        features,
        on="key",
        how="left",
        suffixes=("", "_features")
    )

    # ------------------------------------------------------------
    # ETF KEYS
    # ------------------------------------------------------------

    df["sector_key"] = (
        df["sector_etf"] +
        df["date"].dt.strftime("%Y-%m-%d")
    )

    df["industry_key"] = (
        df["industry_etf"] +
        df["date"].dt.strftime("%Y-%m-%d")
    )

    df["market_key"] = (
        df["market_etf"] +
        df["date"].dt.strftime("%Y-%m-%d")
    )

    # ------------------------------------------------------------
    # MERGE ETF FEATURES
    # ------------------------------------------------------------

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

    # ------------------------------------------------------------
    # RELATIVE MOMENTUM
    # ------------------------------------------------------------

    df["rs_sector_strength"] = df["Momentum_5"] - df["Momentum_5_sector"]
    df["rs_industry_strength"] = df["Momentum_5"] - df["Momentum_5_industry"]
    df["rs_market_strength"] = df["Momentum_5"] - df["Momentum_5_market"]

    # ------------------------------------------------------------
    # RELATIVE TREND
    # ------------------------------------------------------------

    df["trend_strength_sector"] = (
        (df["MA20"] / df["MA50"]) -
        (df["MA20"] / df["MA50"])
    )

    df["trend_strength_industry"] = (
        (df["MA20"] / df["MA50"]) -
        (df["MA20_industry"] / df["MA50_industry"])
    )

    df["trend_strength_market"] = (
        (df["MA20"] / df["MA50"]) -
        (df["MA20_market"] / df["MA50_market"])
    )

    # ------------------------------------------------------------
    # RELATIVE VOLATILITY
    # ------------------------------------------------------------

    df["volatility_strength_sector"] = df["Volatility_sector"] - df["Volatility"]
    df["volatility_strength_industry"] = df["Volatility_industry"] - df["Volatility"]
    df["volatility_strength_market"] = df["Volatility_market"] - df["Volatility"]

    # ------------------------------------------------------------
    # RSI STRENGTH
    # ------------------------------------------------------------

    df["rsi_strength_sector"] = df["RSI_14"] - df["RSI_14_sector"]
    df["rsi_strength_industry"] = df["RSI_14"] - df["RSI_14_industry"]
    df["rsi_strength_market"] = df["RSI_14"] - df["RSI_14_market"]

    # ------------------------------------------------------------
    # BB POSITION STRENGTH
    # ------------------------------------------------------------

    df["bb_strength_sector"] = df["BB_position"] - df["BB_position_sector"]
    df["bb_strength_industry"] = df["BB_position"] - df["BB_position_industry"]
    df["bb_strength_market"] = df["BB_position"] - df["BB_position_market"]

    # ============================================================
    # COMPOSITE SCORES
    # ============================================================

    # ------------------------------------------------------------
    # RELATIVE LEADERSHIP SCORE
    # ------------------------------------------------------------

    df["relative_leadership_score"] = (
        3 * df["rs_sector_strength"] +
        2 * df["rs_industry_strength"] +
        1 * df["rs_market_strength"]
    )

    # ------------------------------------------------------------
    # TREND QUALITY SCORE
    # ------------------------------------------------------------

    df["trend_quality_score"] = (
        2 * df["trend_strength_sector"] +
        1.5 * df["trend_strength_industry"] +
        1 * df["rsi_strength_sector"] +
        1 * df["bb_strength_sector"]
    )

    # ------------------------------------------------------------
    # RISK SCORE
    # ------------------------------------------------------------

    df["risk_score"] = (
        2 * df["volatility_strength_sector"] +
        1.5 * df["volatility_strength_market"]
    )

    # ============================================================
    # NORMALIZATION
    # ============================================================

    score_columns = [
        "relative_leadership_score",
        "trend_quality_score",
        "risk_score"
    ]

    scaler = StandardScaler()

    df[score_columns] = scaler.fit_transform(df[score_columns])

    # ============================================================
    # FINAL QUANT SCORE
    # ============================================================

    df["quant_score"] = (0.45 * df["relative_leadership_score"] + 0.35 * \
                         df["trend_quality_score"] + 0.20 * df["risk_score"])
    df["daily_rank"] = df.groupby("date")["quant_score"].rank(ascending=False, method="dense")
    df["final_signal"] = "AVOID"
    df.loc[df["quant_score"] > 1.5, "final_signal"] = "BUY"
    df.loc[((df["quant_score"] > 0.5) & (df["quant_score"] <= 1.5)), "final_signal"] = "WATCH"

    # ============================================================
    # MAIN DRIVER
    # ============================================================

    df["main_driver"] = "neutral"
    df.loc[df["relative_leadership_score"] > df["trend_quality_score"], "main_driver"] = "relative_strength"
    df.loc[df["trend_quality_score"] > df["relative_leadership_score"], "main_driver"] = "trend"

    # ============================================================
    # MAIN RISK
    # ============================================================

    df["main_risk"] = "normal"
    df.loc[df["risk_score"] < -1, "main_risk"] = "high_volatility"

    # ============================================================
    # SIGNALS COMBINATOIN
    # ============================================================

    # ------------------ Trend Confirmation ----------------------
    df["trend_signal"] = (df["MA20"] - df["MA50"]) / df["MA50"]
    df["momentum_signal"] = df["Momentum_20"]
    df["rs_signal"] = df["rs_sector_strength"]
    df["volume_signal"] = df["volume"] / df["volume"].rolling(20).mean()

    df["trend_confirmation_score"] = (
        0.4 * df["trend_signal"] +
        0.3 * df["momentum_signal"] +
        0.2 * df["rs_signal"] +
        0.1 * df["volume_signal"]
    )

    # ------------------ Mean Reversion Opportunity --------------
    df["mr_distance"] = (df["close"] - df["MA20"]) / df["MA20"]
    df["mr_rsi"] = (30 - df["RSI_14"]) / 30
    df["mr_bb"] = 1 - df["BB_position"]
    df["mr_trend"] = df["Momentum_60"]

    df["mean_reversion_score"] = (
        0.4 * (-df["mr_distance"]) +
        0.3 * df["mr_rsi"] +
        0.2 * df["mr_bb"] +
        0.1 * df["mr_trend"]
    )

    # -------------------- Breakout Score -----------------------
    df["breakout_distance"] = df["close"] / df["close"].rolling(20).max() - 1
    df["breakout_volume"] = df["volume"] / df["volume"].rolling(20).mean()
    df["breakout_volatility"] = df["Volatility"]

    df["breakout_score"] = (
        0.5 * df["breakout_distance"] +
        0.3 * df["breakout_volume"] -
        0.2 * df["breakout_volatility"]
    )

    # -------------------- Quality Momentum ----------------------
    df["qm_momentum"] = df["Momentum_20"]
    df["qm_volatility"] = df["Volatility"]
    df["qm_consistency"] = (
        (df["close"].pct_change() > 0).rolling(20).mean()
    )

    df["quality_momentum_score"] = (
        0.5 * df["qm_momentum"] +
        0.3 * df["qm_consistency"] -
        0.2 * df["qm_volatility"]
    )

    # -------------------- Mean Reversion Score ------------------
    df["mr_distance"] = (df["close"] - df["MA20"]) / df["MA20"]
    df["mr_rsi"] = (50 - df["RSI_14"]) / 50
    df["mr_bb"] = 0.5 - df["BB_position"]
    df["mr_volatility"] = df["Volatility"]

    df["mean_reversion_score"] = (
        -0.4 * df["mr_distance"] +
        0.3 * df["mr_rsi"] +
        0.2 * df["mr_bb"] -
        0.1 * df["mr_volatility"]
    )

    # ----------------- Relative leadership score ----------------
    # df["relative_leadership_score"] = (
    #     0.5 * df["rs_sector_strength_20"] + Faudrait creer ces colonnes mais bon ...
    #     0.3 * df["rs_sector_strength_60"] +
    #     0.2 * df["rs_sector_strength_5"]
    # )

    # ----------------- momentum acceleration score --------------------
    df["momentum_acceleration_score"] = (
        0.6 * (df["Momentum_5"] - df["Momentum_20"]) +
        0.4 * (df["Momentum_20"] - df["Momentum_60"])
    )

    # ------------------------- Trend Quality Score --------------------
    df["trend_slope"] = (df["MA20"] - df["MA50"]) / df["MA50"]
    df["trend_stability"] = 1 / (df["Volatility"] + 1e-6)
    df["trend_persistence"] = (
        (df["close"] > df["MA20"]).rolling(20).mean()
    )
    df["trend_quality_score"] = (
        0.5 * df["trend_slope"] +
        0.3 * df["trend_persistence"] +
        0.2 * df["trend_stability"]
    )

    # -------------- Risk OFF SCOre -----------------------------------
    # Pour la faire simple, Volatility du marche c est Market Stress et Market momentum pour momentum20
    df["risk_off_score"] = (
        0.6 * df["Volatility_market"] -
        0.4 * df["Momentum_20_market"]
    )

    # ------------------ Liquidity Score ----------------------
    df["liquidity_score"] = (
            0.7 * df["volume"] -
            0.3 * df["volume"].rolling(20).std()
        )
    
    # ------------------ Consistency Score ----------------------
    df["consistency_score"] = (
        0.6 * ((df["close"].pct_change() > 0).rolling(20).mean()) -
        0.4 * abs(df["close"] / df["close"].rolling(20).max() - 1)
    )

    # ------------------ Final Alpha Score ----------------------
    df["final_alpha_score"] = (
        0.25 * df["relative_leadership_score"] +
        0.20 * df["trend_quality_score"] +
        0.15 * df["quality_momentum_score"] +
        0.15 * df["breakout_score"] +
        0.10 * df["mean_reversion_score"] +
        0.10 * df["momentum_acceleration_score"] -
        0.05 * df["risk_off_score"]
    )
    # ============================================================
    # OUTPUT
    # ============================================================

    final_columns = [

        "ticker",
        "date",
        "key",

        "quant_score",
        "daily_rank",
        "final_signal",

        "relative_leadership_score",
        "trend_quality_score",
        "risk_score",

        "main_driver",
        "main_risk"

    ]

    final_df = df[final_columns]
    final_df = final_df.sort_values(["date", "daily_rank"])

    final_df.to_csv("tables_csv/relative_strength_signals.csv", index=False)
    final_df.to_sql("relative_strength_signals", conn, if_exists="replace", index=False)

    conn.close()
    return final_df


build_relative_strength_signals()
import pandas as pd
import numpy as np
from datetime import datetime
import os
import joblib
from src.database import get_connection
from engine.prices import fetch_prices
from engine.indicators import add_advanced_indicators
from engine.engineer import tag_signals, handle_outliers, sector_neutralize, cross_sectional_rank, preprocess_for_inference
from models.ml_pipeline import ModelTrainer
from src.config import TICKERS, REPORTS_DIR, MIN_PROBABILITY_BUY, MODELS_DIR

def generate_daily_signals():
    """
    Production entry point: Generate actionable signals for TODAY.
    """
    print(f"\n--- GENERATING DAILY SIGNALS ({datetime.now().strftime('%Y-%m-%d')}) ---")
    
    all_data = []
    raw_latest_data = [] # To keep un-ranked values for the report
    
    for ticker in TICKERS:
        print(f"Fetching latest data for {ticker}...")
        df_ticker = fetch_prices(ticker, "2023-01-01", datetime.now().strftime('%Y-%m-%d'))
        if df_ticker.empty:
            continue
            
        # 1. Calculate indicators on FULL history to avoid NaN in long windows (MA200, etc.)
        df_ticker = add_advanced_indicators(df_ticker)
        
        # Keep the latest raw values for the report before ranking
        raw_latest_data.append(df_ticker.tail(1).copy())
        
        # Append enough history for ranking/neutralization (60 days is fine for cross-section)
        all_data.append(df_ticker.tail(60))
            
    if not all_data:
        print("Error: No data fetched.")
        return

    df = pd.concat(all_data).reset_index()
    if 'Date' in df.columns:
        df.rename(columns={'Date': 'date'}, inplace=True)
    
    # Keep raw data mapping for later
    df_raw = pd.concat(raw_latest_data).reset_index()
    if 'Date' in df_raw.columns:
        df_raw.rename(columns={'Date': 'date'}, inplace=True)

    # 2. Engineering (Ranking, Neutralization, Macro)
    df = preprocess_for_inference(df)
    df = tag_signals(df)
    
    # 3. Load Models and Predict
    xgb = ModelTrainer.load_model("XGBoost")
    cat = ModelTrainer.load_model("CatBoost")
    
    if not xgb or not cat:
        print("Error: Models not found. Run 'make train-ml' first.")
        return
    
    # Prepare features for the latest date
    latest_date = df['date'].max()
    df_latest = df[df['date'] == latest_date].copy()
    
    # Load the EXACT feature list used during training
    feature_list_path = os.path.join(MODELS_DIR, "feature_list.joblib")
    if not os.path.exists(feature_list_path):
        print("Error: Feature list not found. Run 'make train-ml' first.")
        return

    feature_list = joblib.load(feature_list_path)
    X = ModelTrainer.build_feature_matrix(df_latest, feature_list)
    
    # Ensemble Prediction
    prob_xgb = xgb.predict_proba(X)[:, 1]
    prob_cat = cat.predict_proba(X)[:, 1]
    df_latest['Alpha Score'] = (prob_xgb + prob_cat) / 2
    
    # 4. Filter and Sort
    df_latest = df_latest.sort_values('Alpha Score', ascending=False)
    
    # 5. Create Daily Report - MERGE back raw values for readability
    report_cols = ['ticker', 'Alpha Score', 'signal_explanation', 'is_extended']
    report = df_latest[report_cols].copy()
    
    # Add raw metrics from df_raw (prices, RSI, etc.)
    raw_metrics = ['ticker', 'close', 'rsi_14', 'ma20', 'volatility_20', 'bb_position']
    report = report.merge(df_raw[raw_metrics], on='ticker', suffixes=('', '_raw'))
    
    # Add status and Tactical Filters
    report['Status'] = 'SCANNING'
    
    # BUY Signals with Mean Reversion Protection
    buy_mask = report['Alpha Score'] > MIN_PROBABILITY_BUY
    report.loc[buy_mask, 'Status'] = 'TOP BUY'
    
    # If it's a BUY but extended, downgrade it (Prevents buying too high)
    extended_mask = (report['Status'] == 'TOP BUY') & (report['is_extended'] == True)
    report.loc[extended_mask, 'Status'] = 'WATCH (Extended)'
    report.loc[extended_mask, 'signal_explanation'] += " + [OVERBOUGHT - AVOID CHASING]"

    # Tactical EXIT Signals (Exhaustion)
    exit_mask = report['rsi_14'] > 75
    report.loc[exit_mask, 'Status'] = 'EXIT / TAKE PROFIT'
    report.loc[exit_mask, 'signal_explanation'] += " + [RSI EXHAUSTION]"

    # Save CSV
    filename = f"report_{latest_date.strftime('%Y%m%d')}.csv"
    filepath = os.path.join(REPORTS_DIR, filename)
    report.to_csv(filepath, index=False)
    
    print(f"\n>>> DAILY REPORT GENERATED: {filepath}")
    print("\n--- TOP OPPORTUNITIES ---")
    print(report[report['Status'] != 'SCANNING'].head(10))
    
    return report

if __name__ == "__main__":
    generate_daily_signals()

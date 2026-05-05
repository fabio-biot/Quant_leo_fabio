import sqlite3
from config import DB_PATH


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # PRICES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        key TEXT PRIMARY KEY
    )
    """)

    # FEATURES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS features (
        key TEXT PRIMARY KEY,
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        ma20 REAL,
        ma50 REAL,
        bb_upper REAL,
        bb_lower REAL,
        bb_position REAL,
        ma20_diff REAL,
        momentum_5 REAL,
        momentum_20 REAL,
        momentum_60 REAL,
        momentum_120 REAL,
        return REAL,
        volatility REAL,
        rsi_14 REAL,
        future_return_5d REAL,
        future_return_10d REAL,
        future_return_20d REAL,
        future_return_60d REAL,
        future_return_120d REAL,
        future_return_252d REAL,
        close_lag_1 REAL,
        close_lag_2 REAL
    )
    """)

    # FUNDAMENTALS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fundamentals (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        market_cap REAL,
        pe_ratio REAL,
        PRIMARY KEY (ticker, date)
    )
    """)

    # MACRO
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS macro (
        date DATE NOT NULL PRIMARY KEY,
        inflation REAL,
        interest_rate REAL,
        vix REAL
    )
    """)

    # TARGETS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS targets (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        future_return_5d REAL,
        future_return_20d REAL,
        target_5d INTEGER,
        target_20d INTEGER,
        PRIMARY KEY (ticker, date)
    )
    """)
    
    # ASSETS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        ticker TEXT PRIMARY KEY,
        company_name TEXT,
        market_sector TEXT,
        industry TEXT,
        market_country TEXT,
        exchange TEXT,
        exchange_timezone TEXT,
        region TEXT,
        financial_currency TEXT,
        asset_type TEXT,
        recommendation_key TEXT,
        recommendation_mean REAL,
        current_price REAL,
        target_high_price REAL,
        target_low_price REAL,
        target_mean_price REAL,
        target_median_price REAL,
        market_cap INTEGER,
        pe_ratio REAL,
        audit_risk INTEGER,
        board_risk INTEGER,
        compensation_risk INTEGER,
        shareholder_rights_risk INTEGER,
        overall_risk INTEGER,
        sector_etf TEXT,
        industry_etf TEXT,
        market_etf TEXT,
        updated_at TEXT
    )
    """)

    # COMPONENT PRICES
    cursor.execute("CREATE TABLE IF NOT EXISTS sector_prices (ticker TEXT, date DATE, close REAL, volume REAL, key TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS industry_prices (ticker TEXT, date DATE, close REAL, volume REAL, key TEXT PRIMARY KEY)")
    cursor.execute("CREATE TABLE IF NOT EXISTS market_prices (ticker TEXT, date DATE, close REAL, volume REAL, key TEXT PRIMARY KEY)")

    conn.commit()
    conn.close()

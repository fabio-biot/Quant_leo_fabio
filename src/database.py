import sqlite3
from config import DB_PATH


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # =========================
    # 📊 PRICES (RAW DATA)
    # =========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (ticker, date)
    )
    """)

    # =========================
    # 🧠 FEATURES (ML DATA)
    # =========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS features (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        ma20 REAL,
        ma50 REAL,
        bb_upper REAL,
        bb_lower REAL,
        bb_position REAL,
        ma20_diff REAL,
        momentum_5 REAL,
        return REAL,
        volatility REAL,
        rsi_14 REAL,
        close_lag_1 REAL,
        close_lag_2 REAL,
        PRIMARY KEY (ticker, date)
    )
    """)

    # =========================
    # 🏢 FUNDAMENTALS (future use)
    # =========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fundamentals (
        ticker TEXT NOT NULL,
        date TEXT NOT NULL,
        market_cap REAL,
        pe_ratio REAL,
        PRIMARY KEY (ticker, date)
    )
    """)

    # =========================
    # 🌍 MACRO DATA
    # =========================
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS macro (
        date DATE PRIMARY KEY,
        inflation REAL,
        interest_rate REAL,
        vix REAL
    )
    """)

    # =========================
    # ⚡ INDEXES (perf important)
    # =========================
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_prices_symbol_date
    ON prices(ticker, date)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_features_symbol_date
    ON features(ticker, date)
    """)

    cursor.execute("""CREATE TABLE targets (
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        future_return_5d REAL,
        future_return_20d REAL,
        target_5d INTEGER,
        target_20d INTEGER,
        PRIMARY KEY (ticker, date)
        )""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS assets (
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

        updated_at TEXT
    )
    """)

    conn.commit()
    conn.close()

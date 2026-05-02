import yfinance as yf
import pandas as pd
from src.database import get_connection


def import_stock_data(ticker: str, start_date: str, end_date: str):
    df = pd.DataFrame(yf.download(ticker, start=start_date, end=end_date, auto_adjust=True))
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index(0)
    df["ticker"] = ticker
    df = df.rename(columns={'Date': 'date'})
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    return df


def compute_rsi(window: int, serie: pd.Series):
    delta = serie.diff()
    loss = -delta.clip(upper=0)
    gain = delta.clip(lower=0)

    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_prices(ticker: str, start_date: str, end_date: str):
    hist_data = import_stock_data(ticker, start_date, end_date)
    if isinstance(hist_data.columns, pd.MultiIndex):
        hist_data.columns = hist_data.columns.get_level_values(0)
    hist_data = hist_data.reset_index()
    hist_data.rename(columns={'Date': 'date'}, inplace=True)
    hist_data['MA20'] = hist_data['Close'].rolling(window=20).mean()
    hist_data['MA50'] = hist_data['Close'].rolling(window=50).mean()
    rolling_std = hist_data['Close'].rolling(20).std()
    hist_data['BB_upper'] = hist_data['MA20'] + 2 * rolling_std
    hist_data['BB_lower'] = hist_data['MA20'] - 2 * rolling_std
    hist_data['MA20_diff'] = hist_data['Close'] - hist_data['MA20']
    hist_data['BB_position'] = (hist_data['Close'] - hist_data['BB_lower']) / \
        (hist_data['BB_upper'] - hist_data['BB_lower'])
    hist_data['Momentum_5'] = hist_data['Close'] - hist_data['Close'].shift(5)
    hist_data['Return'] = hist_data['Close'].pct_change()
    hist_data['Volatility'] = hist_data['Return'].rolling(10).std()
    hist_data['RSI_14'] = compute_rsi(14, hist_data['Close'])
    hist_data['Close_lag_1'] = hist_data['Close'].shift(1)
    hist_data['Close_lag_2'] = hist_data['Close'].shift(2)
    hist_data = hist_data.dropna()
    return hist_data[[
        'ticker', 'date', 'MA20', 'MA50', 'BB_upper',
        'BB_lower', 'BB_position', 'MA20_diff', 'Momentum_5',
        'Return', 'Volatility', 'RSI_14', 'Close_lag_1', 'Close_lag_2'
    ]]


def save_prices(df: pd.DataFrame):
    conn = get_connection()
    df.to_sql("prices", conn, if_exists="append", index=False)
    conn.close()


def save_prices_features(df: pd.DataFrame):
    conn = get_connection()
    df.to_sql("features", conn, if_exists="append", index=False)
    conn.close()

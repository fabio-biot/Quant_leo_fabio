import yfinance as yf
import pandas as pd
from src.database import get_connection
from .prices import compute_rsi


def fetch_etf_features(type_etf: str):
    conn = get_connection()
    column_map = {
        "sector": "sector_etf",
        "industry": "industry_etf",
        "market": "market_index"
    }
    etf_column = column_map[type_etf]

    query = f"""
    SELECT DISTINCT {etf_column}
    FROM assets
    WHERE {etf_column} IS NOT NULL
    """

    tickers_df = pd.read_sql(query, conn)
    print(f"Found {len(tickers_df)} unique {type_etf} ETFs")
    rows = []
    for ticker in tickers_df[etf_column]:
        try:
            print(f"Processing {type_etf} ETF {ticker}")
            hist_data = yf.download(ticker, period="3y", interval="1d", auto_adjust=True)
            if hist_data.empty:
                continue
            if isinstance(hist_data.columns, pd.MultiIndex):
                hist_data.columns = hist_data.columns.get_level_values(0)
            hist_data = hist_data.reset_index()
            hist_data.rename(columns={'Date': 'date'}, inplace=True)
            hist_data['MA20'] = (hist_data['Close'].rolling(window=20).mean())
            hist_data['MA50'] = (hist_data['Close'].rolling(window=50).mean())
            rolling_std = (hist_data['Close'].rolling(20).std())
            hist_data['BB_upper'] = (hist_data['MA20'] + 2 * rolling_std)
            hist_data['BB_lower'] = (hist_data['MA20'] - 2 * rolling_std)
            hist_data['MA20_diff'] = (hist_data['Close'] - hist_data['MA20'])
            hist_data['BB_position'] = ((hist_data['Close'] - hist_data['BB_lower']) / \
                                        (hist_data['BB_upper'] - hist_data['BB_lower']))
            hist_data['Momentum_5'] = (hist_data['Close'] - hist_data['Close'].shift(5))
            hist_data['Return'] = (hist_data['Close'].pct_change())
            hist_data['Volatility'] = (hist_data['Return'].rolling(10).std())
            hist_data['RSI_14'] = compute_rsi(14, hist_data['Close'])
            hist_data['Close_lag_1'] = (hist_data['Close'].shift(1))
            hist_data['Close_lag_2'] = (hist_data['Close'].shift(2))
            hist_data['ticker'] = ticker
            hist_data = hist_data.dropna()
            hist_data = hist_data[[
                'ticker',
                'date',
                'MA20',
                'MA50',
                'BB_upper',
                'BB_lower',
                'BB_position',
                'MA20_diff',
                'Momentum_5',
                'Return',
                'Volatility',
                'RSI_14',
                'Close_lag_1',
                'Close_lag_2'
            ]]
            rows.append(hist_data)
        except Exception as e:
            print(f"{type_etf} Error {ticker}: {e}")
    conn.close()
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def save_etf_features(df: pd.DataFrame, type_etf: str):
    if df.empty:
        return
    conn = get_connection()
    df.to_sql(f"etf_features_{type_etf}", conn, if_exists="append", index=False)
    df.to_csv(f"tables_csv/etf_features_{type_etf}.csv", mode="a", header=False, index=False)
    conn.close()

# if __name__ == "__main__":
#     df = fetch_etf_features("sector")
#     save_prices(df, "sector")
#     print(df.head())
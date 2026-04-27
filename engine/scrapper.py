try:
    import matplotlib.pyplot as plt
    import pandas as pd
    import yfinance as yf
except ImportError as e:
    print(f"Error importing module: {e}")
    exit()


def import_stock_data(ticker: str, start_date: str, end_date: str):
    data = yf.download(ticker, start=start_date, end=end_date)
    data["ticker"] = ticker
    return data


def compute_rsi(window: int, serie: pd.Series):
    delta = serie.diff()
    loss = -delta.clip(upper=0)
    gain = delta.clip(lower=0)

    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def stock_data_analysis(ticker: str, start_date: str, end_date: str):
    hist_data = import_stock_data(ticker, start_date, end_date)
    if isinstance(hist_data.columns, pd.MultiIndex):
        hist_data.columns = hist_data.columns.get_level_values(0)
    hist_data = hist_data.reset_index()
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
    return hist_data


def list_tickers():
    list_tickers = ['AAPL']
    return list_tickers


def plot_stock_data(ticker: str, hist_data: pd.DataFrame):

    print(f"{ticker} data: {hist_data.head()}")
    print(f"{ticker} data: {hist_data.columns}")
    plt.plot(hist_data['Date'], hist_data['Close'], label=ticker, color='blue')
    plt.plot(hist_data['Date'], hist_data['MA20'], label=ticker, color='blue', linestyle='--')
    plt.plot(hist_data['Date'], hist_data['MA50'], label=ticker, color='blue', linestyle=':')
    plt.plot(hist_data['Date'], hist_data['BB_upper'], label=ticker, color='red', linestyle=':')
    plt.plot(hist_data['Date'], hist_data['BB_lower'], label=ticker, color='red', linestyle=':')
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.title('Stock Price Analysis')
    plt.legend()
    # plt.savefig("matrix_analysis.png")
    plt.show()


def main():
    print("\nUsing Pandas to analyse Stock data:")
    tickers = list_tickers()
    print("====" * 50)
    for ticker in tickers:
        hist_data = stock_data_analysis(ticker, "2020-01-01", "2021-01-01")
        # plot_stock_data(ticker, hist_data)
        print(f"{ticker} data: \n{hist_data.tail(10)}")
    print("====" * 50)
    print(hist_data.head(0))
    print("====" * 10 + "IMPORTATION CASH" + "====" * 10)
    importation = pd.DataFrame(import_stock_data(ticker, "2020-01-01", "2021-01-01"))
    if isinstance(importation.columns, pd.MultiIndex):
        importation.columns = importation.columns.get_level_values(0)
    importation = importation.reset_index(0)
    print(importation.columns)
    print(importation.head())
    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()

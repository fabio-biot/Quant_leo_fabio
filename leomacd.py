import time
import yfinance as yf
from yfinance.exceptions import YFRateLimitError


def get_history_with_retry(symbol, period="1y", max_retries=3):
    ticker = yf.Ticker(symbol)

    for attempt in range(max_retries):
        try:
            hist_data = ticker.history(period=period, auto_adjust=False)

            if hist_data.empty:
                raise ValueError("Aucune donnée récupérée.")

            return ticker, hist_data

        except YFRateLimitError:
            wait_time = 30 * (attempt + 1)
            print(f"Rate limit yfinance. Nouvelle tentative dans {wait_time}s...")
            time.sleep(wait_time)

    raise ValueError("Aucune donnée récupérée : yfinance est probablement rate limited.")


ticker, hist_data = get_history_with_retry("AAPL")

hist_data["EMA_12"] = hist_data["Close"].ewm(span=12, adjust=False).mean()
hist_data["EMA_26"] = hist_data["Close"].ewm(span=26, adjust=False).mean()

hist_data["MACD_Line"] = hist_data["EMA_12"] - hist_data["EMA_26"]
hist_data["Signal_Line"] = hist_data["MACD_Line"].ewm(span=9, adjust=False).mean()
hist_data["MACD_Histogram"] = hist_data["MACD_Line"] - hist_data["Signal_Line"]

macd = hist_data["MACD_Line"].iloc[-1]
signal = hist_data["Signal_Line"].iloc[-1]

print("MACD:", macd)
print("Signal:", signal)

def MACD_recommendation(macd, signal):
    if macd > signal:
        print("MACD Buy")
        return 1
    elif macd < signal:
        print("MACD Sell")
        return -1
    else:
        print("MACD Hold")
        return 0

recommendation_macd = MACD_recommendation(macd, signal)
print(f"MACD Recommendation for {ticker.ticker}: {recommendation_macd}")

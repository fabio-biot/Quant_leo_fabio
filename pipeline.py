from src.database import init_db
from engine.prices import fetch_prices, save_prices, save_prices_features, import_stock_data
from engine.fundamentals import fetch_fundamentals, save_fundamentals
from engine.macro import run_macro_pipeline


SYMBOLS = ["AAPL", "MSFT", "TSLA", "AMZN"]
end_date = "2024-01-01"
start_date = "2020-01-01"


def run_pipeline():
    init_db()

    for symbol in SYMBOLS:
        print(f"Processing {symbol}")

        try:
            raw_prices = import_stock_data(symbol, start_date, end_date)
            raw_prices.columns = [c.lower() for c in raw_prices.columns]
            save_prices(raw_prices)
        except Exception as e:
            print(f"Raw Price Error {symbol}: {e}")
        try:
            features = fetch_prices(symbol, start_date, end_date)
            save_prices_features(features)
        except Exception as e:
            print(f"Features Price Error {symbol}: {e}")
        try:
            fundamentals = fetch_fundamentals(symbol)
            save_fundamentals(fundamentals)
        except Exception as e:
            print(f"Fundamentals Error {symbol}: {e}")

    try:
        run_macro_pipeline()
    except Exception as e:
        print("Macro error:", e)


if __name__ == "__main__":
    run_pipeline()

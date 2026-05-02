from src.database import init_db
from engine.prices import fetch_prices, save_prices, save_prices_features, import_stock_data
from engine.fundamentals import fetch_fundamentals, save_fundamentals
from engine.macro import build_macro_dataset, save_macro
from engine.assets import fetch_assets_data, save_assets_data
from engine.target import fetch_and_compute_targets, save_targets
from engine.sector_price import fetch_sector_prices, save_sector_prices
from engine.industry_price import fetch_industry_prices, save_industry_prices
from engine.market_price import fetch_market_prices, save_market_prices
from engine.features_etf import fetch_etf_features, save_etf_features


# SYMBOLS = ["AAPL", "MSFT", "TSLA", "AMZN", "ROG","BNP.PA", "SHEL", "BP", "BHP", "RIO"]
SYMBOLS = ["AAPL", "MSFT"]
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
        except Exception as e: print(f"Raw Price Error: {e}")
        try:
            features = fetch_prices(symbol, start_date, end_date)
            save_prices_features(features)
        except Exception as e: print(f"Features Error: {e}")
        try:
            fundamentals = fetch_fundamentals(symbol)
            save_fundamentals(fundamentals)
        except Exception as e: print(f"Fundamentals Error: {e}")   
        try:
            assets = fetch_assets_data(symbol)
            save_assets_data(assets)
        except Exception as e: print(f"Assets Error: {e}")
        try:
            targets = fetch_and_compute_targets(symbol, start_date, end_date)
            save_targets(targets)
        except Exception as e: print(f"Targets Error: {e}")


    try:
        save_sector_prices(fetch_sector_prices())
        save_industry_prices(fetch_industry_prices())
        save_market_prices(fetch_market_prices())
    except Exception as e:
        print(f"Component prices error: {e}")
    try:
        sector_features = fetch_etf_features("sector")
        save_etf_features(sector_features, "sector")

        industry_features = fetch_etf_features("industry")
        save_etf_features(industry_features, "industry")
        
        market_features = fetch_etf_features("market")
        save_etf_features(market_features, "market")
    except Exception as e:
        print(f"ETF Features Error: {e}") 
    try:
        df = build_macro_dataset()
        save_macro(df)
    except Exception as e:
        print(f"Macro Data Error: {e}")


if __name__ == "__main__":
    run_pipeline()

from .ticker_vs_etf import build_relative_strength_signals
import pandas as pd
from src.database import get_connection


conn = get_connection()
features = pd.read_sql("SELECT * FROM features", conn)
features = features[["key", "ma20", "ma50", "bb_upper", "bb_lower",
             "bb_position", "ma20_diff", "momentum_5",
             "return", "volatility", "rsi_14",
             "future_return_5d", "future_return_10d",
             "future_return_20d", "future_return_60d",'future_return_120d', 'future_return_252d',
             "close_lag_1", "close_lag_2"]]
dataset_signals = build_relative_strength_signals()
dataset_signals.drop("ticker", axis=1, inplace=True)
dataset_signals.drop("date", axis=1, inplace=True)
dataset_signals.to_csv("dataset_signals.csv", mode="a", header=True, index=False)
features.to_csv("features.csv", mode="a", header=True, index=False)
df = features.merge(dataset_signals, on="key", how="left")
df = df.dropna()
df.to_csv("siu.csv", header=True, index=False)
a = df.groupby("rs_sector_signal")["future_return_5d"].mean()
aa = df.groupby("rs_sector_signal")["future_return_10d"].mean()
aaa = df.groupby("rs_sector_signal")["future_return_20d"].mean()
dd = df.groupby("rs_sector_signal")["future_return_60d"].mean()
ddd = df.groupby("rs_sector_signal")["future_return_120d"].mean()
d = df.groupby("rs_sector_signal")["future_return_252d"].mean()
print(a)
print(aa)
print(aaa)
print(d)
print(ddd)
print(dd)

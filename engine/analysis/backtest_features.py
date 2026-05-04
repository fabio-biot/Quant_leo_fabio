from .ticker_vs_etf import build_relative_strength_signals
import pandas as pd
from src.database import get_connection


conn = get_connection()
features = pd.read_sql("SELECT * FROM features", conn)
dataset_signals = build_relative_strength_signals()


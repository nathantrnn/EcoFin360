import pandas as pd
from src.utils.file_utils import load_csv, save_parquet

RAW_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"
RAW_PRICES_FILE = "data/bronze/stocks/SP500-adj-close.csv"
CLEANED_TICKERS_FILE = "data/silver/stocks/cleaned_tickers.parquet"
CLEANED_PRICES_FILE = "data/silver/stocks/cleaned_prices.parquet"


def clean_tickers():
    """Clean raw tickers data."""
    tickers = load_csv(RAW_TICKERS_FILE)
    tickers = tickers.dropna().drop_duplicates()
    save_parquet(tickers, CLEANED_TICKERS_FILE)


def clean_prices():
    """Clean raw historical prices data."""
    prices = load_csv(RAW_PRICES_FILE, index_col="Date", parse_dates=True)
    prices = prices.dropna(how="all").sort_index()
    save_parquet(prices, CLEANED_PRICES_FILE)


if __name__ == "__main__":
    clean_tickers()
    clean_prices()

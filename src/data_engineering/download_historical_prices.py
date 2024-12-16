import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from src.utils.file_utils import load_csv, save_csv

SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"
HISTORICAL_FILE = "data/bronze/stocks/SP500-adj-close.csv"


def fetch_new_data(tickers, start_date):
    """Fetch adjusted closing prices from Yahoo Finance."""
    today_date = datetime.today().strftime('%Y-%m-%d')
    data = yf.download(tickers, start=start_date, end=today_date)['Adj Close']
    return data


def update_historical_prices():
    """Update historical stock prices in the bronze layer."""
    # Load tickers
    tickers = load_csv(SP500_TICKERS_FILE)["Symbol"]
    if not tickers:
        raise RuntimeError("No tickers found. Please fetch tickers first.")

    # Load existing historical data
    historical_data = load_csv(HISTORICAL_FILE, index_col="Date")
    last_date = historical_data.index.max() if not historical_data.empty else "1995-01-01"

    # Fetch new data
    start_date = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
    if start_date <= datetime.today().strftime('%Y-%m-%d'):
        new_data = fetch_new_data(tickers, start_date)
        if not new_data.empty:
            combined_data = pd.concat([historical_data, new_data])
            combined_data = combined_data[~combined_data.index.duplicated(keep="last")]
            save_csv(combined_data, HISTORICAL_FILE)
            print(f"Updated historical data saved with {len(combined_data)} records.")
        else:
            print("No new data fetched.")
    else:
        print("Historical data is already up-to-date.")


if __name__ == "__main__":
    update_historical_prices()

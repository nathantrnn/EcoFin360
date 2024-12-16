import pandas as pd
import yfinance as yf  # For fetching stock data
from datetime import datetime
import os

SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"
DAILY_PRICE_DIR = "data/bronze/stocks/"

# Ensure the daily price directory exists
if not os.path.exists(DAILY_PRICE_DIR):
    os.makedirs(DAILY_PRICE_DIR)


def load_csv(file_path, index_col=None):
    """
    Load data from a CSV file.
    """
    if os.path.exists(file_path):
        return pd.read_csv(file_path, index_col=index_col)
    else:
        if index_col:
            return pd.DataFrame(columns=[index_col])
        return pd.DataFrame()


def save_csv(dataframe, file_path):
    """
    Save a DataFrame to a CSV file.
    """
    dataframe.to_csv(file_path, index=True)
    print(f"Data successfully saved to {file_path}")


def fetch_all_data(tickers, end_date):
    """
    Fetch adjusted closing prices for all tickers from Yahoo Finance.
    """
    try:
        print("Fetching adjusted closing prices for all tickers...")
        all_data = yf.download(tickers,start= '1995-01-01', end=end_date)['Adj Close']
        return all_data
    except Exception as e:
        print(f"Error fetching data from Yahoo Finance: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of failure


def save_daily_prices():
    """
    Save daily stock prices for all S&P 500 tickers to a separate CSV file.
    """
    # Determine today's file name
    today_date = datetime.today().strftime('%y%m%d')
    daily_file_path = os.path.join(DAILY_PRICE_DIR, f"{today_date}-SP500-adj-close.csv")
    if os.path.exists(daily_file_path):
        print(f"Data for {today_date} already exists. Skipping data fetch.")
        return

    # Load tickers
    tickers = load_csv(SP500_TICKERS_FILE)["Symbol"].tolist()  # Convert column to list
    if not tickers:
        raise RuntimeError("No tickers found. Please fetch tickers first.")

    # Fetch data for all tickers
    today_date_full = datetime.today().strftime('%Y-%m-%d')
    all_data = fetch_all_data(tickers, today_date_full)

    if all_data.empty:
        print("No data fetched for any tickers.")
        return

    # Save the fetched data to today's file
    save_csv(all_data, daily_file_path)
    print(f"Daily prices saved to {daily_file_path}")


if __name__ == "__main__":
    save_daily_prices()

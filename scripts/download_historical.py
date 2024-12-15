import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# File paths
SP500_TICKERS_FILE = 'data/bronze/stocks/SP500-tickers.csv'
HISTORICAL_FILE = 'data/bronze/stocks/historical/SP500-adj-close.csv'


def read_tickers(file_path):
    """Read and return a list of tickers from a CSV file."""
    try:
        return pd.read_csv(file_path)['Symbol'].dropna().tolist()
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except KeyError:
        print(f"'Symbol' column not found in {file_path}")
        return []


def load_existing_data(file_path):
    """Load historical data from the CSV file."""
    try:
        return pd.read_csv(file_path, index_col='Date', parse_dates=True)
    except FileNotFoundError:
        print(f"No historical file found at {file_path}. Starting fresh.")
        return pd.DataFrame()  # Return empty DataFrame if no file exists


def fetch_new_data(tickers, start_date):
    """
    Download new adjusted close data for given tickers starting from start_date.
    """
    print(f"Fetching new data starting from {start_date} to today...")
    today_date = datetime.today().strftime('%Y-%m-%d')
    data = yf.download(tickers, start=start_date, end=today_date)['Adj Close']
    return data


def save_updated_data(data, file_path):
    """Save the updated DataFrame back to the historical file."""
    data.to_csv(file_path)
    print(f"Updated historical data saved to {file_path}.")


if __name__ == "__main__":
    # Step 1: Read tickers from SP500-tickers.csv
    tickers = read_tickers(SP500_TICKERS_FILE)

    # Step 2: Load existing historical data
    existing_data = load_existing_data(HISTORICAL_FILE)

    # Step 3: Determine start date for fetching new data
    if not existing_data.empty:
        last_date = existing_data.index.max()
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # If no historical data exists, start from the given start date
        start_date = '1995-01-01'

    print(f"Last available data is up to: {existing_data.index.max() if not existing_data.empty else 'No data'}")
    print(f"Fetching new data starting from: {start_date}")

    # Step 4: Fetch new data from the determined start date
    if start_date <= datetime.today().strftime('%Y-%m-%d'):
        new_data = fetch_new_data(tickers, start_date)

        # Step 5: Combine new and existing data
        if not new_data.empty:
            combined_data = pd.concat([existing_data, new_data])
            combined_data = combined_data[~combined_data.index.duplicated(keep='last')]  # Remove duplicates

            # Step 6: Save the updated historical data
            save_updated_data(combined_data, HISTORICAL_FILE)
        else:
            print("No new data to add. Historical file is up-to-date.")
    else:
        print("No updates required. The data is already up-to-date.")

import os
import pandas as pd
import yfinance as yf

# Constants
TICKERS_FILE = 'data/bronze/stocks/SP500-tickers.csv'
HISTORICAL_DIR = 'data/bronze/stocks/historical'
START_DATE = "2014-01-01"


def load_tickers(file_path):
    """Load tickers from a CSV file."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Ticker file not found: {file_path}. Please ensure the file exists.")
    tickers_df = pd.read_csv(file_path)
    return tickers_df['Symbol'].tolist()


def create_directory(directory_path):
    """Ensure the directory exists."""
    os.makedirs(directory_path, exist_ok=True)


def download_historical_data(tickers, save_dir, start_date):
    """Download historical data for a list of tickers and save them."""
    errors = []

    # Batch download all tickers at once
    try:
        data = yf.download(tickers, start=start_date, group_by="ticker")

        for ticker in tickers:
            ticker_data = data.get(ticker)
            if ticker_data is None or ticker_data.empty:
                errors.append(f"No data for {ticker}.")
                continue

            # Save ticker data to CSV
            csv_file = os.path.join(save_dir, f"{ticker}.csv")
            ticker_data.to_csv(csv_file)
    except Exception as e:
        errors.append(f"Batch download error: {e}")

    # Log errors if any
    if errors:
        print("\n".join(errors))


def main():
    """Main function to execute the script."""
    # Load tickers
    tickers = load_tickers(TICKERS_FILE)

    # Ensure the directory exists
    create_directory(HISTORICAL_DIR)

    # Download historical data
    download_historical_data(tickers, HISTORICAL_DIR, START_DATE)


if __name__ == "__main__":
    main()

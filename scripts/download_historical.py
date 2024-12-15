import os
import pandas as pd
import yfinance as yf

# Constants
TICKERS_FILE = 'data/bronze/stocks/SP500-tickers.csv'
COMBINED_FILE = 'data/bronze/stocks/combined_historical.csv'
START_DATE = "2014-01-01"


def load_tickers(file_path):
    """Load tickers from a CSV file."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Ticker file not found: {file_path}. Please ensure the file exists.")
    tickers_df = pd.read_csv(file_path)
    return tickers_df['Symbol'].tolist()


def download_combined_historical_data(tickers, start_date, output_file):
    """Download historical data for all tickers and combine them into one CSV file."""
    try:
        # Download all ticker data at once
        data = yf.download(tickers, start=start_date, group_by="ticker")

        # Extract "Adj Close" column, and pivot with Tickers as rows and Dates as columns
        adj_close_data = data['Adj Close']  # Extract the `Adj Close` column
        combined_data = adj_close_data.T  # Transpose so tickers are rows

        # Save to CSV
        combined_data.to_csv(output_file)
        print(f"Combined historical file saved to {output_file}")
    except Exception as e:
        print(f"Error during download or save: {e}")


def main():
    """Main function to execute the script."""
    # Load tickers
    tickers = load_tickers(TICKERS_FILE)

    # Download and combine historical data
    download_combined_historical_data(tickers, START_DATE, COMBINED_FILE)


if __name__ == "__main__":
    main()

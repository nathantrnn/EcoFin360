import os
import pandas as pd
import yfinance as yf
from datetime import datetime

# Define paths
BRONZE_FOLDER = "data/bronze/stocks/"
TICKERS_FILE = os.path.join(BRONZE_FOLDER, "SP500-tickers.csv")
OUTPUT_FILE = os.path.join(BRONZE_FOLDER, "final_price.csv")

# Ensure the folder exists
os.makedirs(BRONZE_FOLDER, exist_ok=True)


def download_consolidated_close_prices():
    # Load the tickers list
    try:
        tickers_df = pd.read_csv(TICKERS_FILE)
        tickers = tickers_df["Symbol"].tolist()
    except Exception as e:
        print(f"Error loading tickers file: {e}")
        return

    # Define the time range (last 10 years)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - pd.DateOffset(years=10)).strftime("%Y-%m-%d")

    # Create a DataFrame to store close price data
    close_prices_df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))

    # Loop through the tickers and retrieve close price data
    for ticker in tickers:
        try:
            print(f"Fetching data for {ticker}...")
            # Retrieve historical data
            ticker_data = yf.download(ticker, start=start_date, end=end_date)

            if not ticker_data.empty:
                # Extract the 'Close' column and rename it with the ticker symbol
                ticker_close = ticker_data['Close'].rename(ticker)
                # Merge the close prices into the consolidated DataFrame
                close_prices_df = pd.merge(close_prices_df, ticker_close, how='left', left_index=True, right_index=True)
                print(f"Added {ticker} to the DataFrame.")
            else:
                print(f"No data found for {ticker}.")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    # Save the consolidated DataFrame to a CSV file
    close_prices_df.to_csv(OUTPUT_FILE)
    print(f"Saved consolidated close prices to {OUTPUT_FILE}")


if __name__ == "__main__":
    download_consolidated_close_prices()

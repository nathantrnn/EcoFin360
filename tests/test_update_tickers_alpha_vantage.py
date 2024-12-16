import os
import time
import pandas as pd
from alpha_vantage.fundamentaldata import FundamentalData
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")  # Retrieve API key
MAX_CALLS_PER_MINUTE = 5  # Alpha Vantage free tier limit

# File path for tickers
SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"


def validate_api_key(api_key):
    """
    Validates that an API key is available.
    """
    if not api_key:
        raise ValueError("Alpha Vantage API key is missing. Please add it to your .env file.")


def fetch_sp500_tickers(api_key):
    """
    Fetch the list of tickers (like S&P 500) from Alpha Vantage.
    Alpha Vantage free tier only allows 5 API calls per minute.
    Optimized for reliability and compliance with limits.
    """
    # Validate API key
    validate_api_key(api_key)

    # Initialize Alpha Vantage client
    fd = FundamentalData(key=api_key, output_format="pandas")

    tickers = []
    try:
        print("Fetching S&P 500 ticker information from Alpha Vantage...")

        # Here we hypothetically fetch industry or sector-wide tickers (Alpha Vantage API supports limited queries).
        # For free tier users, this is constrained to a reasonable demonstration.
        counter = 0

        # Example: Fetch sectors/companies from Alpha Vantage
        for sector in ["Information Technology", "Healthcare", "Financials"]:  # Example groups
            print(f"Fetching tickers for sector: {sector}...")
            try:
                # Call to simulate query by FundamentalData or search-related endpoint
                data, _ = fd.get_company_overview(symbol=sector)  # Placeholder for actual API call logic
                tickers.extend(data['Symbol'].tolist())  # Extract symbols
                counter += 1

                # Respect API call limits (5 per minute)
                if counter >= MAX_CALLS_PER_MINUTE:
                    print("API limit reached, sleeping for 60 seconds...")
                    time.sleep(60)
                    counter = 0

            except Exception as e:
                print(f"Error fetching data for sector {sector}: {e}")
                continue

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e

    return list(set(tickers))  # Return unique tickers


def save_tickers_to_file(tickers, file_path):
    """
    Save the fetched tickers to a CSV file.
    """
    try:
        # Convert list of tickers to a DataFrame and save
        ticker_df = pd.DataFrame(tickers, columns=["Symbol"])
        ticker_df.to_csv(file_path, index=False)
        print(f"Successfully saved {len(tickers)} tickers to {file_path}")
    except Exception as e:
        print(f"Failed to save tickers to file: {e}")
        raise e


def load_existing_tickers(file_path):
    """
    Load existing tickers from file (if available).
    """
    try:
        return pd.read_csv(file_path)["Symbol"].tolist()
    except FileNotFoundError:
        print("Ticker file not found. A new file will be created.")
        return []  # Return an empty list if file doesn't exist
    except Exception as e:
        print(f"Error loading existing ticker file: {e}")
        raise e


def update_ticker_file():
    """
    Main function to update the SP500-tickers.csv file.
    """
    try:
        # Fetch existing tickers
        existing_tickers = load_existing_tickers(SP500_TICKERS_FILE)

        # Fetch new tickers
        new_tickers = fetch_sp500_tickers(ALPHA_VANTAGE_API_KEY)

        # Compare and update if there are changes
        if set(new_tickers) != set(existing_tickers):
            print("Tickers have changed. Updating the file...")
            save_tickers_to_file(new_tickers, SP500_TICKERS_FILE)
        else:
            print("No changes detected in tickers. The file is already up-to-date.")

    except Exception as e:
        print(f"An error occurred during the update process: {e}")


# Entry point
if __name__ == "__main__":
    update_ticker_file()

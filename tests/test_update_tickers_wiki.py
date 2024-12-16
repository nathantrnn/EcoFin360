import pandas as pd
import requests
from bs4 import BeautifulSoup

# File path for S&P 500 tickers
SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"


def fetch_sp500_tickers_from_wikipedia():
    """
    Fetch the current list of S&P 500 tickers from Wikipedia.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Ensure no HTTP error

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", id="constituents")
        if not table:
            raise ValueError("S&P 500 table not found on Wikipedia page.")

        # Extract tickers using list comprehension
        tickers = [
            row.find_all("td")[0].text.strip()
            for row in table.find_all("tr")[1:]
        ]
        return tickers
    except Exception as e:
        raise RuntimeError(f"Error fetching tickers from Wikipedia: {e}") from e


def save_tickers_to_file(tickers, file_path):
    """
    Save the tickers to a CSV file.
    """
    try:
        pd.DataFrame(tickers, columns=["Symbol"]).to_csv(file_path, index=False)
        print(f"Successfully saved {len(tickers)} tickers to {file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to save tickers to file: {e}") from e


def load_existing_tickers(file_path):
    """
    Load existing tickers from the file, or return an empty list if the file doesn't exist.
    """
    try:
        return pd.read_csv(file_path)["Symbol"].tolist()
    except FileNotFoundError:
        print(f"{file_path} not found. A new file will be created.")
        return []
    except Exception as e:
        raise RuntimeError(f"Error loading tickers from file: {e}") from e


def update_ticker_file():
    """
    Main function to update the SP500 tickers CSV file.
    """
    # Fetch existing and new tickers
    existing_tickers = load_existing_tickers(SP500_TICKERS_FILE)
    new_tickers = fetch_sp500_tickers_from_wikipedia()

    # Ensure '^GSPC' is included in the list
    if "^GSPC" not in new_tickers:
        new_tickers.append("^GSPC")

    # Update the file only if tickers have changed
    if set(new_tickers) != set(existing_tickers):
        print("Tickers have changed. Updating the file...")
        save_tickers_to_file(new_tickers, SP500_TICKERS_FILE)
    else:
        print("No changes detected in tickers. File is up-to-date.")


# Entry point
if __name__ == "__main__":
    try:
        update_ticker_file()
    except Exception as error:
        print(f"An error occurred: {error}")

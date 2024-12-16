import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.utils.file_utils import save_csv, load_csv

# File path for S&P 500 tickers
SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"


def fetch_sp500_tickers_from_wikipedia():
    """
    Fetch the current list of S&P 500 tickers from Wikipedia.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", id="constituents")
        if not table:
            raise ValueError("S&P 500 table not found on Wikipedia page.")

        tickers = [
            row.find_all("td")[0].text.strip()
            for row in table.find_all("tr")[1:]
        ]
        return tickers
    except Exception as e:
        raise RuntimeError(f"Error fetching tickers from Wikipedia: {e}")


def update_sp500_tickers():
    """
    Update the stored S&P 500 tickers in the bronze layer.
    """
    existing_tickers = load_csv(SP500_TICKERS_FILE).get("Symbol", [])
    new_tickers = fetch_sp500_tickers_from_wikipedia()

    # Include ^GSPC (Index itself)
    if "^GSPC" not in new_tickers:
        new_tickers.append("^GSPC")

    # Only update if there are changes
    if set(new_tickers) != set(existing_tickers):
        save_csv({"Symbol": new_tickers}, SP500_TICKERS_FILE)
        print(f"Updated {len(new_tickers)} tickers.")
    else:
        print("No changes detected in the tickers.")


# For command-line usage
if __name__ == "__main__":
    update_sp500_tickers()

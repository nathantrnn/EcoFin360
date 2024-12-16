import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# File path for S&P 500 tickers
SP500_TICKERS_FILE = "data/bronze/stocks/SP500-tickers.csv"


def save_csv(data, file_path, index=False):
    """
    Save data (dictionary or DataFrame) to a CSV file.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if isinstance(data, dict):
        pd.DataFrame(data).to_csv(file_path, index=index)
    elif isinstance(data, pd.DataFrame):
        data.to_csv(file_path, index=index)
    else:
        raise ValueError("Data must be a dictionary or pandas DataFrame.")


def load_csv(file_path, index_col=None):
    """
    Load a CSV file and return its content as a pandas DataFrame.
    """
    if os.path.exists(file_path):
        return pd.read_csv(file_path, index_col=index_col)
    else:
        if index_col:
            return pd.DataFrame(columns=[index_col])
        return pd.DataFrame()


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

        # Replace `.` with `-` in tickers
        tickers = [
            row.find_all("td")[0].text.strip().replace(".", "-")
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
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

# Path to save the updated ticker list
BRONZE_FOLDER = "data/bronze/stocks/"
TICKERS_FILE = os.path.join(BRONZE_FOLDER, "SP500-tickers.csv")


def update_sp500_tickers():
    # Wikipedia URL for the S&P 500 companies list
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Fetch the webpage content
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    # Locate the table with the list of S&P 500 companies
    table = soup.find("table", {"class": "wikitable"})
    df = pd.read_html(str(table))[0]

    # Extract and clean the tickers
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)  # Replace . with - for Yahoo Finance compatibility
    tickers = df[['Symbol', 'Security']]

    # Ensure output folder exists
    os.makedirs(BRONZE_FOLDER, exist_ok=True)

    # Save the updated tickers to CSV
    tickers.to_csv(TICKERS_FILE, index=False)
    print(f"Updated tickers saved to {TICKERS_FILE}")


if __name__ == "__main__":
    update_sp500_tickers()

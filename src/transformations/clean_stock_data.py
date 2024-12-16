import os
import glob
import logging

import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths
BRONZE_LAYER_DIR = "data/bronze/stocks/"
SILVER_LAYER_DIR = "data/silver/stocks/"
os.makedirs(SILVER_LAYER_DIR, exist_ok=True)


def get_latest_file(directory: str, file_pattern: str) -> str:
    """
    Get the latest file in a directory based on a specified pattern.

    Args:
        directory (str): Path to the directory containing files.
        file_pattern (str): Pattern to match files (e.g., '*-SP500-adj-close.csv').

    Returns:
        str: Path to the latest file based on modification time.

    Raises:
        FileNotFoundError: If no matching files are found in the directory.
    """
    files = glob.glob(os.path.join(directory, file_pattern))
    if not files:
        raise FileNotFoundError(f"No files matching pattern '{file_pattern}' found in '{directory}'.")
    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Latest file identified: {latest_file}")
    return latest_file


def clean_stock_data(file_path: str) -> pd.DataFrame:
    """
    Cleans a raw stock data file where rows are dates and columns are tickers.

    Args:
        file_path (str): Path to the raw stock data file.

    Returns:
        pd.DataFrame: Cleaned stock data.

    Raises:
        Exception: If the data processing fails.
    """
    logger.info(f"Loading raw stock data from: {file_path}")
    try:
        # Load raw stock data
        data = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Remove duplicate rows
        data = data[~data.index.duplicated(keep="first")]

        # Ensure the date index is sorted
        if not data.index.is_monotonic_increasing:
            data = data.sort_index()
            logger.info("Date index sorted.")

        # Align to business-day frequency
        data = data.asfreq("B")

        # Drop columns with all NaN values
        data = data.dropna(axis=1, how="all")

        # Round adjusted close prices to 4 decimal places
        data = data.round(4)

        logger.info("Stock data cleaned successfully.")
        return data
    except Exception as e:
        logger.error(f"Error cleaning stock data: {e}")
        raise


def save_data(data: pd.DataFrame, csv_path: str, parquet_path: str):
    """
    Save cleaned stock data to both CSV and Parquet formats.

    Args:
        data (pd.DataFrame): Cleaned stock data.
        csv_path (str): Path to save the data as a CSV file.
        parquet_path (str): Path to save the data as a Parquet file.
    """
    try:
        # Save as CSV
        logger.info(f"Saving data to CSV: {csv_path}")
        data.to_csv(csv_path, index=True)

        # Save as a single Parquet file
        logger.info(f"Saving data to Parquet: {parquet_path}")
        data.to_parquet(parquet_path, index=True, engine="pyarrow")

        logger.info("Data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise


def process_latest_stock_data():
    """
    Identifies the latest combined stock prices file, cleans it,
    and saves it both as a single .parquet file and a .csv file.
    """
    try:
        # Find the latest combined stock data file
        latest_file_path = get_latest_file(BRONZE_LAYER_DIR, "*-SP500-adj-close.csv")

        # Generate output paths
        latest_file_name = os.path.basename(latest_file_path)
        csv_output_path = os.path.join(SILVER_LAYER_DIR, f"cleaned_{latest_file_name}")
        parquet_output_path = os.path.join(SILVER_LAYER_DIR, f"cleaned_{latest_file_name.replace('.csv', '.parquet')}")

        # Clean the data
        cleaned_data = clean_stock_data(latest_file_path)

        # Save the cleaned data
        save_data(cleaned_data, csv_output_path, parquet_output_path)

        # Display a sample of the cleaned data
        logger.info("Sample of the cleaned stock data:")
        logger.info(f"\n{cleaned_data.head()}")

    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError: {e}")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    process_latest_stock_data()

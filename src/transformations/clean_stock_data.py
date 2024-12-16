import os
import pandas as pd
import glob
from deltalake.writer import write_deltalake
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paths
BRONZE_LAYER_DIR = "data/bronze/stocks/"
SILVER_LAYER_DIR = "data/silver/stocks/"


def get_latest_combined_file(directory: str) -> str:
    """
    Get the latest combined file in the directory based on the filename format 'YYMMDD-SP500-adj-close.csv'.

    Args:
        directory (str): Directory of raw stock data in the Bronze Layer.

    Returns:
        str: Full path to the latest combined file.
    """
    combined_files = glob.glob(os.path.join(directory, "*-SP500-adj-close.csv"))
    if not combined_files:
        raise FileNotFoundError("No combined stock files found in the directory.")

    # Identify the latest file based on modification time
    latest_file = max(combined_files, key=os.path.getmtime)
    logger.info(f"Latest combined file identified: {latest_file}")
    return latest_file


def clean_combined_stock_data(file_path: str, output_file: str) -> pd.DataFrame:
    """
    Cleans the combined stock data file where rows are dates and columns are tickers.

    Args:
        file_path (str): Full path to the raw stock data file.
        output_file (str): Path to save the cleaned stock data.

    Returns:
        pd.DataFrame: Cleaned stock data.
    """
    try:
        logger.info(f"Loading raw combined stock data from: {file_path}")
        # Load raw stock data
        data = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Step 1: Remove duplicate rows
        data = data[~data.index.duplicated(keep="first")]

        # Step 2: Ensure the date index is sorted
        if not data.index.is_monotonic_increasing:
            data = data.sort_index()
            logger.info("Date index sorted.")

        # Step 3: Align to business-day frequency
        data = data.asfreq("B")  # Align to business days while retaining NaN for gaps

        # Step 4: Drop columns with all NaN
        data = data.dropna(axis=1, how="all")

        # Step 5: Round adjusted close prices to 4 decimal places
        data = data.round(4)

        # Step 6: Save the cleaned data
        data.to_csv(output_file)
        logger.info(f"Cleaned data saved to: {output_file}")

        # Step 7: Print a sample of the saved data
        print("Sample of the cleaned stock data:")
        print(data.head())  # Print the first 5 rows as a sample

        return data

    except Exception as e:
        logger.error(f"Failed to clean the stock data: {e}")
        raise


def save_cleaned_data_to_deltalake(data: pd.DataFrame, output_path: str):
    """
    Save cleaned data to Delta Lake in Parquet format.

    Args:
        data (pd.DataFrame): The cleaned stock data.
        output_path (str): Destination path for the Parquet file.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save the data to Delta Lake in Parquet format
        write_deltalake(table_or_uri=output_path, data=data, mode="overwrite")
        logger.info(f"Cleaned data saved to Delta Lake at {output_path}.")
    except Exception as e:
        logger.error(f"Error saving cleaned data to Delta Lake: {e}")
        raise

def clean_and_save_latest_combined_data():
    """
    Automatically identifies the latest combined stock prices file in the Bronze Layer,
    cleans it using predefined logic, and saves it both as a CSV file and as a Delta Lake table.
    """
    try:
        # Identify the latest combined stock data file
        latest_file_path = get_latest_combined_file(BRONZE_LAYER_DIR)

        # Generate the paths
        latest_file_name = os.path.basename(latest_file_path)
        csv_output_file = os.path.join(SILVER_LAYER_DIR, f"cleaned_{latest_file_name}")  # Save as a CSV file
        delta_table_path = os.path.join(SILVER_LAYER_DIR,
                                        f"cleaned_{latest_file_name.replace('.csv', '')}")  # Delta Lake path

        # Clean the combined stock data and save as a CSV file
        cleaned_data = clean_combined_stock_data(latest_file_path, csv_output_file)

        # Save cleaned data to Delta Lake directory
        save_cleaned_data_to_deltalake(cleaned_data, delta_table_path)

    except FileNotFoundError as fnfe:
        logger.error(f"FileNotFoundError: {fnfe}")
    except Exception as e:
        logger.error(f"Failed to clean and save combined stock data: {e}")


if __name__ == "__main__":
    clean_and_save_latest_combined_data()

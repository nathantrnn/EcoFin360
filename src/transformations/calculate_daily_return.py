import os
import glob
import pandas as pd
from deltalake.writer import write_deltalake
import logging
import re  # For extracting dates from filenames

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Directories for input and output
CLEANED_DATA_DIR = "data/silver/stocks/"  # Directory where cleaned data is stored
DAILY_RETURN_DIR = "data/silver/returns/"  # Directory where daily return data will be stored

# Ensure the daily returns directory exists
os.makedirs(DAILY_RETURN_DIR, exist_ok=True)


def get_latest_cleaned_file(directory: str) -> str:
    """
    Get the latest cleaned file from the directory based on the filename format.

    Args:
        directory (str): Directory where the cleaned files are stored.

    Returns:
        str: Full path to the latest cleaned file.

    Raises:
        FileNotFoundError: If no cleaned files are found in the directory.
    """
    cleaned_files = glob.glob(
        os.path.join(directory, "cleaned_*SP500-adj-close"))  # Only look for Parquet files
    if not cleaned_files:
        raise FileNotFoundError("No cleaned files found in the specified directory.")

    # Use modification time to find the latest file
    latest_file = max(cleaned_files, key=os.path.getmtime)
    logger.info(f"Latest cleaned file identified: {latest_file}")
    return latest_file


def extract_date_from_filename(filename: str) -> str:
    """
    Extracts the date in YYMMDD format from the cleaned file's name.

    Args:
        filename (str): Name of the cleaned file (e.g., 'cleaned_231015-SP500-adj-close.parquet').

    Returns:
        str: The extracted date in YYMMDD format.

    Raises:
        ValueError: If no date is found in the filename.
    """
    match = re.search(r"cleaned_(\d{6})-", filename)
    if not match:
        raise ValueError(f"Filename does not contain a valid date: {filename}")
    return match.group(1)  # Return the date part (YYMMDD)

"""


def calculate_and_save_daily_returns(file_path: str, output_dir: str) -> None:
    try:
        # Loading cleaned data
        logger.info(f"Loading cleaned data from: {file_path}")
        stock_data = pd.read_parquet(file_path)

        # Ensure only numeric data is used for daily returns calculation
        numeric_data = stock_data.select_dtypes(include=["number"])

        # Calculate daily returns
        daily_returns = numeric_data.pct_change(fill_method=None)

        # Round the daily returns to 3 decimal places
        daily_returns = daily_returns.round(3)

        # Restore original index (if necessary)
        daily_returns.index = stock_data.index

        # Extract the date for the output filename
        filename = os.path.basename(file_path)
        date_part = extract_date_from_filename(filename)

        # Generate file names
        csv_output_file_name = f"returns_cleaned_{date_part}-SP500-adj-close.csv"
        csv_output_file_path = os.path.join(output_dir, csv_output_file_name)

        delta_table_dir_name = f"returns_cleaned_{date_part}-SP500-adj-close"
        delta_table_dir_path = os.path.join(output_dir, delta_table_dir_name)

        # Save daily returns to a CSV file
        logger.info(f"Saving daily returns to CSV: {csv_output_file_path}")
        daily_returns.to_csv(csv_output_file_path)

        # Save daily returns to Delta Lake directory
        logger.info(f"Saving daily returns to Delta Lake: {delta_table_dir_path}")
        write_deltalake(table_or_uri=delta_table_dir_path, data=daily_returns, mode="overwrite")

        # Display the first 5 rows of the saved daily returns
        logger.info(f"Loading and showing a sample of the saved file: {csv_output_file_path}")
        daily_returns_sample = daily_returns.head()
        print("Sample of the saved daily returns:")
        print(daily_returns_sample)

    except Exception as e:
        logger.error(f"An error occurred while processing {file_path}: {e}")
        raise
"""

def calculate_and_save_daily_returns(delta_table_path: str, output_dir: str) -> None:
    try:
        # Read the Delta Lake directory as a Parquet file
        logger.info(f"Loading cleaned data from Delta Lake folder: {delta_table_path}")
        stock_data = pd.read_parquet(delta_table_path)  # Use pandas to read the Parquet data

        # Ensure only numeric data is used for daily returns calculation
        numeric_data = stock_data.select_dtypes(include=["number"])

        # Calculate daily returns
        daily_returns = numeric_data.pct_change(fill_method=None)

        # Round the daily returns to 3 decimal places
        daily_returns = daily_returns.round(3)

        # Restore original index (if necessary)
        daily_returns.index = stock_data.index

        # Extract the date for the output filename
        delta_dir_name = os.path.basename(delta_table_path)
        date_part = extract_date_from_filename(delta_dir_name)

        # Generate file names for the outputs
        csv_output_file_name = f"returns_cleaned_{date_part}-SP500-adj-close.csv"
        csv_output_file_path = os.path.join(output_dir, csv_output_file_name)

        delta_output_dir_name = f"returns_cleaned_{date_part}-SP500-adj-close"
        delta_output_dir_path = os.path.join(output_dir, delta_output_dir_name)

        # Save daily returns to a CSV file
        logger.info(f"Saving daily returns to CSV: {csv_output_file_path}")
        daily_returns.to_csv(csv_output_file_path)

        # Save daily returns to Delta Lake directory
        logger.info(f"Saving daily returns to Delta Lake: {delta_output_dir_path}")
        write_deltalake(table_or_uri=delta_output_dir_path, data=daily_returns, mode="overwrite")

        # Display the first 5 rows of the saved daily returns
        logger.info(f"Sample of the saved daily returns into: {csv_output_file_path}")
        daily_returns_sample = daily_returns.head()
        print("Sample of the saved daily returns:")
        print(daily_returns_sample)

    except Exception as e:
        logger.error(f"An error occurred while processing the Delta Lake folder {delta_table_path}: {e}")
        raise


def main():
    """
    Main function to calculate daily returns for the latest cleaned file and save it as Parquet in Delta Lake format.
    """
    try:
        # Find the latest cleaned stock data file
        latest_file_path = get_latest_cleaned_file(CLEANED_DATA_DIR)

        # Calculate and save daily returns for the latest cleaned file
        calculate_and_save_daily_returns(latest_file_path, DAILY_RETURN_DIR)

    except FileNotFoundError as fnf_error:
        logger.error(f"FileNotFoundError: {fnf_error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()

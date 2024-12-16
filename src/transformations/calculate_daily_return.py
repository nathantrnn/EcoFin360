import os
import glob
import pandas as pd
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Directories for input and output
CLEANED_DATA_DIR = "data/silver/stocks/"  # Directory for cleaned data
DAILY_RETURN_DIR = "data/silver/returns/"  # Output directory for daily returns
os.makedirs(DAILY_RETURN_DIR, exist_ok=True)


def get_latest_file(directory: str, pattern: str) -> str:
    """
    Get the latest file in the directory matching the given filename pattern.

    Args:
        directory (str): Path to the directory containing the files.
        pattern (str): Glob pattern for matching filenames (e.g., 'cleaned_*SP500-adj-close').

    Returns:
        str: Path to the latest file based on modification time.

    Raises:
        FileNotFoundError: If no files matching the pattern are found.
    """
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        raise FileNotFoundError(f"No files matching pattern '{pattern}' found in directory '{directory}'.")
    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Latest file identified: {latest_file}")
    return latest_file


def extract_date(filename: str, regex: str) -> str:
    """
    Extract a substring (e.g., date) from a filename based on a given regex pattern.

    Args:
        filename (str): The filename to extract the substring from.
        regex (str): The regular expression to match and extract the desired substring.

    Returns:
        str: The matched substring.

    Raises:
        ValueError: If no match is found in the filename.
    """
    match = re.search(regex, filename)
    if not match:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern: '{regex}'")
    return match.group(1)


def calculate_daily_returns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily percentage returns for stock data.

    Args:
        data (pd.DataFrame): DataFrame containing stock prices with a 'Date' column.

    Returns:
        pd.DataFrame: DataFrame with daily percentage returns, including the 'Date' column.
    """
    if 'Date' not in data.columns and data.index.name != 'Date':
        raise ValueError("The dataset must include a 'Date' column or index.")

    # Set 'Date' as a column if it is the index
    if data.index.name == 'Date':
        data = data.reset_index()

    # Ensure the 'Date' column is present
    date_column = data['Date']

    # Select only numeric columns for return calculations
    numeric_data = data.select_dtypes(include=["number"])
    if numeric_data.empty:
        raise ValueError("No numeric data found to calculate returns.")

    # Calculate daily returns and round to 3 decimal places
    daily_returns = numeric_data.pct_change(fill_method=None).round(3)

    # Insert the 'Date' column back into the results
    daily_returns.insert(0, 'Date', date_column)
    return daily_returns


def save_daily_returns(daily_returns: pd.DataFrame, date_part: str, output_dir: str):
    """
    Save the daily returns DataFrame as both a CSV file and a single Parquet file.

    Args:
        daily_returns (pd.DataFrame): DataFrame containing daily returns.
        date_part (str): Date string for naming the output files.
        output_dir (str): Base directory for saving files.
    """
    # Define file paths
    csv_file = os.path.join(output_dir, f"returns_cleaned_{date_part}-SP500-adj-close.csv")
    parquet_file = os.path.join(output_dir, f"returns_cleaned_{date_part}-SP500-adj-close.parquet")

    # Save as CSV
    logger.info(f"Saving daily returns to CSV: {csv_file}")
    daily_returns.to_csv(csv_file, index=False)

    # Save as a single Parquet file
    logger.info(f"Saving daily returns to Parquet: {parquet_file}")
    daily_returns.to_parquet(parquet_file, index=False, engine="pyarrow")


def main():
    """
    Main function to calculate and save daily returns for the latest cleaned stock data file.
    """
    try:
        # Get the latest cleaned file
        latest_file = get_latest_file(CLEANED_DATA_DIR, "cleaned_*SP500-adj-close.parquet")

        # Extract the date from the filename
        date_part = extract_date(
            os.path.basename(latest_file), r"cleaned_(\d{6})-"
        )

        # Load the cleaned data
        logger.info(f"Loading data from: {latest_file}")
        stock_data = pd.read_parquet(latest_file)

        # Calculate daily returns
        logger.info("Calculating daily returns...")
        daily_returns = calculate_daily_returns(stock_data)

        # Save the daily returns
        save_daily_returns(daily_returns, date_part, DAILY_RETURN_DIR)

        # Display a sample of the results
        logger.info("Sample of the calculated daily returns:")
        logger.info(f"\n{daily_returns.head()}")

    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logger.error(f"ValueError: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()

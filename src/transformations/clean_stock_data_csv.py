import pandas as pd
import os
import glob

# Define input and output directories
RAW_DATA_DIR = "data/bronze/stocks/"
CLEANED_DATA_DIR = "data/silver/stocks/"

# Ensure the output directory exists
if not os.path.exists(CLEANED_DATA_DIR):
    os.makedirs(CLEANED_DATA_DIR)


def get_latest_combined_file(directory):
    """
    Get the latest combined file in the directory based on the filename format 'YYMMDD-SP500-adj-close.csv'.
    """
    combined_files = glob.glob(os.path.join(directory, "*-SP500-adj-close.csv"))
    if not combined_files:
        raise FileNotFoundError("No combined files found in the specified directory.")

    # Sort files by creation/modification time and return the latest file
    latest_file = max(combined_files, key=os.path.getmtime)
    return latest_file

def clean_combined_stock_data(file_path):
    """
    Cleans the combined stock data file where rows are dates and columns are tickers.

    Steps:
    1. Retain missing data as NaN.
    2. Remove duplicate rows for the same date.
    3. Ensure date index is properly set and sorted.
    4. Align to business-day frequency while keeping NaN for gaps.
    5. Drop columns (tickers) with all NaN values.
removed
    """
    try:
        print(f"Loading raw data from: {file_path}")
        # Load the combined data
        data = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Step 1: Retain missing data as NaN (handled by default)

        # Step 2: Remove duplicate rows for the same date
        data = data[~data.index.duplicated(keep='first')]

        # Step 3: Ensure the date index is sorted
        if not data.index.is_monotonic_increasing:
            data = data.sort_index()

        # Step 4: Align to business-day frequency
        data = data.asfreq('B')  # Align to business days

        # Step 5: Drop columns with all NaN
        data = data.dropna(axis=1, how='all')


        print("Data cleaned successfully.")
        return data

    except Exception as e:
        print(f"Error occurred while cleaning data: {e}")
        return pd.DataFrame()  # Return empty DataFrame in case of failure


def clean_and_save_latest_combined_data():
    """
    Automatically identifies the latest combined stock prices file in the Bronze directory,
    cleans it, and saves it to the Silver directory with the modified naming format (e.g., cleaned_<original_name>.csv).
    """
    try:
        # Identify the latest file
        latest_file_path = get_latest_combined_file(RAW_DATA_DIR)

        # Extract the file name from the path
        latest_file_name = os.path.basename(latest_file_path)

        # Clean the data
        cleaned_data = clean_combined_stock_data(latest_file_path)

        # Save the cleaned data with 'cleaned_' prefix in the filename
        if not cleaned_data.empty:
            # Add 'cleaned_' prefix to the filename
            cleaned_file_name = f"cleaned_{latest_file_name}"
            cleaned_file_path = os.path.join(CLEANED_DATA_DIR, cleaned_file_name)

            # Save the cleaned stock prices
            cleaned_data.to_csv(cleaned_file_path)  # Save with "N/A" for missing data
            print(f"Cleaned data saved to: {cleaned_file_path}")

        else:
            print("No data to save. The cleaned DataFrame is empty.")

    except FileNotFoundError as fe:
        print(f"File not found error: {fe}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    clean_and_save_latest_combined_data()

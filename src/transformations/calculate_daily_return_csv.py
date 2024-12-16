import os
import glob
import pandas as pd

# Define directories for input and output
CLEANED_DATA_DIR = "data/silver/stocks/"  # Directory where cleaned data is stored
DAILY_RETURN_DIR = "data/silver/returns/"  # Directory where daily return data will be stored

# Ensure the output directory exists
os.makedirs(DAILY_RETURN_DIR, exist_ok=True)


def get_latest_cleaned_file(directory: str) -> str:
    """
    Get the latest cleaned file from the directory based on the filename format.

    :param directory: Directory where the cleaned files are stored.
    :return: Name of the latest cleaned file (basename).
    :raises FileNotFoundError: If no matching files are found.
    """
    cleaned_files = glob.glob(os.path.join(directory, "cleaned_*SP500-adj-close.csv"))
    if not cleaned_files:
        raise FileNotFoundError("No cleaned files found in the specified directory.")
    # Use modification time to find the latest file
    return max(cleaned_files, key=os.path.getmtime)


def clean_and_calculate_daily_returns(file_path: str, output_dir: str) -> None:
    """
    Clean the stock data and calculate daily returns, saving the output to a CSV file.
    """
    try:
        # Load the cleaned stock data
        print(f"Loading cleaned data from: {file_path}")
        stock_data = pd.read_csv(file_path, index_col=0, parse_dates=True)

        # Combine all cleaning steps
        stock_data = stock_data[~((stock_data == 0) | (stock_data.isna())).all(axis=1)]

        # Calculate daily returns
        daily_returns = stock_data.pct_change()

        # Round daily returns to 3 decimal places
        daily_returns = daily_returns.round(3)

        # Construct output file path
        output_file_name = f"returns_{os.path.basename(file_path)}"
        output_file_path = os.path.join(output_dir, output_file_name)

        # Save daily returns to output file
        daily_returns.to_csv(output_file_path)
        print(f"Daily returns saved to: {output_file_path}")

        # Display a sample of the saved data
        print("Sample of the saved daily returns:")
        print(daily_returns.head())  # Print the first 5 rows of daily returns

    except Exception as e:
        print(f"Error while processing file: {e}")


if __name__ == "__main__":
    try:
        # Get the latest cleaned file
        latest_file_path = get_latest_cleaned_file(CLEANED_DATA_DIR)
        print(f"Latest cleaned file identified: {latest_file_path}")

        # Clean the data and calculate daily returns
        clean_and_calculate_daily_returns(latest_file_path, DAILY_RETURN_DIR)

    except FileNotFoundError as fnf_error:
        print(f"Error: {fnf_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

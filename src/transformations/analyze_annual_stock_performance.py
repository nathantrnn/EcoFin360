import os
import re
import pandas as pd
import logging

# Initialize logger and relevant directory paths
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DAILY_RETURN_DIR = "data/silver/returns/"  # Directory for daily returns
ANNUAL_PERFORMANCE_DIR = "data/silver/performance/"  # Directory for annual performance output
os.makedirs(ANNUAL_PERFORMANCE_DIR, exist_ok=True)


def get_latest_daily_return_parquet_file(directory: str) -> str:
    """
    Get the latest Parquet file for daily returns from the given directory.

    Args:
        directory (str): Path to the directory containing Parquet folders.

    Returns:
        str: Path to the latest Parquet folder.
    """
    try:
        # Identify files matching the naming convention

        parquet_files = [
            os.path.join(directory, f) for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f)) and
               re.match(r"returns_cleaned_.*\.parquet$", f)
        ]
        if not parquet_files:
            raise FileNotFoundError("No suitable Parquet files found in the directory.")

        # Get the most recently modified file
        latest_file = max(parquet_files, key=os.path.getmtime)
        logger.info(f"Latest daily return Parquet file identified: {latest_file}")
        return latest_file

        # Get the most recently modified file
        # Get the most recently modified folder
        latest_folder = max(parquet_folders, key=os.path.getmtime)
        logger.info(f"Latest daily return Parquet file identified: {latest_file}")
        return latest_file
    except Exception as e:
        logger.error(f"Error finding latest daily return Parquet folder: {e}")
        raise


def extract_date_from_file(file_name: str) -> str:
    """
    Extract the date (YYMMDD) from the file name.

    Args:
        file_name (str): Name of the file (e.g., 'returns_cleaned_231015-SP500-adj-close.parquet').

    Returns:
        str: Extracted date in YYMMDD format.
    """
    match = re.search(r"returns_cleaned_(\d{6})-", file_name)
    if match:
        return match.group(1)
    raise ValueError(f"File name does not contain a valid date: {file_name}")


def calculate_annual_metrics_for_latest(folder_path: str, output_dir: str) -> None:
    """
    Calculate annual stock performance metrics from the latest daily return data.

    Args:
        folder_path (str): Path to the latest Parquet file.
        output_dir (str): Directory to save annual performance outputs.
    """
    try:
        file_name = os.path.basename(folder_path)
        date_part = extract_date_from_file(file_name)

        logger.info(f"Reading daily return data from: {folder_path}")
        daily_returns = pd.read_parquet(folder_path)

        # Ensure 'Date' column is set as a DatetimeIndex
        if 'Date' in daily_returns.columns:
            daily_returns['Date'] = pd.to_datetime(daily_returns['Date'])
            daily_returns.set_index('Date', inplace=True)

        if not isinstance(daily_returns.index, pd.DatetimeIndex):
            raise ValueError("Daily returns data index must be a DatetimeIndex.")

        # Check for the benchmark ticker
        benchmark_ticker = "^GSPC"
        if benchmark_ticker not in daily_returns.columns:
            raise ValueError(f"Benchmark ticker '{benchmark_ticker}' not found in the data.")

        # Annualized metrics calculations for all tickers
        logger.info(f"Calculating metrics for all tickers (benchmark included)...")
        annual_data = daily_returns.groupby(daily_returns.index.year).agg(["mean", "std", "var"])

        # Benchmark Annual Volatility
        benchmark_volatility = (
                annual_data[benchmark_ticker]["std"].mean() * (252 ** 0.5)
        )
        logger.info(
            f"Annual Volatility of the benchmark ticker '{benchmark_ticker}': {benchmark_volatility:.4f}"
        )

        # Calculate metrics for each ticker
        metrics = []
        for ticker in daily_returns.columns:
            ticker_data = annual_data[ticker]
            avg_annual_return = ticker_data["mean"].mean() * 252
            avg_annual_volatility = ticker_data["std"].mean() * (252 ** 0.5)
            avg_annual_variance = ticker_data["var"].mean() * 252
            beta = avg_annual_volatility / benchmark_volatility if benchmark_volatility > 0 else None
            metrics.append(
                [
                    ticker,
                    round(avg_annual_return, 2),
                    round(avg_annual_volatility, 2),
                    round(avg_annual_variance, 2),
                    round(beta, 2),
                ]
            )

        # Create DataFrame for all metrics
        performance_df = pd.DataFrame(
            metrics,
            columns=[
                "Ticker",
                "Average Annual Return",
                "Annual Volatility",
                "Annual Variance",
                "Beta",
            ],
        )

        # Define output file paths
        csv_output_file = os.path.join(output_dir, f"performance_{date_part}-SP500-adj-close.csv")
        parquet_output_file = os.path.join(output_dir, f"performance_{date_part}-SP500-adj-close.parquet")

        # Save to CSV
        logger.info(f"Saving performance data to CSV: {csv_output_file}")
        performance_df.to_csv(csv_output_file, index=False)

        # Save to Parquet
        logger.info(f"Saving performance data to Parquet: {parquet_output_file}")
        performance_df.to_parquet(parquet_output_file, index=False, engine="pyarrow")

        # Display a sample
        logger.info(f"Sample of calculated annual performance metrics:")
        print(performance_df.head())

    except Exception as e:
        logger.error(f"Failed to calculate annual performance metrics: {e}")
        raise



if __name__ == "__main__":
    try:
        latest_file = get_latest_daily_return_parquet_file(DAILY_RETURN_DIR)
        calculate_annual_metrics_for_latest(latest_file, ANNUAL_PERFORMANCE_DIR)
    except Exception as e:
        logger.error(f"Error in processing: {e}")

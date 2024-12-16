import pandas as pd


def load_csv(file_path, **kwargs):
    """Load a CSV file."""
    try:
        return pd.read_csv(file_path, **kwargs)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return pd.DataFrame()


def save_csv(data, file_path):
    """Save data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)


def save_parquet(data, file_path):
    """Save data to a Parquet file."""
    data.to_parquet(file_path, index=True)

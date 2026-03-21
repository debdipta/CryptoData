import pandas as pd
import tarfile
import os
import glob
import json
from datetime import datetime
import pyarrow.feather as feather
import tempfile

class DataLoader:
    """Class to load and analyze cryptocurrency data from tar.gz archives"""

    def __init__(self, data_dir="../crypto_data"):
        """
        Initialize DataLoader

        Args:
            data_dir (str): Path to the directory containing tar.gz files
        """
        self.data_dir = data_dir
        self.temp_dir = tempfile.mkdtemp()

    def list_available_files(self, symbol=None):
        """
        List all available tar.gz files

        Args:
            symbol (str, optional): Filter by symbol (e.g., 'BTCUSD')

        Returns:
            list: List of available tar.gz files
        """
        if symbol:
            pattern = os.path.join(self.data_dir, symbol, "*.tar.gz")
        else:
            pattern = os.path.join(self.data_dir, "**", "*.tar.gz")

        files = glob.glob(pattern, recursive=True)
        return sorted(files)

    def load_single_file(self, tar_gz_path):
        """Load data from a single tar.gz file

        Args:
            tar_gz_path (str): Path to the tar.gz file

        Returns:
            tuple[pd.DataFrame, dict]: (DataFrame containing OHLCV data, options data dict)
        """
        try:
            # Extract the tar.gz file
            with tarfile.open(tar_gz_path, 'r:gz') as tar:
                members = tar.getnames()
                feather_file = None
                options_file = None

                # Identify expected files
                for m in members:
                    if m.endswith('.feather'):
                        feather_file = m
                    if m.endswith('_options.json'):
                        options_file = m

                # Extract and read feather file
                df = pd.DataFrame()
                if feather_file:
                    tar.extract(feather_file, self.temp_dir)
                    extracted_feather = os.path.join(self.temp_dir, feather_file)
                    df = feather.read_feather(extracted_feather)
                    os.remove(extracted_feather)

                    # Ensure timestamp is datetime
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])

                # Extract and read options file
                options_data = {}
                if options_file:
                    # Use extractall with explicit member to avoid future tarfile security changes
                    member = tar.getmember(options_file)
                    tar.extractall(self.temp_dir, members=[member])
                    extracted_options = os.path.join(self.temp_dir, options_file)
                    with open(extracted_options, 'r') as f:
                        options_data = json.load(f)
                    os.remove(extracted_options)

                print(f"Loaded {len(df)} records and options data from {tar_gz_path}")
                return df, options_data

        except Exception as e:
            print(f"Error loading {tar_gz_path}: {e}")
            return None, {}

    def load_date_range(self, start_date, end_date, symbol=None):
        """
        Load data for a specific date range

        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format
            end_date (str): End date in 'YYYY-MM-DD' format
            symbol (str, optional): Symbol to filter by

        Returns:
            pd.DataFrame: Combined DataFrame for the date range
        """
        files = self.list_available_files(symbol)

        # Filter files by date range
        filtered_files = []
        for file_path in files:
            # Extract date from filename (format: YYYY-MM-DD.tar.gz)
            filename = os.path.basename(file_path)
            date_str = filename.replace('.tar.gz', '')

            try:
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                start = datetime.strptime(start_date, '%Y-%m-%d').date()
                end = datetime.strptime(end_date, '%Y-%m-%d').date()

                if start <= file_date <= end:
                    filtered_files.append(file_path)
            except ValueError:
                continue

        # Load and combine all files
        dfs = []
        for file_path in filtered_files:
            df = self.load_single_file(file_path)
            if df is not None:
                dfs.append(df)

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
            print(f"Loaded {len(combined_df)} total records from {len(filtered_files)} files")
            return combined_df
        else:
            print("No data found for the specified date range")
            return pd.DataFrame()

    def load_latest_days(self, days=7, symbol=None):
        """
        Load data for the last N days

        Args:
            days (int): Number of days to load
            symbol (str, optional): Symbol to filter by

        Returns:
            pd.DataFrame: Combined DataFrame for the last N days
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d')

        return self.load_date_range(start_date, end_date, symbol)

    def get_data_summary(self, df):
        """
        Get summary statistics for the loaded data

        Args:
            df (pd.DataFrame): DataFrame to analyze

        Returns:
            dict: Summary statistics
        """
        if df.empty:
            return {"error": "DataFrame is empty"}

        summary = {
            "total_records": len(df),
            "date_range": {
                "start": df['timestamp'].min(),
                "end": df['timestamp'].max()
            },
            "price_stats": {
                "open": {
                    "min": df['open'].min(),
                    "max": df['open'].max(),
                    "mean": df['open'].mean()
                },
                "high": {
                    "min": df['high'].min(),
                    "max": df['high'].max(),
                    "mean": df['high'].mean()
                },
                "low": {
                    "min": df['low'].min(),
                    "max": df['low'].max(),
                    "mean": df['low'].mean()
                },
                "close": {
                    "min": df['close'].min(),
                    "max": df['close'].max(),
                    "mean": df['close'].mean()
                }
            },
            "volume_stats": {
                "total": df['volume'].sum(),
                "average": df['volume'].mean(),
                "max": df['volume'].max()
            }
        }

        return summary

    def cleanup(self):
        """Clean up temporary files"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)


# Convenience functions for quick access
def load_file(tar_gz_path):
    """Quick function to load a single tar.gz file"""
    loader = DataLoader()
    df, options = loader.load_single_file(tar_gz_path)
    loader.cleanup()
    return df, options

def load_last_days(days=7, symbol=None):
    """Quick function to load data for the last N days"""
    loader = DataLoader()
    df = loader.load_latest_days(days, symbol)
    loader.cleanup()
    return df

def list_files(symbol=None):
    """Quick function to list available files"""
    loader = DataLoader()
    files = loader.list_available_files(symbol)
    loader.cleanup()
    return files


if __name__ == "__main__":
    # Example usage
    print("CryptoData Loader")
    print("=================")

    # List available files
    print("\nAvailable files:")
    files = list_files()
    for file in files:
        print(f"  {file}")

    if files:
        # Load the most recent file
        print(f"\nLoading most recent file: {files[-1]}")
        df, options = load_file(files[-1])

        if df is not None and not df.empty:
            print(f"Data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")

            if 'timestamp' in df.columns:
                print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            # Show all data
            print("\nComplete Data:")
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(df.to_string())

            # Show summary
            loader = DataLoader()
            summary = loader.get_data_summary(df)
            print(f"\nSummary: {summary}")

            # Print options sample
            if options:
                print("\nOption data sample:")
                for strike, data in list(options.items())[:5]:
                    print(f"  Strike {strike}: Call={data.get('call')} Put={data.get('put')}")

            loader.cleanup()
        else:
            print("No OHLCV data available in the file.")
            if options:
                print("Option data is present:")
                for strike, data in list(options.items())[:5]:
                    print(f"  Strike {strike}: Call={data.get('call')} Put={data.get('put')}")
    else:
        print("No tar.gz files found. Run the engine first to collect some data!")
#!/usr/bin/env python3
"""
Example usage of the CryptoData loader
"""

from data_loader import DataLoader, load_last_days, load_file, list_files

def main():
    print("CryptoData Analysis Example")
    print("=" * 30)

    # Initialize the data loader
    loader = DataLoader()

    try:
        # List all available files
        print("\n1. Available data files:")
        files = loader.list_available_files()
        if files:
            for file in files:
                print(f"   {file}")
        else:
            print("   No data files found. Run the engine to collect data first!")

        # Example: Load data for the last 7 days
        print("\n2. Loading data for the last 7 days...")
        df = loader.load_latest_days(days=7)

        if not df.empty:
            print(f"   Loaded {len(df)} records")
            print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            # Show basic statistics
            print("\n3. Basic Statistics:")
            print(f"   Average price: ${df['close'].mean():.2f}")
            print(f"   Highest price: ${df['high'].max():.2f}")
            print(f"   Lowest price: ${df['low'].min():.2f}")
            print(f"   Total volume: {df['volume'].sum():,.0f}")

            # Show first few rows
            print("\n4. Sample data (first 5 rows):")
            print(df.head().to_string(index=False))

        else:
            print("   No data available for the last 7 days")

        # Load and print options data from the most recent file
        if files:
            print("\n5. Options data (from most recent file):")
            latest_file = files[-1]
            df2, options = load_file(latest_file)
            if options:
                # Print a small sample of the option strikes
                for strike, data in list(options.items())[:5]:
                    print(f"   Strike {strike}: Call={data.get('call')} Put={data.get('put')}")
            else:
                print("   No options data found in the latest file")

    finally:
        # Always cleanup temporary files
        loader.cleanup()

    print("\n" + "=" * 30)
    print("Example completed!")

if __name__ == "__main__":
    main()
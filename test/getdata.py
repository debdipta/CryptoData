import requests
import time
from datetime import datetime

headers = {
  'Accept': 'application/json'
}

def get_current_timestamp():
    """Get current timestamp in seconds"""
    return int(time.time())

def fetch_data():
    """Fetch data from Delta Exchange API"""
    try:
        current_time = get_current_timestamp()
        # Get data for the last 10 minutes to ensure we get some candles
        start_time = current_time - 600  # 10 minutes ago

        print(f"[{datetime.now()}] Fetching data from {start_time} to {current_time}")

        r = requests.get('https://api.india.delta.exchange/v2/history/candles', params={
          'resolution': '1m',
          'symbol': 'BTCUSD',
          'start': str(start_time),
          'end': str(current_time)
        }, headers=headers, timeout=10)

        print(f"[{datetime.now()}] API Response Status: {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            candles = data.get('result', [])
            print(f"[{datetime.now()}] Fetched {len(candles)} candles")

            if candles:
                print(f"[{datetime.now()}] Latest candle: {candles[-1]}")
                print(f"[{datetime.now()}] Sample data structure: {list(candles[0].keys()) if candles else 'No data'}")
            else:
                print(f"[{datetime.now()}] No candles in response. Full response: {data}")

            return data
        else:
            print(f"[{datetime.now()}] Error: HTTP {r.status_code} - {r.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] Network error: {e}")
        return None
    except Exception as e:
        print(f"[{datetime.now()}] Error fetching data: {e}")
        return None

if __name__ == "__main__":
    print("Starting infinite data collection every 1 minute...")
    print("Press Ctrl+C to stop")

    try:
        while True:
            fetch_data()
            time.sleep(60)  # Wait 1 minute before next fetch

    except KeyboardInterrupt:
        print("\nStopped by user")
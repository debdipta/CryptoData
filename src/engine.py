import requests
import pandas as pd
import time
import os
import tarfile
import pdb
from datetime import datetime
import pyarrow.feather as feather
from .config import SYMBOL, TIMEFRAME, DATA_DIR

headers = {
    'Accept': 'application/json'
}

buffer = []

class AlphaExchange(object):
    def __self__(self):
        pass

    def __fetch_ohlc(self):
        global buffer

        try:
            current_time = int(time.time())
            # Get data for the last 10 minutes to ensure we get some candles
            start_time = current_time - 600  # 10 minutes ago

            print(f"[{datetime.now()}] Fetching data from {start_time} to {current_time}")

            r = requests.get('https://api.india.delta.exchange/v2/history/candles', params={
                'resolution': TIMEFRAME,
                'symbol': SYMBOL.replace('/', ''),  # Remove slash for API
                'start': str(start_time),
                'end': str(current_time)
            }, headers=headers, timeout=10)

            if r.status_code == 200:
                data = r.json()
                candles = data.get('result', [])

                if not candles:
                    print(f"[{datetime.now()}] No new candles available. Waiting for next candle...")
                    return

                # Get the latest candle
                latest_candle = candles[-1]

                # Convert to our record format
                # Delta API returns: {'time': timestamp, 'open': open, 'high': high, 'low': low, 'close': close, 'volume': volume}
                record = {
                    "timestamp": datetime.fromtimestamp(latest_candle['time']),
                    "open": latest_candle['open'],
                    "high": latest_candle['high'],
                    "low": latest_candle['low'],
                    "close": latest_candle['close'],
                    "volume": latest_candle['volume']
                }

                buffer.append(record)
                print(f"[{datetime.now()}] Fetched: {record}")

            else:
                print(f"[{datetime.now()}] API Error: HTTP {r.status_code} - {r.text}")
                print(f"[{datetime.now()}] Waiting before retry...")
                time.sleep(10)  # Wait 10 seconds before retry on API error

        except requests.exceptions.RequestException as e:
            print(f"[{datetime.now()}] Network error: {e}")
            print(f"[{datetime.now()}] Waiting before retry...")
            time.sleep(10)  # Wait 10 seconds before retry on network error

        except Exception as e:
            print(f"[{datetime.now()}] Unknown error: {e}")
            print(f"[{datetime.now()}] Waiting before retry...")
            time.sleep(10)  # Wait 10 seconds before retry on unknown error

    def __save_daily(self):

        global buffer

        if not buffer:
            return

        try:

            df = pd.DataFrame(buffer)

            # Ensure we only keep unique 1-minute candles (by timestamp)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)

            date_str = datetime.utcnow().strftime("%Y-%m-%d")

            folder = f"{DATA_DIR}/{SYMBOL.replace('/','')}"
            os.makedirs(folder, exist_ok=True)

            feather_file = f"{folder}/{date_str}.feather"

            feather.write_feather(df, feather_file)

            tar_file = feather_file + ".tar.gz"

            with tarfile.open(tar_file, 'w:gz') as tar:
                tar.add(feather_file, arcname=os.path.basename(feather_file))

            os.remove(feather_file)

            buffer = []

            print("Saved:", tar_file)

        except Exception as e:
            print("Save error:", e)


    def start_loop(self):

        last_day = datetime.utcnow().day

        while True:

            try:

                self.__fetch_ohlc()
                #pdb.set_trace()

                current_day = datetime.utcnow().day

                if current_day != last_day:
                    self.__save_daily()
                    last_day = current_day

                time.sleep(60)

            except KeyboardInterrupt:
                self.__save_daily()
                break

            except Exception as e:
                print("Main loop error:", e)
                time.sleep(10)
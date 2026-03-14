import ccxt
import pandas as pd
import time
import os
import tarfile
import pdb
from datetime import datetime
import pyarrow.feather as feather
from .config import SYMBOL, TIMEFRAME, DATA_DIR

exchange = ccxt.binance()

buffer = []

class AlphaExchange(object):
    def __self__(self):
        pass

    def __fetch_ohlc(self):

        global buffer

        try:

            data = exchange.fetch_ohlcv(
                SYMBOL,
                timeframe=TIMEFRAME,
                limit=1
            )

            if not data:
                print("No data available. Waiting...")
                return

            row = data[0]

            record = {
                "timestamp": datetime.utcfromtimestamp(row[0]/1000),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5]
            }

            buffer.append(record)

            print("Fetched:", record)

        except ccxt.NetworkError as e:
            print("Network error:", e)

        except ccxt.ExchangeError as e:
            print("Exchange error:", e)

        except Exception as e:
            print("Unknown error:", e)

    def __save_daily(self):

        global buffer

        if not buffer:
            return

        try:

            df = pd.DataFrame(buffer)

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
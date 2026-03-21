import requests
import pandas as pd
import json
import time
import os
import tarfile
import pdb
from datetime import datetime, timedelta, time as dt_time
import pyarrow.feather as feather
from .config import SYMBOL, TIMEFRAME, DATA_DIR

headers = {
    'Accept': 'application/json'
}

buffer = []
options_buffer = {}

class AlphaExchange(object):
    def __init__(self):
        pass

    def __get_expiry_date(self):
        # India Standard Time (UTC+5:30)
        now_utc = datetime.utcnow()
        now_ist = now_utc + timedelta(hours=5, minutes=30)
        cutoff = dt_time(17, 30)

        if now_ist.time() >= cutoff:
            target = now_ist + timedelta(days=1)
        else:
            target = now_ist

        expiry_date = target.strftime('%d-%m-%Y')
        print(f"[{datetime.now()}] IST now: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}, using expiry date: {expiry_date}")
        return expiry_date

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

    def __fetch_options(self):
        """Fetch options prices for Call and Put with strike prices relative to current spot price"""
        global options_buffer

        try:
            timestamp = datetime.now().isoformat()
            options_data = {}

            # Get current spot price from the latest buffer entry
            if not buffer:
                print(f"[{datetime.now()}] No buffer data available. Skipping options fetch.")
                return

            current_spot = buffer[-1]['close']

            # Round current spot price to nearest 1000
            current_spot = round(current_spot / 1000) * 1000

            if current_spot <= 0:
                print(f"[{datetime.now()}] Invalid current spot price. Skipping options fetch.")
                return

            # Determine expiry date based on India time: before 17:30 IST use today, otherwise use tomorrow
            expiry_date = self.__get_expiry_date()

            # Define strike price range: current_spot - 2000 to current_spot + 2000
            lower_limit = max(0, int(current_spot - 2000))  # Ensure non-negative
            upper_limit = int(current_spot + 2000)
            increment = 1000  # Fetch every 1000 units for granularity

            print(f"[{datetime.now()}] Current spot price: {current_spot}")
            print(f"[{datetime.now()}] Using expiry date: {expiry_date}")
            print(f"[{datetime.now()}] Fetching options data for range {lower_limit} to {upper_limit}...")

            # Fetch options using contract_types with strike range
            if '/' in SYMBOL:
                underlying = SYMBOL.split('/')[0]
            elif SYMBOL.endswith('USDT'):
                underlying = SYMBOL[:-4]
            elif SYMBOL.endswith('USD'):
                underlying = SYMBOL[:-3]
            else:
                underlying = SYMBOL

            # Fetch options using contract_types with strike range
            tickers_url = f"https://api.india.delta.exchange/v2/tickers?contract_types=call_options,put_options&underlying_asset_symbols={underlying}&expiry_date={expiry_date}&min_strike={lower_limit}&max_strike={upper_limit}"
            params = {}

            response = requests.get(tickers_url, params=params, headers=headers, timeout=10)
            print(f"[{datetime.now()}] Requested ticker data: {response.url}")
            if response.status_code == 200:
                data = response.json()
                tickers = data.get('result', [])

                for ticker in tickers:
                    try:
                        symbol = ticker.get('symbol', '')
                        last_price = ticker.get('last_price', 0)

                        # Expect symbol format: C-BTC-38100-230124 or P-BTC-55800-170224
                        parts = symbol.split('-')
                        if len(parts) != 4:
                            continue

                        option_type = parts[0]  # 'C' or 'P'
                        strike_price = int(parts[2])

                        if strike_price < lower_limit or strike_price > upper_limit:
                            continue

                        if strike_price not in options_data:
                            options_data[strike_price] = {'call': 0, 'put': 0}

                        if option_type == 'C':
                            options_data[strike_price]['call'] = last_price
                        elif option_type == 'P':
                            options_data[strike_price]['put'] = last_price

                    except Exception:
                        continue

                if options_data:
                    # Store in nested dict: {timestamp: {strike_price: {call/put prices}}}
                    options_buffer[timestamp] = options_data
                    print(f"[{datetime.now()}] Fetched options data for {len(options_data)} strike prices (range: {lower_limit}-{upper_limit})")
                    chain_df = self.__prepare_option_chain_data(tickers)
                    self.__print_option_chain(chain_df, timestamp)
                else:
                    print(f"[{datetime.now()}] No options data available for range {lower_limit}-{upper_limit}")
            else:
                print(f"[{datetime.now()}] Options ticker request failed: HTTP {response.status_code} - {response.text}")

        except Exception as e:
            print(f"[{datetime.now()}] Options fetch error: {e}")

    def __prepare_option_chain_data(self, raw_option_items):
        """Convert raw option entries into a normalized chain form."""
        rows = []
        for item in raw_option_items:
            try:
                strike = int(item.get('strike_price', 0))
                ctype = item.get('contract_type', '').replace('_options', '').upper()
                symbol = item.get('symbol', '')
                last = float(item.get('last_price', item.get('close', 0) or 0))
                bid = float(item.get('quotes', {}).get('best_bid', 0) or 0)
                ask = float(item.get('quotes', {}).get('best_ask', 0) or 0)
                mid = (bid + ask) / 2 if bid and ask else float(item.get('mark_price', 0) or 0)
                iv = float(item.get('quotes', {}).get('mark_iv', item.get('quotes', {}).get('ask_iv', 0) or 0))
                oi = float(item.get('oi', 0) or 0)
                vol = float(item.get('volume', 0) or 0)
                greeks = item.get('greeks', {}) or {}
                delta = float(greeks.get('delta', 0) or 0)
                gamma = float(greeks.get('gamma', 0) or 0)
                theta = float(greeks.get('theta', 0) or 0)
                vega = float(greeks.get('vega', 0) or 0)
                rows.append({
                    'strike': strike,
                    'type': ctype,
                    'symbol': symbol,
                    'bid': bid,
                    'ask': ask,
                    'mid': mid,
                    'last': last,
                    'iv': iv,
                    'oi': oi,
                    'volume': vol,
                    'delta': delta,
                    'gamma': gamma,
                    'theta': theta,
                    'vega': vega,
                })
            except Exception:
                continue

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows).sort_values(['strike', 'type'], ascending=[True, True]).reset_index(drop=True)

    def __print_option_chain(self, options_df, timestamp):
        """Print option chain in professional table format with put on left, strike in middle, call on right."""
        if options_df is None or options_df.empty:
            print(f"[{datetime.now()}] Empty option chain for {timestamp}")
            return

        header = f"{datetime.now()} | Options Chain at {timestamp} (strikes {options_df['strike'].min()} - {options_df['strike'].max()})"
        print(header)
        print("=" * len(header))
        print(f"{'PUT OI':>10} {'PUT BID':>10} {'PUT ASK':>10} {'PUT IV':>10} {'Strike':>12} {'CALL IV':>10} {'CALL BID':>10} {'CALL ASK':>10} {'CALL OI':>10}")
        print('-' * 110)

        pivot = options_df.pivot(index='strike', columns='type', values=['bid', 'ask', 'iv', 'oi'])
        for strike in sorted(pivot.index):
            put_bid = pivot.at[strike, ('bid', 'PUT')] if ('bid', 'PUT') in pivot.columns else None
            put_ask = pivot.at[strike, ('ask', 'PUT')] if ('ask', 'PUT') in pivot.columns else None
            put_iv = pivot.at[strike, ('iv', 'PUT')] if ('iv', 'PUT') in pivot.columns else None
            put_oi = pivot.at[strike, ('oi', 'PUT')] if ('oi', 'PUT') in pivot.columns else None

            call_bid = pivot.at[strike, ('bid', 'CALL')] if ('bid', 'CALL') in pivot.columns else None
            call_ask = pivot.at[strike, ('ask', 'CALL')] if ('ask', 'CALL') in pivot.columns else None
            call_iv = pivot.at[strike, ('iv', 'CALL')] if ('iv', 'CALL') in pivot.columns else None
            call_oi = pivot.at[strike, ('oi', 'CALL')] if ('oi', 'CALL') in pivot.columns else None

            put_bid_s = f"{put_bid:>10.2f}" if put_bid is not None else ' ' * 10
            put_ask_s = f"{put_ask:>10.2f}" if put_ask is not None else ' ' * 10
            put_iv_s = f"{put_iv:>10.2%}" if put_iv is not None else ' ' * 10
            put_oi_s = f"{put_oi:>10.2f}" if put_oi is not None else ' ' * 10

            call_bid_s = f"{call_bid:>10.2f}" if call_bid is not None else ' ' * 10
            call_ask_s = f"{call_ask:>10.2f}" if call_ask is not None else ' ' * 10
            call_iv_s = f"{call_iv:>10.2%}" if call_iv is not None else ' ' * 10
            call_oi_s = f"{call_oi:>10.2f}" if call_oi is not None else ' ' * 10

            print(f"{put_oi_s} {put_bid_s} {put_ask_s} {put_iv_s} {strike:>12} {call_iv_s} {call_bid_s} {call_ask_s} {call_oi_s}")

    def __save_daily(self):

        global buffer, options_buffer

        if not buffer and not options_buffer:
            return

        try:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            folder = f"{DATA_DIR}/{SYMBOL.replace('/','')}"
            os.makedirs(folder, exist_ok=True)

            # Save OHLCV data
            if buffer:
                df = pd.DataFrame(buffer)

                # Ensure we only keep unique 1-minute candles
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)

                ohlcv_file = f"{folder}/{date_str}_ohlcv.feather"
                feather.write_feather(df, ohlcv_file)
                print(f"[{datetime.now()}] Saved OHLCV data: {len(df)} records")

            # Save Options data
            if options_buffer:
                options_file = f"{folder}/{date_str}_options.json"
                with open(options_file, 'w') as f:
                    json.dump(options_buffer, f, indent=2)
                print(f"[{datetime.now()}] Saved Options data: {len(options_buffer)} timestamps")

            # Create tar.gz with both files
            tar_file = f"{folder}/{date_str}.tar.gz"

            with tarfile.open(tar_file, 'w:gz') as tar:
                if ohlcv_file and os.path.exists(ohlcv_file):
                    tar.add(ohlcv_file, arcname=os.path.basename(ohlcv_file))
                    os.remove(ohlcv_file)

                if options_file and os.path.exists(options_file):
                    tar.add(options_file, arcname=os.path.basename(options_file))
                    os.remove(options_file)

            # Clear buffers
            buffer = []
            options_buffer = {}

            print(f"[{datetime.now()}] Saved: {tar_file}")

        except Exception as e:
            print(f"[{datetime.now()}] Save error: {e}")


    def start_loop(self):

        last_day = datetime.utcnow().day

        while True:

            try:

                self.__fetch_ohlc()
                self.__fetch_options()

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
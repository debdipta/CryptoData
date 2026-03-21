import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('https://api.india.delta.exchange/v2/tickers?contract_types=call_options,put_options&underlying_asset_symbols=BTC&expiry_date=22-03-2026', params={
}, headers = headers)

print(r.json())
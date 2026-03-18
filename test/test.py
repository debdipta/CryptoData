import requests

import hashlib
import hmac
import requests
import time

base_url = 'https://api.india.delta.exchange'
api_key = 'xo6Bb88O0FjGIeIqDvtZid2fYQics1'
api_secret = 'jITNzyoLBHaPBj8HOJYvemgeT2Ify9QC0v8f2eRvEWyAEZlWJzqKfVRo0gzE'

def generate_signature(secret, message):
    message = bytes(message, 'utf-8')
    secret = bytes(secret, 'utf-8')
    hash = hmac.new(secret, message, hashlib.sha256)
    return hash.hexdigest()

# Get open orders
method = 'GET'
timestamp = str(int(time.time()))
path = '/v2/orders'
url = f'{base_url}{path}'
query_string = '?product_id=1&state=open'
payload = ''
signature_data = method + timestamp + path + query_string + payload
signature = generate_signature(api_secret, signature_data)

req_headers = {
  'api-key': api_key,
  'timestamp': timestamp,
  'signature': signature,
  'User-Agent': 'python-rest-client',
  'Content-Type': 'application/json'
}

query = {"product_id": 1, "state": 'open'}

response = requests.request(
    method, url, data=payload, params=query, timeout=(3, 27), headers=req_headers
)

# Place new order
method = 'POST'
timestamp = str(int(time.time()))
path = '/v2/orders'
url = f'{base_url}{path}'
query_string = ''
payload = "{\"order_type\":\"limit_order\",\"size\":3,\"side\":\"buy\",\"limit_price\":\"0.0005\",\"product_id\":16}"
signature_data = method + timestamp + path + query_string + payload
signature = generate_signature(api_secret, signature_data)

req_headers = {
  'api-key': api_key,
  'timestamp': timestamp,
  'signature': signature,
  'User-Agent': 'rest-client',
  'Content-Type': 'application/json'
}

headers = {
  'Accept': 'application/json',
  'api-key': api_key,
  'signature': signature,
  'timestamp': timestamp
}

r = requests.get('https://api.india.delta.exchange/v2/positions/margined', params={

}, headers = headers)
print(r.json())


response = requests.request(
    method, url, data=payload, params={}, timeout=(3, 27), headers=req_headers
)
print(response.json())
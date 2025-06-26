import hmac
import hashlib
import json
import time
import requests

# API credentials
api_key = "afef5d937dcd8ae532be3e9ed5335c53c0b4c8ee71085828"
api_secret = "e24e612ef44a775ea98905174c11e59cbab665a1e17059312b23cef27d06f127"
url = "https://api.coindcx.com/exchange/v1/derivatives/futures/orders"

# Generate timestamp
timestamp = int(time.time() * 1000)

# Request body
body = {
    "timestamp": timestamp,
    "order": {
        "pair": "B-ADA_USDT",
        "side": "buy",
        "total_quantity": 1000,  # Integer
        "order_type": "market_order",
        "notification": "no_notification",
        "time_in_force": "good_till_cancel",
        "leverage": 5.0  # Float
    }
}

# Generate signature
body_str = json.dumps(body, separators=(',', ':'))
print("Body String:", body_str)
signature = hmac.new(
    api_secret.encode('utf-8'),
    body_str.encode('utf-8'),
    hashlib.sha256
).hexdigest()
print("Signature:", signature)

# Headers
headers = {
    'Content-Type': 'application/json',
    'X-AUTH-APIKEY': api_key,
    'X-AUTH-SIGNATURE': signature
}

# Send request
try:
    response = requests.post(url, json=body, headers=headers)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print(f"Response Text: {response.text}")
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")
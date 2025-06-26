import socketio
import json
import time

# Initialize Socket.IO client with reconnection logic
sio = socketio.Client(reconnection=True, reconnection_attempts=10, reconnection_delay=2, reconnection_delay_max=10)

# WebSocket endpoint (verify with CoinDCX API docs for futures-specific endpoint)
socket_endpoint = "wss://stream.coindcx.com"  # Update if futures use a different endpoint

# Event handler for connection
@sio.event
def connect():
    print("Connected to CoinDCX Futures WebSocket")
    # Subscribe to a futures channel, e.g., BTC/USDT perpetual futures
    channel = 'F-BTCUSDT_PERP'  # Adjust channel name based on CoinDCX futures API
    sio.emit('join', {'channelName': channel})
    print(f"Subscribed to {channel}")

# Event handler for connection error
@sio.event
def connect_error(data):
    print(f"Connection error: {data}")

# Event handler for disconnection
@sio.event
def disconnect():
    print("Disconnected from CoinDCX Futures WebSocket")

# Catch-all event handler for debugging incoming data
@sio.on('*')
def catch_all(event, data):
    print(f"Event: {event}, Data: {data}")

# Specific event handler for futures channel data
@sio.on('F-BTCUSDT_PERP')  # Adjust based on actual channel name
def on_futures_data(data):
    try:
        # Parse data (assuming JSON string or dict)
        if isinstance(data, str):
            data = json.loads(data)
        
        # Extract relevant futures data (adjust fields based on API response)
        if 'lastPrice' in data:
            last_price = float(data['lastPrice'])
            volume = float(data.get('volume', 0))
            timestamp = data.get('timestamp', int(time.time() * 1000))
            
            # Example: Process futures data
            print(f"BTC/USDT Futures - Last Price: {last_price}, Volume: {volume}, Timestamp: {timestamp}")
            
            # Add futures-specific logic (e.g., calculate funding rate, leverage, etc.)
            # Example: Monitor for price thresholds
            if last_price > 60000:
                print("Alert: Futures price above $60,000!")
            elif last_price < 50000:
                print("Alert: Futures price below $50,000!")
                
    except Exception as e:
        print(f"Error processing futures data: {e}")

# Connect to CoinDCX WebSocket
try:
    sio.connect(socket_endpoint, transports=['websocket'], headers={'User-Agent': 'Python-SocketIO-Client'})
    sio.wait()
except socketio.exceptions.ConnectionError as e:
    print(f"Connection failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
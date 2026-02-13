import websocket
import json
import threading

def on_message(ws, message):
    data = json.loads(message)
    if 'topic' in data and 'tickers' in data['topic']:
        if 'data' in data:
            ticker = data['data']
            symbol = ticker['symbol']
            print(f"\n📊 {symbol}:")
            print(f"  lastPrice: {ticker.get('lastPrice', 'N/A')}")
            print(f"  bid1Price: {ticker.get('bid1Price', 'N/A')}")
            print(f"  ask1Price: {ticker.get('ask1Price', 'N/A')}")
            print(f"  highPrice24h: {ticker.get('highPrice24h', 'N/A')}")
            print(f"  lowPrice24h: {ticker.get('lowPrice24h', 'N/A')}")
            print(f"  volume24h: {ticker.get('volume24h', 'N/A')}")
            print("-" * 40)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("Connected to Bybit")
    # Subscribe to ETHUSDT
    subscribe_msg = {
        "op": "subscribe",
        "args": ["tickers.ETHUSDT"]
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribed to ETHUSDT")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://stream.bybit.com/v5/public/linear",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()
    
    # Keep running for 30 seconds
    import time
    time.sleep(30)

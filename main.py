import os
import json
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
import websocket
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

price_data = {}
subscribed_symbols = set()
user_subscriptions = {}
price_history = {}

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Telegram Price Bot is running!')
    def log_message(self, format, *args):
        pass

def run_health_server():
    try:
        port = int(os.environ.get('PORT', 10000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"🏥 Health check server running on port {port}")
        server.serve_forever()
    except Exception as e:
        print(f"⚠️ Health server error: {e}")

class BybitWebSocket:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.connect()
    
    def connect(self):
        try:
            self.ws = websocket.WebSocketApp(
                BYBIT_WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            wst = threading.Thread(target=self.ws.run_forever)
            wst.daemon = True
            wst.start()
        except Exception as e:
            print(f"WebSocket connection error: {e}")
    
    def on_open(self, ws):
        print("✅ WebSocket connected to Bybit")
        self.connected = True
        for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
            self.subscribe_ticker(symbol.replace('USDT', ''))
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if 'topic' in data and 'tickers' in data['topic']:
                if 'data' in data:
                    ticker_data = data['data']
                    symbol = ticker_data['symbol']
                    
                    # Lấy giá
                    last_price = ticker_data.get('lastPrice')
                    if last_price in [None, 'N/A', '']:
                        last_price = ticker_data.get('bid1Price', 'N/A')
                    
                    # Lấy volume - thử nhiều field khác nhau
                    volume = 'N/A'
                    for field in ['volume24h', 'turnover24h', 'volume', 'turnover24hUsd']:
                        val = ticker_data.get(field)
                        if val not in [None, 'N/A', '']:
                            try:
                                # Thử convert sang float để kiểm tra
                                float(val)
                                volume = val
                                break
                            except:
                                continue
                    
                    bid = ticker_data.get('bid1Price', 'N/A')
                    ask = ticker_data.get('ask1Price', 'N/A')
                    
                    # Lưu lịch sử giá
                    if symbol not in price_history:
                        price_history[symbol] = []
                    
                    if last_price != 'N/A':
                        try:
                            price_history[symbol].append(float(last_price))
                            if len(price_history[symbol]) > 100:
                                price_history[symbol].pop(0)
                        except:
                            pass
                    
                    # Tính high/low
                    high = 'N/A'
                    low = 'N/A'
                    if price_history[symbol]:
                        try:
                            high = f"{max(price_history[symbol]):.2f}"
                            low = f"{min(price_history[symbol]):.2f}"
                        except:
                            pass
                    
                    price_data[symbol] = {
                        'last_price': last_price,
                        'bid_price': bid,
                        'ask_price': ask,
                        'volume': volume,
                        'high': high,
                        'low': low,
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    }
                    
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def on_error(self, ws, error):
        print(f"❌ WebSocket error: {error}")
        self.connected = False
    
    def on_close(self, ws, close_status_code, close_msg):
        print("🔴 WebSocket closed")
        self.connected = False
        time.sleep(5)
        self.connect()
    
    def subscribe_ticker(self, symbol):
        if not self.connected or not self.ws:
            return False
        try:
            formatted_symbol = symbol.upper()
            if not formatted_symbol.endswith('USDT'):
                formatted_symbol += 'USDT'
            
            if formatted_symbol in subscribed_symbols:
                return True
                
            subscribe_msg = {
                "op": "subscribe",
                "args": [f"tickers.{formatted_symbol}"]
            }
            self.ws.send(json.dumps(subscribe_msg))
            subscribed_symbols.add(formatted_symbol)
            print(f"📡 Subscribed to {formatted_symbol}")
            return True
        except Exception as e:
            print(f"Error subscribing: {e}")
            return False

bybit_ws = BybitWebSocket()

def format_price(price):
    if price in [None, 'N/A', '']:
        return 'N/A'
    try:
        return f"${float(price):,.2f}"
    except:
        return f"${price}"

def format_volume(volume):
    if volume in [None, 'N/A', '']:
        return 'N/A'
    try:
        vol = float(volume)
        if vol > 1_000_000_000:
            return f"{vol/1_000_000_000:.2f}B"
        elif vol > 1_000_000:
            return f"{vol/1_000_000:.2f}M"
        elif vol > 1_000:
            return f"{vol/1_000:.2f}K"
        else:
            return f"{vol:.2f}"
    except:
        return str(volume)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *Bybit Crypto Price Bot*\n\n"
        "💰 /price btc - Giá Bitcoin\n"
        "📌 /subscribe btc - Theo dõi\n"
        "❌ /unsubscribe btc - Hủy\n"
        "📋 /mylist - Danh sách\n"
        "📊 /status - Trạng thái\n"
        "🆘 /help - Hướng dẫn",
        parse_mode='Markdown'
    )

async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /price btc eth sol")
        return
    
    responses = []
    for arg in context.args:
        symbol = arg.upper()
        formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
        
        if formatted_symbol not in subscribed_symbols:
            bybit_ws.subscribe_ticker(symbol)
            time.sleep(1)
        
        found = False
        for i in range(6):
            if formatted_symbol in price_data:
                data = price_data[formatted_symbol]
                
                # Lấy giá
                price = data['last_price']
                if price in [None, 'N/A', '']:
                    price = data['bid_price']
                if price in [None, 'N/A', '']:
                    price = data['ask_price']
                
                # Format bid/ask
                bid = data['bid_price'] if data['bid_price'] not in [None, 'N/A', ''] else '?'
                ask = data['ask_price'] if data['ask_price'] not in [None, 'N/A', ''] else '?'
                
                # Format volume
                vol_display = format_volume(data['volume'])
                
                msg = (
                    f"📊 *{formatted_symbol}*\n"
                    f"💰 *Giá:* `{format_price(price)}`\n"
                    f"💵 *Bid/Ask:* `{format_price(bid)}` / `{format_price(ask)}`\n"
                    f"📦 *Volume:* `{vol_display}`\n"
                    f"🕐 `{data['timestamp']}`"
                )
                responses.append(msg)
                found = True
                break
            time.sleep(0.5)
        
        if not found:
            responses.append(f"❌ *{formatted_symbol}*: Không lấy được giá")
    
    for response in responses:
        await update.message.reply_text(response, parse_mode='Markdown')

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /subscribe btc")
        return
    
    symbol = context.args[0].upper()
    formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
    
    if user_id not in user_subscriptions:
        user_subscriptions[user_id] = []
    
    if formatted_symbol not in user_subscriptions[user_id]:
        user_subscriptions[user_id].append(formatted_symbol)
        bybit_ws.subscribe_ticker(symbol)
        await update.message.reply_text(f"✅ Đã theo dõi *{formatted_symbol}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo dõi *{formatted_symbol}*", parse_mode='Markdown')

async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /unsubscribe btc")
        return
    
    symbol = context.args[0].upper()
    formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
    
    if user_id in user_subscriptions and formatted_symbol in user_subscriptions[user_id]:
        user_subscriptions[user_id].remove(formatted_symbol)
        await update.message.reply_text(f"✅ Đã hủy *{formatted_symbol}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"❌ Không theo dõi *{formatted_symbol}*", parse_mode='Markdown')

async def mylist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_subscriptions and user_subscriptions[user_id]:
        msg = "📋 *Danh sách theo dõi:*\n\n"
        for symbol in sorted(user_subscriptions[user_id]):
            if symbol in price_data:
                price = price_data[symbol]['last_price']
                if price in [None, 'N/A', '']:
                    price = price_data[symbol]['bid_price']
                if price in [None, 'N/A', '']:
                    price = price_data[symbol]['ask_price']
                msg += f"• *{symbol}*: `{format_price(price)}`\n"
            else:
                msg += f"• *{symbol}*: `Đang cập nhật...`\n"
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text("📭 Chưa theo dõi coin nào!")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"📡 *Trạng thái*\n\n"
        f"• WebSocket: {'🟢 ONLINE' if bybit_ws.connected else '🔴 OFFLINE'}\n"
        f"• Subscribed: `{len(subscribed_symbols)} coins`\n"
        f"• Users: `{len(user_subscriptions)}`\n"
        f"• Data: `{len(price_data)} coins`",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 *Hướng dẫn*\n\n"
        "• /price btc - Check giá\n"
        "• /subscribe btc - Theo dõi\n"
        "• /unsubscribe btc - Hủy\n"
        "• /mylist - Danh sách\n"
        "• /status - Trạng thái\n\n"
        "⚡ Nguồn: Bybit",
        parse_mode='Markdown'
    )

def auto_update_worker(app):
    while True:
        time.sleep(60)
        for user_id, symbols in user_subscriptions.items():
            updates = []
            for symbol in symbols:
                if symbol in price_data:
                    price = price_data[symbol]['last_price']
                    if price in [None, 'N/A', '']:
                        price = price_data[symbol]['bid_price']
                    if price in [None, 'N/A', '']:
                        price = price_data[symbol]['ask_price']
                    if price not in [None, 'N/A', '']:
                        updates.append(f"• *{symbol}*: `{format_price(price)}`")
            if updates:
                try:
                    app.bot.send_message(
                        chat_id=user_id,
                        text="🔄 *Cập nhật giá:*\n\n" + "\n".join(updates),
                        parse_mode='Markdown'
                    )
                except:
                    pass

def main():
    if not TELEGRAM_TOKEN:
        print("❌ Lỗi: Chưa có TELEGRAM_TOKEN")
        return
    
    print("=" * 50)
    print("🤖 BYBIT CRYPTO PRICE BOT")
    print("=" * 50)
    
    # Start health check server
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Create application
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("price", price_command))
    app.add_handler(CommandHandler("subscribe", subscribe_command))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    app.add_handler(CommandHandler("mylist", mylist_command))
    app.add_handler(CommandHandler("status", status_command))
    
    # Start auto update
    update_thread = threading.Thread(target=auto_update_worker, args=(app,), daemon=True)
    update_thread.start()
    
    print("🚀 Bot is starting...")
    app.run_polling()

if __name__ == '__main__':
    main()

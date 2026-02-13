import os
import json
import threading
import time
from datetime import datetime
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
price_history = {}  # Lưu lịch sử giá để tính high/low

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
        # Subscribe to default symbols
        for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
            self.subscribe_ticker(symbol.replace('USDT', ''))
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            if 'topic' in data and 'tickers' in data['topic']:
                if 'data' in data:
                    ticker_data = data['data']
                    symbol = ticker_data['symbol']
                    
                    # Lấy giá - ưu tiên lastPrice, nếu không có thì dùng bid1Price
                    last_price = ticker_data.get('lastPrice')
                    if last_price in [None, 'N/A', '']:
                        last_price = ticker_data.get('bid1Price', 'N/A')
                    
                    # Lấy volume - thử nhiều field khác nhau
                    volume = ticker_data.get('volume24h')
                    if volume in [None, 'N/A', '']:
                        volume = ticker_data.get('turnover24h', 'N/A')
                    if volume in [None, 'N/A', '']:
                        volume = ticker_data.get('volume', 'N/A')
                    
                    # Lấy bid/ask
                    bid = ticker_data.get('bid1Price', 'N/A')
                    ask = ticker_data.get('ask1Price', 'N/A')
                    
                    # Cập nhật lịch sử giá để tính high/low
                    if symbol not in price_history:
                        price_history[symbol] = []
                    
                    if last_price != 'N/A':
                        price_history[symbol].append(float(last_price))
                        # Giữ 100 giá gần nhất
                        if len(price_history[symbol]) > 100:
                            price_history[symbol].pop(0)
                    
                    # Tính high/low từ lịch sử
                    high = 'N/A'
                    low = 'N/A'
                    if price_history[symbol]:
                        high = f"{max(price_history[symbol]):.2f}"
                        low = f"{min(price_history[symbol]):.2f}"
                    
                    price_data[symbol] = {
                        'last_price': last_price,
                        'bid_price': bid,
                        'ask_price': ask,
                        'volume': volume,
                        'high': high,
                        'low': low,
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    }
                    
                    # Log khi có update
                    if last_price != 'N/A':
                        vol_display = volume if volume != 'N/A' else 'N/A'
                        print(f"📊 {symbol}: ${last_price} | Vol: {vol_display}")
                    
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
    """Format giá tiền"""
    if price in [None, 'N/A', '']:
        return 'N/A'
    try:
        return f"${float(price):,.2f}"
    except:
        return f"${price}"

def format_volume(volume):
    """Format volume - hiển thị dạng K/M/B"""
    if volume in [None, 'N/A', '']:
        return 'N/A'
    try:
        vol = float(volume)
        if vol > 1_000_000_000:  # Tỷ
            return f"{vol/1_000_000_000:.2f}B"
        elif vol > 1_000_000:  # Triệu
            return f"{vol/1_000_000:.2f}M"
        elif vol > 1_000:  # Nghìn
            return f"{vol/1_000:.2f}K"
        else:
            return f"{vol:.2f}"
    except:
        return str(volume)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *Bybit Crypto Price Bot*\n\n"
        "💰 *Lệnh check giá:*\n"
        "• /price btc - Bitcoin\n"
        "• /price eth - Ethereum\n"
        "• /price sol - Solana\n"
        "• /price bnb - BNB\n\n"
        "📌 *Lệnh theo dõi:*\n"
        "• /subscribe btc - Theo dõi coin\n"
        "• /unsubscribe btc - Hủy theo dõi\n"
        "• /mylist - Danh sách theo dõi\n\n"
        "ℹ️ *Khác:*\n"
        "• /status - Trạng thái hệ thống\n"
        "• /help - Hướng dẫn chi tiết",
        parse_mode='Markdown'
    )

async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "❌ *Sai cú pháp!*\n\n"
            "📝 *Cách dùng:* `/price [coin]`\n"
            "📌 *Ví dụ:* `/price btc`\n"
            "        `/price eth sol bnb`\n\n"
            "💡 Có thể check nhiều coin cùng lúc",
            parse_mode='Markdown'
        )
        return
    
    responses = []
    
    for arg in context.args:
        symbol = arg.upper()
        formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
        
        # Subscribe nếu chưa có
        if formatted_symbol not in subscribed_symbols:
            bybit_ws.subscribe_ticker(symbol)
            await update.message.reply_text(f"⏳ Đang kết nối đến *{formatted_symbol}*...", parse_mode='Markdown')
            time.sleep(1)
        
        # Thử lấy giá trong 3 giây
        for i in range(6):
            if formatted_symbol in price_data:
                data = price_data[formatted_symbol]
                
                # Lấy giá - ưu tiên last, nếu không thì bid
                price = data['last_price']
                if price in [None, 'N/A', '']:
                    price = data['bid_price']
                if price in [None, 'N/A', '']:
                    price = data['ask_price']
                
                bid = data['bid_price']
                ask = data['ask_price']
                volume = data['volume']
                high = data['high']
                low = data['low']
                
                # Format message
                msg_parts = [f"📊 *{formatted_symbol}*"]
                
                if price not in [None, 'N/A', '']:
                    msg_parts.append(f"\n💰 *Giá:* `{format_price(price)}`")
                
                if high not in [None, 'N/A', ''] and low not in [None, 'N/A', '']:
                    msg_parts.append(f"📈 *Cao/Low:* `{format_price(high)}` / `{format_price(low)}`")
                
                if bid not in [None, 'N/A', ''] and ask not in [None, 'N/A', '']:
                    msg_parts.append(f"💵 *Bid/Ask:* `{format_price(bid)}` / `{format_price(ask)}`")
                
                if volume not in [None, 'N/A', '']:
                    msg_parts.append(f"📦 *Volume 24h:* `{format_volume(volume)}`")
                
                msg_parts.append(f"\n🕐 `{data['timestamp']}`")
                msg_parts.append(f"⚡ Bybit")
                
                responses.append("\n".join(msg_parts))
                break
            time.sleep(0.5)
        else:
            responses.append(f"❌ *{formatted_symbol}*: Không thể lấy giá")
    
    # Gửi response
    if responses:
        # Nếu nhiều coin, gửi riêng từng coin để tránh lỗi Markdown
        if len(responses) > 1:
            for response in responses:
                await update.message.reply_text(response, parse_mode='Markdown')
        else:
            await update.message.reply_text(responses[0], parse_mode='Markdown')

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "❌ *Sai cú pháp!*\n\n"
            "📝 `/subscribe [coin]`\n"
            "📌 Ví dụ: `/subscribe btc`",
            parse_mode='Markdown'
        )
        return
    
    symbol = context.args[0].upper()
    formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
    
    if user_id not in user_subscriptions:
        user_subscriptions[user_id] = []
    
    if formatted_symbol not in user_subscriptions[user_id]:
        user_subscriptions[user_id].append(formatted_symbol)
        bybit_ws.subscribe_ticker(symbol)
        await update.message.reply_text(
            f"✅ Đã thêm *{formatted_symbol}* vào danh sách theo dõi!",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            f"ℹ️ *{formatted_symbol}* đã có trong danh sách theo dõi!",
            parse_mode='Markdown'
        )

async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "❌ *Sai cú pháp!*\n\n"
            "📝 `/unsubscribe [coin]`\n"
            "📌 Ví dụ: `/unsubscribe btc`",
            parse_mode='Markdown'
        )
        return
    
    symbol = context.args[0].upper()
    formatted_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
    
    if user_id in user_subscriptions and formatted_symbol in user_subscriptions[user_id]:
        user_subscriptions[user_id].remove(formatted_symbol)
        await update.message.reply_text(
            f"✅ Đã xóa *{formatted_symbol}* khỏi danh sách theo dõi!",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            f"❌ *{formatted_symbol}* không có trong danh sách theo dõi!",
            parse_mode='Markdown'
        )

async def mylist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id in user_subscriptions and user_subscriptions[user_id]:
        msg = "📋 *DANH SÁCH THEO DÕI*\n\n"
        
        for symbol in sorted(user_subscriptions[user_id]):
            if symbol in price_data:
                price = price_data[symbol]['last_price']
                if price in [None, 'N/A', '']:
                    price = price_data[symbol]['bid_price']
                
                if price not in [None, 'N/A', '']:
                    msg += f"• *{symbol}*: `{format_price(price)}`\n"
                else:
                    msg += f"• *{symbol}*: `Đang cập nhật...`\n"
            else:
                msg += f"• *{symbol}*: `Đang cập nhật...`\n"
        
        msg += f"\n📊 *Tổng số:* {len(user_subscriptions[user_id])} coins"
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text(
            "📭 *Chưa theo dõi coin nào!*\n\n"
            "💡 Dùng `/subscribe [coin]` để bắt đầu theo dõi.",
            parse_mode='Markdown'
        )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Kiểm tra trạng thái hệ thống"""
    # Tính tổng số user đang theo dõi
    active_users = len([u for u in user_subscriptions.keys() if user_subscriptions[u]])
    
    status_msg = f"""
📡 *HỆ THỐNG*

• *WebSocket:* {'🟢 ONLINE' if bybit_ws.connected else '🔴 OFFLINE'}
• *Subscribed:* `{len(subscribed_symbols)} coins`
• *Users:* `{active_users}`
• *Price data:* `{len(price_data)} coins`

📊 *DỮ LIỆU MỚI NHẤT:*
"""
    # Thêm giá mới nhất của các coin phổ biến
    for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
        if symbol in price_data:
            price = price_data[symbol]['last_price']
            if price not in [None, 'N/A', '']:
                status_msg += f"\n• {symbol}: `{format_price(price)}`"
    
    await update.message.reply_text(status_msg, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Hướng dẫn chi tiết"""
    help_text = """
📘 *HƯỚNG DẪN CHI TIẾT*

🔹 *CHECK GIÁ NHANH*
• `/price btc` - Giá Bitcoin
• `/price eth` - Giá Ethereum  
• `/price sol` - Giá Solana
• `/price btc eth sol` - Check nhiều coin

🔹 *THEO DÕI GIÁ*
• `/subscribe btc` - Theo dõi Bitcoin
• `/unsubscribe btc` - Hủy theo dõi
• `/mylist` - Xem danh sách

🔹 *HỆ THỐNG*
• `/status` - Kiểm tra kết nối
• `/help` - Xem hướng dẫn này

💡 *MẸO*
• Bot tự động cập nhật giá mỗi 60s
• Không phân biệt chữ hoa/thường
• Có thể check coin khác như: BNB, XRP, ADA, DOGE, DOT

⚡ *Nguồn dữ liệu:* Bybit (Real-time)
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

def auto_update_worker(app):
    """Thread tự động cập nhật giá cho users"""
    while True:
        try:
            time.sleep(60)
            for user_id, symbols in user_subscriptions.items():
                if not symbols:
                    continue
                
                updates = []
                for symbol in symbols:
                    if symbol in price_data:
                        # Lấy giá
                        price = price_data[symbol]['last_price']
                        if price in [None, 'N/A', '']:
                            price = price_data[symbol]['bid_price']
                        
                        if price not in [None, 'N/A', '']:
                            updates.append(f"• *{symbol}*: `{format_price(price)}`")
                
                if updates:
                    try:
                        app.bot.send_message(
                            chat_id=user_id,
                            text="🔄 *CẬP NHẬT GIÁ MỚI*\n\n" + "\n".join(updates),
                            parse_mode='Markdown'
                        )
                        print(f"📨 Sent update to user {user_id}: {len(updates)} coins")
                    except Exception as e:
                        print(f"❌ Error sending to {user_id}: {e}")
        except Exception as e:
            print(f"❌ Auto update error: {e}")
        time.sleep(60)

def main():
    if not TELEGRAM_TOKEN:
        print("❌ Lỗi: Chưa có TELEGRAM_TOKEN trong file .env")
        print("📝 Tạo file .env và thêm: TELEGRAM_TOKEN=your_token_here")
        return
    
    print("=" * 60)
    print("🤖 BYBIT CRYPTO PRICE BOT")
    print("=" * 60)
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Add command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("price", price_command))
    app.add_handler(CommandHandler("subscribe", subscribe_command))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
    app.add_handler(CommandHandler("mylist", mylist_command))
    app.add_handler(CommandHandler("status", status_command))
    
    # Start auto update thread
    update_thread = threading.Thread(target=auto_update_worker, args=(app,), daemon=True)
    update_thread.start()
    print("⏰ Auto update: Mỗi 60 giây")
    
    # Đợi WebSocket kết nối
    print("📡 Đang kết nối WebSocket...")
    time.sleep(2)
    
    print(f"📡 WebSocket: {'🟢 ONLINE' if bybit_ws.connected else '🟡 CONNECTING...'}")
    print("🚀 Bot đang chạy...")
    print("=" * 60)
    
    app.run_polling()

if __name__ == '__main__':
    main()
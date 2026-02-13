import os
import json
import threading
import time
import requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
COINGECKO_API = "https://api.coingecko.com/api/v3"

price_data = {}
price_cache = {}
last_update = {}
user_subscriptions = {}

# Health check server cho Render
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

# Hàm lấy giá từ CoinGecko
def get_price_from_coingecko(symbol):
    """Lấy giá coin từ CoinGecko API"""
    try:
        # Map symbol sang ID của CoinGecko
        symbol_map = {
            'BTC': 'bitcoin',
            'ETH': 'ethereum',
            'SOL': 'solana',
            'BNB': 'binancecoin',
            'XRP': 'ripple',
            'ADA': 'cardano',
            'DOGE': 'dogecoin',
            'DOT': 'polkadot',
            'AVAX': 'avalanche-2',
            'MATIC': 'matic-network',
            'LINK': 'chainlink',
            'UNI': 'uniswap',
            'ATOM': 'cosmos',
            'LTC': 'litecoin',
            'BCH': 'bitcoin-cash',
            'TRX': 'tron',
            'ETC': 'ethereum-classic',
            'VET': 'vechain',
            'FIL': 'filecoin',
            'ALGO': 'algorand',
            'OP': 'optimism',
            'ARB': 'arbitrum'
        }
        
        clean_symbol = symbol.upper().replace('USDT', '').replace('USD', '')
        
        if clean_symbol not in symbol_map:
            return None
        
        coin_id = symbol_map[clean_symbol]
        
        # Gọi API CoinGecko
        url = f"{COINGECKO_API}/simple/price"
        params = {
            'ids': coin_id,
            'vs_currencies': 'usd',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if coin_id in data:
                coin_data = data[coin_id]
                return {
                    'price': coin_data.get('usd', 'N/A'),
                    'volume': coin_data.get('usd_24h_vol', 'N/A'),
                    'change_24h': coin_data.get('usd_24h_change', 'N/A'),
                    'last_update': datetime.now().strftime('%H:%M:%S')
                }
        return None
        
    except Exception as e:
        print(f"CoinGecko error: {e}")
        return None

def format_price(price):
    if price in [None, 'N/A']:
        return 'N/A'
    try:
        if price < 0.01:
            return f"${price:.6f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:,.2f}"
    except:
        return f"${price}"

def format_volume(volume):
    if volume in [None, 'N/A']:
        return 'N/A'
    try:
        vol = float(volume)
        if vol > 1_000_000_000:
            return f"${vol/1_000_000_000:.2f}B"
        elif vol > 1_000_000:
            return f"${vol/1_000_000:.2f}M"
        elif vol > 1_000:
            return f"${vol/1_000:.2f}K"
        else:
            return f"${vol:,.2f}"
    except:
        return str(volume)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *CoinGecko Price Bot*\n\n"
        "💰 /price btc - Giá Bitcoin\n"
        "💰 /price eth - Giá Ethereum\n"
        "💰 /price sol - Giá Solana\n"
        "📌 /subscribe btc - Theo dõi\n"
        "❌ /unsubscribe btc - Hủy\n"
        "📋 /mylist - Danh sách\n"
        "📊 /status - Trạng thái\n"
        "🆘 /help - Hướng dẫn\n\n"
        "⚡ Dữ liệu từ CoinGecko",
        parse_mode='Markdown'
    )

async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /price btc eth sol")
        return
    
    responses = []
    for arg in context.args:
        symbol = arg.upper()
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        
        # Gửi typing indicator
        await update.message.chat.send_action(action='typing')
        
        # Lấy giá
        data = get_price_from_coingecko(clean_symbol)
        
        if data:
            msg = (
                f"📊 *{clean_symbol}/USD*\n"
                f"💰 *Giá:* `{format_price(data['price'])}`\n"
                f"📈 *24h Change:* `{data['change_24h']:.2f}%`\n"
                f"📦 *Volume:* `{format_volume(data['volume'])}`\n"
                f"🕐 `{data['last_update']}`\n"
                f"⚡ CoinGecko"
            )
            responses.append(msg)
            
            # Cache lại cho subscribe
            price_cache[clean_symbol] = {
                'price': data['price'],
                'volume': data['volume'],
                'change': data['change_24h'],
                'time': datetime.now()
            }
        else:
            responses.append(f"❌ *{clean_symbol}*: Không tìm thấy dữ liệu")
    
    for response in responses:
        await update.message.reply_text(response, parse_mode='Markdown')

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /subscribe btc")
        return
    
    symbol = context.args[0].upper().replace('USDT', '').replace('USD', '')
    
    if user_id not in user_subscriptions:
        user_subscriptions[user_id] = []
    
    if symbol not in user_subscriptions[user_id]:
        user_subscriptions[user_id].append(symbol)
        await update.message.reply_text(f"✅ Đã theo dõi *{symbol}/USD*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo dõi *{symbol}* rồi!", parse_mode='Markdown')

async def unsubscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /unsubscribe btc")
        return
    
    symbol = context.args[0].upper().replace('USDT', '').replace('USD', '')
    
    if user_id in user_subscriptions and symbol in user_subscriptions[user_id]:
        user_subscriptions[user_id].remove(symbol)
        await update.message.reply_text(f"✅ Đã hủy *{symbol}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"❌ Không theo dõi *{symbol}*", parse_mode='Markdown')

async def mylist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_subscriptions and user_subscriptions[user_id]:
        msg = "📋 *Danh sách theo dõi:*\n\n"
        for symbol in sorted(user_subscriptions[user_id]):
            if symbol in price_cache:
                price = price_cache[symbol]['price']
                msg += f"• *{symbol}*: `{format_price(price)}`\n"
            else:
                msg += f"• *{symbol}*: `Đang cập nhật...`\n"
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text("📭 Chưa theo dõi coin nào!")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"📡 *Trạng thái*\n\n"
        f"• API: CoinGecko 🟢\n"
        f"• Users: `{len(user_subscriptions)}`\n"
        f"• Cache: `{len(price_cache)} coins`\n"
        f"• Requests/min: Không giới hạn (free tier)",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📘 *Hướng dẫn chi tiết*

*Lệnh cơ bản:*
• /price btc - Giá Bitcoin
• /price eth - Giá Ethereum
• /price sol - Giá Solana
• /price bnb - Giá BNB

*Theo dõi:*
• /subscribe btc - Theo dõi
• /unsubscribe btc - Hủy
• /mylist - Xem danh sách

*Hỗ trợ:* BTC, ETH, SOL, BNB, XRP, ADA, DOGE, DOT, AVAX, MATIC, LINK, UNI, ATOM, LTC, BCH, TRX, OP, ARB và 10,000+ coin khác

*Nguồn:* CoinGecko (cập nhật mỗi 60s)
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

def auto_update_worker(app):
    """Thread tự động cập nhật giá cho users"""
    while True:
        time.sleep(60)
        for user_id, symbols in user_subscriptions.items():
            updates = []
            for symbol in symbols:
                # Lấy giá mới
                data = get_price_from_coingecko(symbol)
                if data:
                    price_cache[symbol] = {
                        'price': data['price'],
                        'volume': data['volume'],
                        'change': data['change_24h'],
                        'time': datetime.now()
                    }
                    updates.append(f"• *{symbol}*: `{format_price(data['price'])}` ({data['change_24h']:.1f}%)")
            
            if updates:
                try:
                    app.bot.send_message(
                        chat_id=user_id,
                        text="🔄 *Cập nhật giá mới:*\n\n" + "\n".join(updates),
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    print(f"Send error: {e}")

def main():
    if not TELEGRAM_TOKEN:
        print("❌ Lỗi: Chưa có TELEGRAM_TOKEN")
        return
    
    print("=" * 50)
    print("🤖 COINGECKO PRICE BOT")
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
    print("⏰ Auto update: 60 giây")
    
    print("🚀 Bot đang chạy...")
    app.run_polling()

if __name__ == '__main__':
    main()

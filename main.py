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
CMC_API_KEY = os.getenv('CMC_API_KEY')  # Thêm API key vào .env
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

price_data = {}
price_cache = {}
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

# Hàm lấy giá từ CoinMarketCap
def get_price_from_cmc(symbol):
    """Lấy giá coin từ CoinMarketCap API"""
    try:
        if not CMC_API_KEY:
            print("❌ Thiếu CMC_API_KEY")
            return None
        
        clean_symbol = symbol.upper().replace('USDT', '').replace('USD', '')
        
        # Gọi API CoinMarketCap
        url = f"{CMC_API_URL}/cryptocurrency/quotes/latest"
        headers = {
            'X-CMC_PRO_API_KEY': CMC_API_KEY,
            'Accept': 'application/json'
        }
        params = {
            'symbol': clean_symbol,
            'convert': 'USD'
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and clean_symbol in data['data']:
                coin_data = data['data'][clean_symbol]
                quote = coin_data['quote']['USD']
                
                return {
                    'price': quote.get('price', 'N/A'),
                    'volume': quote.get('volume_24h', 'N/A'),
                    'change_24h': quote.get('percent_change_24h', 'N/A'),
                    'market_cap': quote.get('market_cap', 'N/A'),
                    'rank': coin_data.get('cmc_rank', 'N/A'),
                    'name': coin_data.get('name', clean_symbol),
                    'last_update': datetime.now().strftime('%H:%M:%S')
                }
            else:
                print(f"❌ Không tìm thấy {clean_symbol} trong response")
                return None
                
        elif response.status_code == 400:
            print(f"❌ Lỗi 400: Symbol không hợp lệ - {clean_symbol}")
            return None
        elif response.status_code == 401:
            print("❌ Lỗi 401: API Key không hợp lệ")
            return None
        else:
            print(f"❌ Lỗi {response.status_code}: {response.text}")
            return None
        
    except Exception as e:
        print(f"CMC API error: {e}")
        return None

def format_price(price):
    if price in [None, 'N/A']:
        return 'N/A'
    try:
        price = float(price)
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
        "🚀 *CoinMarketCap Price Bot*\n\n"
        "💰 /price btc - Giá Bitcoin\n"
        "💰 /price eth - Giá Ethereum\n"
        "💰 /price sol - Giá Solana\n"
        "📌 /subscribe btc - Theo dõi\n"
        "❌ /unsubscribe btc - Hủy\n"
        "📋 /mylist - Danh sách\n"
        "📊 /status - Trạng thái\n"
        "🆘 /help - Hướng dẫn\n\n"
        "⚡ Dữ liệu từ CoinMarketCap",
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
        
        # Lấy giá từ CMC
        data = get_price_from_cmc(clean_symbol)
        
        if data:
            msg = (
                f"📊 *{data['name']} ({clean_symbol})*\n"
                f"🏆 *Rank:* #{data['rank']}\n"
                f"💰 *Giá:* `{format_price(data['price'])}`\n"
                f"📈 *24h Change:* `{data['change_24h']:.2f}%`\n"
                f"📦 *Volume:* `{format_volume(data['volume'])}`\n"
                f"💎 *Market Cap:* `{format_volume(data['market_cap'])}`\n"
                f"🕐 `{data['last_update']}`\n"
                f"⚡ CoinMarketCap"
            )
            responses.append(msg)
            
            # Cache lại cho subscribe
            price_cache[clean_symbol] = {
                'price': data['price'],
                'volume': data['volume'],
                'change': data['change_24h'],
                'market_cap': data['market_cap'],
                'rank': data['rank'],
                'name': data['name'],
                'time': datetime.now()
            }
        else:
            responses.append(f"❌ *{clean_symbol}*: Không tìm thấy dữ liệu\n🔍 Kiểm tra lại symbol hoặc API key")
    
    for response in responses:
        await update.message.reply_text(response, parse_mode='Markdown')

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("❌ Ví dụ: /subscribe btc")
        return
    
    symbol = context.args[0].upper().replace('USDT', '').replace('USD', '')
    
    # Kiểm tra xem có tồn tại không
    data = get_price_from_cmc(symbol)
    if not data:
        await update.message.reply_text(f"❌ *{symbol}* không tồn tại hoặc không có dữ liệu", parse_mode='Markdown')
        return
    
    if user_id not in user_subscriptions:
        user_subscriptions[user_id] = []
    
    if symbol not in user_subscriptions[user_id]:
        user_subscriptions[user_id].append(symbol)
        await update.message.reply_text(f"✅ Đã theo dõi *{data['name']} ({symbol})*", parse_mode='Markdown')
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
                cache = price_cache[symbol]
                msg += f"• *{cache['name']}* ({symbol}): `{format_price(cache['price'])}`\n"
            else:
                msg += f"• *{symbol}*: `Đang cập nhật...`\n"
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text("📭 Chưa theo dõi coin nào!")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Kiểm tra API key
    api_status = "🟢 OK" if CMC_API_KEY else "🔴 Missing"
    
    await update.message.reply_text(
        f"📡 *Trạng thái*\n\n"
        f"• API: CoinMarketCap {api_status}\n"
        f"• Users: `{len(user_subscriptions)}`\n"
        f"• Cache: `{len(price_cache)} coins`\n"
        f"• Giới hạn: 10,000 calls/tháng (Free)",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📘 *Hướng dẫn chi tiết*

*Lệnh cơ bản:*
• /price btc - Bitcoin
• /price eth - Ethereum
• /price sol - Solana
• /price bnb - BNB

*Theo dõi:*
• /subscribe btc - Thêm vào danh sách
• /unsubscribe btc - Xóa khỏi danh sách
• /mylist - Xem danh sách

*Hỗ trợ:* 
Hầu hết các coin có trên CoinMarketCap
Dùng symbol chuẩn (BTC, ETH, SOL, BNB, XRP, ADA, v.v.)

*Nguồn:* CoinMarketCap (cập nhật mỗi 60s)
*API Key:* Cần thiết để hoạt động
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
                data = get_price_from_cmc(symbol)
                if data:
                    price_cache[symbol] = {
                        'price': data['price'],
                        'volume': data['volume'],
                        'change': data['change_24h'],
                        'market_cap': data['market_cap'],
                        'rank': data['rank'],
                        'name': data['name'],
                        'time': datetime.now()
                    }
                    updates.append(f"• *{data['name']}*: `{format_price(data['price'])}` ({data['change_24h']:.1f}%)")
            
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
    
    if not CMC_API_KEY:
        print("❌ Lỗi: Chưa có CMC_API_KEY trong file .env")
        print("📝 Thêm dòng: CMC_API_KEY=your_key_here")
        return
    
    print("=" * 50)
    print("🤖 COINMARKETCAP PRICE BOT")
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

import os
import threading
import time
import requests
import json
import sqlite3
import logging
import shutil
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode

# Cấu hình logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

# ==================== CẤU HÌNH DATABASE TRÊN RENDER DISK ====================

# Đường dẫn lưu database - Render Disk được mount tại /data
DATA_DIR = '/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, 'crypto_bot.db')
BACKUP_DIR = os.path.join(DATA_DIR, 'backups')

# Tạo thư mục nếu chưa có
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

logger.info(f"📁 Dữ liệu sẽ được lưu tại: {DB_PATH}")
logger.info(f"💾 Backup sẽ được lưu tại: {BACKUP_DIR}")

# Cache
price_cache = {}
usdt_cache = {'rate': None, 'time': None}

# Biến toàn cục cho bot
app = None

# ==================== HEALTH CHECK SERVER CHO RENDER ====================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"""
        <html>
            <head><title>Crypto Bot</title></head>
            <body>
                <h1>🚀 Crypto Investment Bot is Running!</h1>
                <p>Database: /data/crypto_bot.db</p>
                <p>Time: {}</p>
            </body>
        </html>
        """.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).encode())
    
    def log_message(self, format, *args):
        return  # Tắt log để tránh spam

def run_health_server():
    """Chạy HTTP server cho Render health check"""
    port = int(os.environ.get('PORT', 10000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"✅ Health server running on port {port}")
    server.serve_forever()

# ==================== DATABASE SETUP ====================

def init_database():
    """Khởi tạo database và các bảng"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Bảng theo dõi coin
    c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                 (user_id INTEGER, symbol TEXT, 
                  PRIMARY KEY (user_id, symbol))''')
    
    # Bảng danh mục đầu tư
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  symbol TEXT,
                  amount REAL,
                  buy_price REAL,
                  buy_date TEXT,
                  total_cost REAL)''')
    
    conn.commit()
    conn.close()
    logger.info(f"✅ Database initialized at {DB_PATH}")

def backup_database():
    """Tự động backup database"""
    try:
        if os.path.exists(DB_PATH):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')
            
            # Copy file
            shutil.copy2(DB_PATH, backup_path)
            logger.info(f"✅ Đã backup: {backup_path}")
            
            # Xóa backup cũ hơn 7 ngày
            clean_old_backups()
    except Exception as e:
        logger.error(f"❌ Lỗi backup: {e}")

def clean_old_backups(days=7):
    """Xóa backup cũ"""
    now = time.time()
    for f in os.listdir(BACKUP_DIR):
        if f.startswith('backup_') and f.endswith('.db'):
            filepath = os.path.join(BACKUP_DIR, f)
            if os.path.getmtime(filepath) < now - days * 86400:
                os.remove(filepath)
                logger.info(f"🗑 Đã xóa backup cũ: {f}")

def schedule_backup():
    """Chạy backup mỗi ngày"""
    while True:
        try:
            backup_database()
            time.sleep(86400)  # 24 giờ
        except Exception as e:
            logger.error(f"Lỗi trong schedule_backup: {e}")
            time.sleep(3600)  # Thử lại sau 1 giờ nếu lỗi

# ==================== DATABASE FUNCTIONS ====================

def add_subscription(user_id, symbol):
    """Thêm theo dõi"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO subscriptions (user_id, symbol) VALUES (?, ?)",
                  (user_id, symbol))
        conn.commit()
        success = True
    except sqlite3.IntegrityError:
        success = False
    conn.close()
    return success

def remove_subscription(user_id, symbol):
    """Xóa theo dõi"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM subscriptions WHERE user_id = ? AND symbol = ?",
              (user_id, symbol))
    conn.commit()
    conn.close()

def get_subscriptions(user_id):
    """Lấy danh sách theo dõi"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT symbol FROM subscriptions WHERE user_id = ? ORDER BY symbol",
              (user_id,))
    result = [row[0] for row in c.fetchall()]
    conn.close()
    return result

def add_transaction(user_id, symbol, amount, buy_price):
    """Thêm giao dịch mua"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_cost = amount * buy_price
    
    c.execute('''INSERT INTO portfolio 
                 (user_id, symbol, amount, buy_price, buy_date, total_cost)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (user_id, symbol, amount, buy_price, buy_date, total_cost))
    conn.commit()
    conn.close()

def get_portfolio(user_id):
    """Lấy toàn bộ danh mục"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost 
                 FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
              (user_id,))
    result = c.fetchall()
    conn.close()
    return result

def delete_sold_transactions(user_id, kept_transactions):
    """Xóa các giao dịch đã bán và cập nhật lại"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("DELETE FROM portfolio WHERE user_id = ?", (user_id,))
    
    for tx in kept_transactions:
        c.execute('''INSERT INTO portfolio 
                     (user_id, symbol, amount, buy_price, buy_date, total_cost)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (user_id, tx['symbol'], tx['amount'], tx['buy_price'], 
                   tx['buy_date'], tx['total_cost']))
    
    conn.commit()
    conn.close()

# ==================== HÀM LẤY GIÁ COIN ====================

def get_price(symbol):
    """Lấy giá coin từ CoinMarketCap"""
    try:
        if symbol.upper() == 'USDT':
            clean = 'USDT'
        else:
            clean = symbol.upper().replace('USDT', '').replace('USD', '')
        
        headers = {
            'X-CMC_PRO_API_KEY': CMC_API_KEY,
            'Accept': 'application/json'
        }
        
        params = {
            'symbol': clean,
            'convert': 'USD'
        }
        
        res = requests.get(
            f"{CMC_API_URL}/cryptocurrency/quotes/latest", 
            headers=headers,
            params=params, 
            timeout=10
        )
        
        if res.status_code == 200:
            data = res.json()
            coin_data = data['data'][clean]
            quote_data = coin_data['quote']['USD']
            
            return {
                'p': quote_data['price'], 
                'v': quote_data['volume_24h'], 
                'c': quote_data['percent_change_24h'], 
                'm': quote_data['market_cap'],
                'n': coin_data['name'],
                'r': coin_data.get('cmc_rank', 'N/A')
            }
        else:
            logger.error(f"CMC API error: {res.status_code} - {res.text}")
            return None
            
    except Exception as e:
        logger.error(f"Lỗi get_price {symbol}: {e}")
        return None

# ==================== HÀM LẤY TỶ GIÁ USDT/VND ====================

def get_usdt_vnd_rate():
    """Lấy tỷ giá USDT/VND từ nhiều nguồn"""
    global usdt_cache
    
    if usdt_cache['rate'] and usdt_cache['time']:
        time_diff = (datetime.now() - usdt_cache['time']).total_seconds()
        if time_diff < 180:  # Cache 3 phút
            return usdt_cache['rate']
    
    # Nguồn 1: CoinGecko
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            'ids': 'tether',
            'vs_currencies': 'vnd',
            'include_last_updated_at': 'true'
        }
        res = requests.get(url, params=params, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if 'tether' in data:
                vnd_rate = float(data['tether']['vnd'])
                last_update = data['tether'].get('last_updated_at', int(time.time()))
                
                result = {
                    'source': 'CoinGecko',
                    'vnd': vnd_rate,
                    'update_time': datetime.fromtimestamp(last_update).strftime('%H:%M:%S %d/%m/%Y')
                }
                usdt_cache['rate'] = result
                usdt_cache['time'] = datetime.now()
                return result
    except Exception as e:
        logger.warning(f"CoinGecko error: {e}")
    
    # Nguồn 2: Coinbase
    try:
        url = "https://api.coinbase.com/v2/prices/USDT-VND/spot"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            data = res.json()
            vnd_rate = float(data['data']['amount'])
            
            result = {
                'source': 'Coinbase',
                'vnd': vnd_rate,
                'update_time': datetime.now().strftime('%H:%M:%S %d/%m/%Y')
            }
            usdt_cache['rate'] = result
            usdt_cache['time'] = datetime.now()
            return result
    except Exception as e:
        logger.warning(f"Coinbase error: {e}")
    
    # Fallback
    result = {
        'source': 'Fallback (25000)',
        'vnd': 25000,
        'update_time': datetime.now().strftime('%H:%M:%S %d/%m/%Y')
    }
    usdt_cache['rate'] = result
    usdt_cache['time'] = datetime.now()
    return result

# ==================== HÀM ĐỊNH DẠNG ====================

def fmt_price(p):
    try:
        p = float(p)
        if p < 0.01:
            return f"${p:.6f}"
        elif p < 1:
            return f"${p:.4f}"
        else:
            return f"${p:,.2f}"
    except: 
        return f"${p}"

def fmt_vnd(p):
    try:
        p = float(p)
        return f"₫{p:,.0f}"
    except:
        return f"₫{p}"

def fmt_vol(v):
    try:
        v = float(v)
        if v > 1e9:
            return f"${v/1e9:.2f}B"
        elif v > 1e6:
            return f"${v/1e6:.2f}M"
        elif v > 1e3:
            return f"${v/1e3:.2f}K"
        else:
            return f"${v:,.2f}"
    except: 
        return str(v)

# ==================== KEYBOARD ====================

def get_main_keyboard():
    keyboard = [
        [KeyboardButton("💰 ĐẦU TƯ COIN")],
        [KeyboardButton("❓ HƯỚNG DẪN")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_invest_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
         InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
         InlineKeyboardButton("💵 USDT", callback_data="price_USDT")],
        [InlineKeyboardButton("📊 Top 10", callback_data="show_top10"),
         InlineKeyboardButton("🔔 Theo dõi", callback_data="show_subscribe")],
        [InlineKeyboardButton("📋 DS theo dõi", callback_data="show_mylist"),
         InlineKeyboardButton("💼 Danh mục", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 Lợi nhuận", callback_data="show_profit"),
         InlineKeyboardButton("➕ Mua coin", callback_data="show_buy")],
        [InlineKeyboardButton("➖ Bán coin", callback_data="show_sell")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS ====================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    welcome_msg = (
        "🚀 *ĐẦU TƯ COIN BOT*\n\n"
        "🤖 Bot hỗ trợ:\n"
        "• Xem giá BTC/ETH/USDT (có USDT/VND)\n"
        "• Xem tỷ giá USDT/VND\n"
        "• Top 10 coin\n"
        "• Theo dõi biến động giá\n"
        "• Quản lý danh mục đầu tư (lưu vĩnh viễn trên Render Disk)\n"
        "• Tính lợi nhuận\n\n"
        "👇 *Bấm ĐẦU TƯ COIN để bắt đầu*"
    )
    await update.message.reply_text(
        welcome_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    help_msg = (
        "📘 *HƯỚNG DẪN*\n\n"
        "*Cách dùng:*\n"
        "1️⃣ Bấm *ĐẦU TƯ COIN*\n"
        "2️⃣ Chọn chức năng trong menu\n\n"
        "*Các chức năng:*\n"
        "• BTC/ETH/USDT - Xem giá\n"
        "• Top 10 - Top coin vốn hóa\n"
        "• Theo dõi - Theo dõi giá coin\n"
        "• DS theo dõi - Danh sách đang theo\n"
        "• Danh mục - Xem danh mục đầu tư\n"
        "• Lợi nhuận - Chi tiết lợi nhuận\n"
        "• Mua coin - Thêm giao dịch mua\n"
        "• Bán coin - Bán coin\n\n"
        "*Lệnh nhanh:*\n"
        "/usdt - Xem tỷ giá USDT/VND\n"
        "/s btc - Xem giá BTC\n"
        "/su btc - Theo dõi BTC\n"
        "/uns btc - Hủy theo dõi\n"
        "/my - Xem danh sách theo dõi\n"
        "/buy btc 0.5 40000 - Mua BTC\n"
        "/sell btc 0.2 - Bán BTC\n\n"
        "*Lưu ý:* Dữ liệu được lưu vĩnh viễn trên Render Disk"
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

async def usdt_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Đang tra cứu...")
    
    rate_data = get_usdt_vnd_rate()
    vnd = rate_data['vnd']
    
    text = (
        "💱 *TỶ GIÁ USDT/VND*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"🇺🇸 *1 USDT* = `{fmt_vnd(vnd)}`\n"
        f"🇻🇳 *1,000,000 VND* = `{1000000/vnd:.4f} USDT`\n\n"
        f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
        f"📊 *Nguồn:* `{rate_data['source']}`"
    )
    
    keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
    
    await msg.delete()
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def s_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        return await update.message.reply_text("❌ /s btc eth usdt")
    
    for arg in ctx.args:
        d = get_price(arg)
        if d:
            if arg.upper() == 'USDT':
                rate_data = get_usdt_vnd_rate()
                vnd_price = rate_data['vnd']
                msg = (
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 USD: `{fmt_price(d['p'])}`\n"
                    f"🇻🇳 VND: `{fmt_vnd(vnd_price)}`\n"
                    f"📈 24h: `{d['c']:.2f}%`\n"
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`"
                )
            else:
                msg = (
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 Giá: `{fmt_price(d['p'])}`\n"
                    f"📈 24h: `{d['c']:.2f}%`\n"
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`"
                )
            price_cache[arg.upper()] = d
        else:
            msg = f"❌ *{arg.upper()}*: Không có dữ liệu"
        await update.message.reply_text(msg, parse_mode='Markdown')

async def su_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /su btc")
    
    s = ctx.args[0].upper()
    if s not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    if not get_price(s): 
        return await update.message.reply_text(f"❌ *{s}* không tồn tại", parse_mode='Markdown')
    
    if add_subscription(uid, s):
        await update.message.reply_text(f"✅ Đã theo dõi *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo dõi *{s}* rồi", parse_mode='Markdown')

async def uns_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /uns btc")
    
    s = ctx.args[0].upper()
    remove_subscription(uid, s)
    await update.message.reply_text(f"✅ Đã hủy theo dõi *{s}*", parse_mode='Markdown')

async def my_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    subs = get_subscriptions(uid)
    
    if subs:
        msg = "📋 *DANH SÁCH THEO DÕI:*\n"
        for s in sorted(subs):
            c = price_cache.get(s, {})
            current_price = fmt_price(c.get('p', '?'))
            msg += f"• *{s}*: `{current_price}`\n"
        
        # Thêm hướng dẫn hủy
        msg += "\n*Để hủy theo dõi:*\n/uns [coin]"
        
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text(
            "📭 Chưa theo dõi coin nào!\nDùng /su btc để bắt đầu theo dõi",
            parse_mode='Markdown'
        )

async def buy_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(ctx.args) < 3:
        return await update.message.reply_text(
            "❌ /buy btc 0.5 40000\n"
            "Ý nghĩa: /buy [coin] [số lượng] [giá mua]"
        )
    
    symbol = ctx.args[0].upper()
    if symbol not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    try:
        amount = float(ctx.args[1])
        buy_price = float(ctx.args[2])
    except ValueError:
        return await update.message.reply_text("❌ Số lượng hoặc giá không hợp lệ!")
    
    if amount <= 0 or buy_price <= 0:
        return await update.message.reply_text("❌ Số lượng và giá phải lớn hơn 0")
    
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Không thể lấy giá *{symbol}*", parse_mode='Markdown')
    
    add_transaction(uid, symbol, amount, buy_price)
    
    current_price = price_data['p']
    profit_loss = (current_price - buy_price) * amount
    profit_loss_percent = ((current_price - buy_price) / buy_price) * 100
    
    msg = (
        f"✅ *ĐÃ MUA {symbol}*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"📊 Số lượng: `{amount:.4f}`\n"
        f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
        f"💵 Tổng vốn: `{fmt_price(amount * buy_price)}`\n"
        f"📈 Giá hiện tại: `{fmt_price(current_price)}`\n"
        f"{'✅' if profit_loss>=0 else '❌'} Lợi nhuận: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def sell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        return await update.message.reply_text("❌ /sell btc 0.2")
    
    symbol = ctx.args[0].upper()
    if symbol not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    try:
        sell_amount = float(ctx.args[1])
    except ValueError:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    if sell_amount <= 0:
        return await update.message.reply_text("❌ Số lượng phải lớn hơn 0")
    
    portfolio_data = get_portfolio(uid)
    if not portfolio_data:
        return await update.message.reply_text("📭 Danh mục đầu tư trống!")
    
    portfolio = []
    for row in portfolio_data:
        portfolio.append({
            'symbol': row[0],
            'amount': row[1],
            'buy_price': row[2],
            'buy_date': row[3],
            'total_cost': row[4]
        })
    
    symbol_txs = [tx for tx in portfolio if tx['symbol'] == symbol]
    if not symbol_txs:
        return await update.message.reply_text(f"❌ Không có *{symbol}* trong danh mục", parse_mode='Markdown')
    
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(f"❌ Chỉ có {total_amount:.4f} {symbol} trong danh mục")
    
    # Lấy giá hiện tại
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Không thể lấy giá *{symbol}*", parse_mode='Markdown')
    
    current_price = price_data['p']
    
    remaining_sell = sell_amount
    new_portfolio = []
    sold_value = 0
    sold_cost = 0
    
    for tx in portfolio:
        if tx['symbol'] == symbol and remaining_sell > 0:
            if tx['amount'] <= remaining_sell:
                sold_cost += tx['total_cost']
                sold_value += tx['amount'] * current_price
                remaining_sell -= tx['amount']
            else:
                sell_part = remaining_sell
                sold_cost += sell_part * tx['buy_price']
                sold_value += sell_part * current_price
                tx['amount'] -= sell_part
                tx['total_cost'] = tx['amount'] * tx['buy_price']
                new_portfolio.append(tx)
                remaining_sell = 0
        else:
            new_portfolio.append(tx)
    
    delete_sold_transactions(uid, new_portfolio)
    
    profit = sold_value - sold_cost
    profit_percent = (profit / sold_cost) * 100 if sold_cost > 0 else 0
    
    msg = (
        f"✅ *ĐÃ BÁN {sell_amount:.4f} {symbol}*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"💰 Giá bán: `{fmt_price(current_price)}`\n"
        f"💵 Giá trị bán: `{fmt_price(sold_value)}`\n"
        f"📊 Vốn gốc: `{fmt_price(sold_cost)}`\n"
        f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

# ==================== HANDLE MESSAGE ====================

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text(
            "💰 *MENU ĐẦU TƯ COIN*\nChọn chức năng bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_menu_keyboard()
        )
    elif text == "❓ HƯỚNG DẪN":
        await help_command(update, ctx)

# ==================== HANDLE CALLBACK ====================

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "back_to_invest":
        await query.edit_message_text(
            "💰 *MENU ĐẦU TƯ COIN*\nChọn chức năng bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_menu_keyboard()
        )
    
    elif data == "refresh_usdt":
        rate_data = get_usdt_vnd_rate()
        vnd = rate_data['vnd']
        
        text = (
            "💱 *TỶ GIÁ USDT/VND*\n"
            "━━━━━━━━━━━━━━━━\n\n"
            f"🇺🇸 *1 USDT* = `{fmt_vnd(vnd)}`\n"
            f"🇻🇳 *1,000,000 VND* = `{1000000/vnd:.4f} USDT`\n\n"
            f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
            f"📊 *Nguồn:* `{rate_data['source']}`"
        )
        
        keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                    [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("price_"):
        symbol = data.replace("price_", "")
        d = get_price(symbol)
        
        if d:
            if symbol == 'USDT':
                rate_data = get_usdt_vnd_rate()
                vnd_price = rate_data['vnd']
                
                msg = (
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 USD: `{fmt_price(d['p'])}`\n"
                    f"🇻🇳 VND: `{fmt_vnd(vnd_price)}`\n"
                    f"📈 24h: `{d['c']:.2f}%`\n"
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`"
                )
            else:
                msg = (
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 Giá: `{fmt_price(d['p'])}`\n"
                    f"📈 24h: `{d['c']:.2f}%`\n"
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`"
                )
            price_cache[symbol] = d
        else:
            msg = f"❌ *{symbol}*: Không có dữ liệu"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_subscribe":
        await query.edit_message_text(
            "🔔 *THEO DÕI COIN*\n\n"
            "Dùng lệnh:\n"
            "/su btc - Theo dõi BTC\n"
            "/su eth - Theo dõi ETH\n"
            "/su usdt - Theo dõi USDT\n\n"
            "Hoặc bấm nút bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Theo BTC", callback_data="sub_BTC"),
                 InlineKeyboardButton("➕ Theo ETH", callback_data="sub_ETH")],
                [InlineKeyboardButton("➕ Theo USDT", callback_data="sub_USDT")],
                [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]
            ])
        )
    
    elif data.startswith("sub_"):
        symbol = data.replace("sub_", "")
        uid = query.from_user.id
        
        if add_subscription(uid, symbol):
            msg = f"✅ Đã theo dõi *{symbol}*"
        else:
            msg = f"ℹ️ Đang theo dõi *{symbol}* rồi"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_mylist":
        uid = query.from_user.id
        subs = get_subscriptions(uid)
        
        if subs:
            msg = "📋 *DANH SÁCH THEO DÕI*\n━━━━━━━━━━━━\n\n"
            for s in sorted(subs):
                c = price_cache.get(s, {})
                current_price = fmt_price(c.get('p', '?'))
                msg += f"• *{s}*: `{current_price}`\n"
            
            msg += "\n*Để hủy:* dùng lệnh /uns [coin]"
            
            keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
            await query.edit_message_text(
                msg,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await query.edit_message_text(
                "📭 Chưa theo dõi coin nào!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
    
    elif data == "show_portfolio":
        uid = query.from_user.id
        portfolio_data = get_portfolio(uid)
        
        if not portfolio_data:
            await query.edit_message_text(
                "📭 Danh mục đầu tư trống!\nDùng /buy để thêm giao dịch",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
            return
        
        portfolio_summary = {}
        total_investment = 0
        total_current_value = 0
        
        for row in portfolio_data:
            symbol = row[0]
            amount = row[1]
            cost = row[4]
            
            if symbol not in portfolio_summary:
                portfolio_summary[symbol] = {'amount': 0, 'cost': 0}
            portfolio_summary[symbol]['amount'] += amount
            portfolio_summary[symbol]['cost'] += cost
        
        msg = "📊 *DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━\n\n"
        
        for symbol, data in portfolio_summary.items():
            price_data = get_price(symbol)
            if price_data:
                current_value = data['amount'] * price_data['p']
                profit = current_value - data['cost']
                profit_percent = (profit / data['cost']) * 100 if data['cost'] > 0 else 0
                
                total_investment += data['cost']
                total_current_value += current_value
                
                avg_price = data['cost'] / data['amount']
                
                msg += f"*{symbol}*\n"
                msg += f"📊 SL: `{data['amount']:.4f}`\n"
                msg += f"💰 TB: `{fmt_price(avg_price)}`\n"
                msg += f"💎 TT: `{fmt_price(current_value)}`\n"
                msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
        
        total_profit = total_current_value - total_investment
        total_profit_percent = (total_profit / total_investment) * 100 if total_investment > 0 else 0
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Tổng vốn: `{fmt_price(total_investment)}`\n"
        msg += f"💰 Giá trị: `{fmt_price(total_current_value)}`\n"
        msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_profit":
        uid = query.from_user.id
        portfolio_data = get_portfolio(uid)
        
        if not portfolio_data:
            await query.edit_message_text(
                "📭 Danh mục đầu tư trống!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
            return
        
        msg = "📈 *CHI TIẾT LỢI NHUẬN*\n━━━━━━━━━━━━\n\n"
        
        total_investment = 0
        total_current_value = 0
        
        for i, row in enumerate(portfolio_data, 1):
            symbol = row[0]
            amount = row[1]
            buy_price = row[2]
            buy_date = row[3]
            cost = row[4]
            
            price_data = get_price(symbol)
            
            if price_data:
                current_value = amount * price_data['p']
                profit = current_value - cost
                profit_percent = (profit / cost) * 100
                
                total_investment += cost
                total_current_value += current_value
                
                msg += f"*GD #{i}: {symbol}*\n"
                msg += f"📅 {buy_date}\n"
                msg += f"📊 SL: `{amount:.4f}`\n"
                msg += f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
                msg += f"💎 Giá trị: `{fmt_price(current_value)}`\n"
                msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
        
        total_profit = total_current_value - total_investment
        total_profit_percent = (total_profit / total_investment) * 100 if total_investment > 0 else 0
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Tổng vốn: `{fmt_price(total_investment)}`\n"
        msg += f"💰 Tổng giá trị: `{fmt_price(total_current_value)}`\n"
        msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_buy":
        await query.edit_message_text(
            "➕ *MUA COIN*\n\n"
            "Dùng lệnh: `/buy [coin] [số lượng] [giá]`\n\n"
            "*Ví dụ:*\n"
            "• `/buy btc 0.5 40000` - Mua 0.5 BTC giá $40,000\n"
            "• `/buy eth 5 2500` - Mua 5 ETH giá $2,500\n"
            "• `/buy usdt 1000 1.00` - Mua 1000 USDT giá $1.00\n\n"
            "*Lưu ý:* Chỉ hỗ trợ BTC, ETH, USDT",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_sell":
        await query.edit_message_text(
            "➖ *BÁN COIN*\n\n"
            "Dùng lệnh: `/sell [coin] [số lượng]`\n\n"
            "*Ví dụ:*\n"
            "• `/sell btc 0.2` - Bán 0.2 BTC\n"
            "• `/sell eth 2` - Bán 2 ETH\n"
            "• `/sell usdt 500` - Bán 500 USDT\n\n"
            "*Lưu ý:*\n"
            "• Chỉ hỗ trợ BTC, ETH, USDT\n"
            "• Hệ thống tự động bán theo FIFO (vào trước bán trước)",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_top10":
        await query.edit_message_text("🔄 Đang tải dữ liệu...")
        
        try:
            headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
            params = {'limit': 10, 'convert': 'USD'}
            
            res = requests.get(
                f"{CMC_API_URL}/cryptocurrency/listings/latest",
                headers=headers,
                params=params,
                timeout=10
            )
            
            if res.status_code == 200:
                data = res.json()['data']
                msg = "📊 *TOP 10 COIN*\n━━━━━━━━━━━━\n\n"
                
                for i, coin in enumerate(data, 1):
                    quote = coin['quote']['USD']
                    change = quote['percent_change_24h']
                    emoji = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                    
                    msg += (
                        f"{i}. *{coin['symbol']}* - {coin['name']}\n"
                        f"   💰 `{fmt_price(quote['price'])}`\n"
                        f"   {emoji} `{change:+.2f}%`\n"
                    )
            else:
                msg = "❌ Không thể lấy dữ liệu từ CoinMarketCap"
        except Exception as e:
            logger.error(f"Lỗi top10: {e}")
            msg = "❌ Lỗi kết nối đến CoinMarketCap"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ==================== AUTO UPDATE ====================

def auto_update():
    """Tự động cập nhật giá cho người theo dõi"""
    global app
    
    while True:
        try:
            time.sleep(60)  # Cập nhật mỗi phút
            
            # Lấy danh sách user theo dõi
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT DISTINCT user_id FROM subscriptions")
            users = c.fetchall()
            conn.close()
            
            for (uid,) in users:
                try:
                    subs = get_subscriptions(uid)
                    if not subs:
                        continue
                    
                    updates = []
                    for s in subs:
                        d = get_price(s)
                        if d:
                            price_cache[s] = d
                            change = d['c']
                            emoji = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                            updates.append(f"• *{d['n']}*: `{fmt_price(d['p'])}` {emoji} `{change:+.1f}%`")
                    
                    if updates and app:
                        try:
                            msg = "🔄 *CẬP NHẬT GIÁ*\n" + "\n".join(updates)
                            app.bot.send_message(
                                uid, 
                                msg, 
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            logger.error(f"Không thể gửi tin nhắn cho user {uid}: {e}")
                            
                except Exception as e:
                    logger.error(f"Lỗi xử lý user {uid}: {e}")
                    
        except Exception as e:
            logger.error(f"Lỗi auto_update: {e}")
            time.sleep(10)  # Đợi 10 giây nếu lỗi

# ==================== MAIN ====================

if __name__ == '__main__':
    global app
    
    # Kiểm tra token
    if not TELEGRAM_TOKEN:
        logger.error("❌ Thiếu TELEGRAM_TOKEN trong file .env")
        exit(1)
    
    if not CMC_API_KEY:
        logger.warning("⚠️ Cảnh báo: Thiếu CMC_API_KEY - Chức năng lấy giá coin sẽ không hoạt động")
    
    # Khởi tạo database
    try:
        init_database()
        
        # Kiểm tra quyền ghi trên disk
        test_file = os.path.join(DATA_DIR, 'test_write.txt')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        logger.info("✅ Disk có quyền ghi")
        
    except Exception as e:
        logger.error(f"❌ Lỗi khởi tạo database: {e}")
        exit(1)
    
    # Thông báo
    logger.info("🚀 Khởi động bot ĐẦU TƯ COIN...")
    logger.info(f"💾 Database: {DB_PATH}")
    logger.info(f"📂 Backup: {BACKUP_DIR}")
    
    if os.path.exists('/data'):
        logger.info("✅ Đang sử dụng Render Disk")
        # Kiểm tra dung lượng
        try:
            stat = os.statvfs('/data')
            free_gb = (stat.f_frsize * stat.f_bavail) / (1024**3)
            logger.info(f"💿 Dung lượng trống: {free_gb:.2f} GB")
        except:
            pass
    else:
        logger.warning("⚠️ Đang chạy local (không dùng Render Disk)")
    
    # Khởi tạo bot
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("usdt", usdt_command))
    app.add_handler(CommandHandler("s", s_command))
    app.add_handler(CommandHandler("su", su_command))
    app.add_handler(CommandHandler("uns", uns_command))
    app.add_handler(CommandHandler("my", my_command))
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("sell", sell_command))
    
    # Message handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Callback handler
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Khởi chạy các thread
    threading.Thread(target=auto_update, daemon=True).start()
    threading.Thread(target=schedule_backup, daemon=True).start()
    threading.Thread(target=run_health_server, daemon=True).start()
    
    logger.info("✅ Bot đã sẵn sàng!")
    logger.info("💰 Bấm 'ĐẦU TƯ COIN' để xem menu đầy đủ")
    
    # Chạy bot
    try:
        app.run_polling()
    except Exception as e:
        logger.error(f"❌ Lỗi khi chạy bot: {e}")
        exit(1)

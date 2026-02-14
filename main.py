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
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        response = f"Crypto Bot Running - {current_time}"
        self.wfile.write(response.encode('utf-8'))
    
    def log_message(self, format, *args):
        return

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
                  added_date TEXT,
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
        added_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        symbol_upper = symbol.upper()
        c.execute("INSERT INTO subscriptions (user_id, symbol, added_date) VALUES (?, ?, ?)",
                  (user_id, symbol_upper, added_date))
        conn.commit()
        logger.info(f"✅ User {user_id} đã thêm {symbol_upper}")
        success = True
    except sqlite3.IntegrityError as e:
        logger.warning(f"⚠️ User {user_id} đã có {symbol.upper()}: {e}")
        success = False
    conn.close()
    return success

def remove_subscription(user_id, symbol):
    """Xóa theo dõi"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM subscriptions WHERE user_id = ? AND symbol = ?",
              (user_id, symbol.upper()))
    conn.commit()
    affected = c.rowcount
    conn.close()
    logger.info(f"🗑 User {user_id} đã xóa {symbol.upper()}, affected: {affected}")
    return affected > 0

def get_subscriptions(user_id):
    """Lấy danh sách theo dõi"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT symbol FROM subscriptions WHERE user_id = ? ORDER BY symbol",
              (user_id,))
    result = [row[0] for row in c.fetchall()]
    conn.close()
    # Đảm bảo tất cả đều là chữ hoa
    return [s.upper() for s in result]

def add_transaction(user_id, symbol, amount, buy_price):
    """Thêm giao dịch mua"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_cost = amount * buy_price
    
    c.execute('''INSERT INTO portfolio 
                 (user_id, symbol, amount, buy_price, buy_date, total_cost)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (user_id, symbol.upper(), amount, buy_price, buy_date, total_cost))
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

def get_transaction_detail(user_id):
    """Lấy chi tiết từng giao dịch kèm ID"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost 
                 FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
              (user_id,))
    result = c.fetchall()
    conn.close()
    return result

def update_transaction(transaction_id, user_id, new_amount, new_price):
    """Cập nhật thông tin giao dịch"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''SELECT symbol, amount, buy_price, total_cost 
                 FROM portfolio WHERE id = ? AND user_id = ?''',
              (transaction_id, user_id))
    old_tx = c.fetchone()
    
    if not old_tx:
        conn.close()
        return False
    
    new_total = new_amount * new_price
    
    c.execute('''UPDATE portfolio 
                 SET amount = ?, buy_price = ?, total_cost = ?
                 WHERE id = ? AND user_id = ?''',
              (new_amount, new_price, new_total, transaction_id, user_id))
    
    conn.commit()
    conn.close()
    return True

def delete_transaction(transaction_id, user_id):
    """Xóa một giao dịch"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''DELETE FROM portfolio 
                 WHERE id = ? AND user_id = ?''',
              (transaction_id, user_id))
    conn.commit()
    affected = c.rowcount
    conn.close()
    return affected > 0

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

def fmt_percent(c):
    try:
        c = float(c)
        emoji = "📈" if c > 0 else "📉" if c < 0 else "➡️"
        return f"{emoji} {c:+.2f}%"
    except:
        return str(c)

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
         InlineKeyboardButton("🔔 Quản lý theo dõi", callback_data="show_subscribe")],
        [InlineKeyboardButton("📋 DS theo dõi", callback_data="show_mylist"),
         InlineKeyboardButton("💼 Danh mục", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 Lợi nhuận", callback_data="show_profit"),
         InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_transactions")],
        [InlineKeyboardButton("➖ Bán coin", callback_data="show_sell"),
         InlineKeyboardButton("➕ Mua coin", callback_data="show_buy")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS ====================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    welcome_msg = (
        "🚀 *ĐẦU TƯ COIN BOT*\n\n"
        "🤖 Bot hỗ trợ:\n"
        "• Xem giá bất kỳ coin nào (BTC, ETH, DOGE, SOL...)\n"
        "• Xem tỷ giá USDT/VND\n"
        "• Top 10 coin\n"
        "• Theo dõi nhiều coin cùng lúc\n"
        "• Quản lý danh mục đầu tư\n"
        "• ✏️ Sửa/Xóa giao dịch\n"
        "• Tính lợi nhuận chi tiết\n\n"
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
        "*LỆNH NHANH:*\n"
        "• `/s btc eth doge` - Xem giá nhiều coin\n"
        "• `/su btc eth doge` - Thêm nhiều coin theo dõi\n"
        "• `/uns` - Menu xóa coin\n"
        "• `/list` - Xem danh sách theo dõi\n"
        "• `/usdt` - Xem tỷ giá USDT/VND\n\n"
        "*QUẢN LÝ ĐẦU TƯ:*\n"
        "• `/buy btc 0.5 40000` - Mua coin\n"
        "• `/sell btc 0.2` - Bán coin\n"
        "• `/edit` - Xem/sửa giao dịch\n"
        "• `/edit 5` - Xem chi tiết giao dịch #5\n"
        "• `/edit 5 0.8 42000` - Sửa giao dịch #5\n"
        "• `/del 5` - Xóa giao dịch #5\n\n"
        "*Lưu ý:* Dữ liệu được lưu vĩnh viễn"
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
        return await update.message.reply_text("❌ /s btc eth doge")
    
    msg = await update.message.reply_text("🔄 Đang tra cứu...")
    results = []
    
    for arg in ctx.args:
        symbol = arg.upper()
        d = get_price(symbol)
        
        if d:
            if symbol == 'USDT':
                rate_data = get_usdt_vnd_rate()
                vnd_price = rate_data['vnd']
                results.append(
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 USD: `{fmt_price(d['p'])}`\n"
                    f"🇻🇳 VND: `{fmt_vnd(vnd_price)}`\n"
                    f"📈 24h: `{d['c']:.2f}%`"
                )
            else:
                results.append(
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 Giá: `{fmt_price(d['p'])}`\n"
                    f"📈 24h: `{d['c']:.2f}%`"
                )
            price_cache[symbol] = d
        else:
            results.append(f"❌ *{symbol}*: Không có dữ liệu")
    
    await msg.delete()
    await update.message.reply_text(
        "\n━━━━━━━━━━━━\n".join(results),
        parse_mode='Markdown'
    )

async def su_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /su btc eth doge")
    
    msg = await update.message.reply_text("🔄 Đang xử lý...")
    coins = [arg.upper() for arg in ctx.args]
    
    logger.info(f"User {uid} đang thêm coins: {coins}")
    
    results = []
    added = []
    failed = []
    existed = []
    
    for coin in coins:
        price_data = get_price(coin)
        if not price_data:
            failed.append(coin)
            continue
        
        if add_subscription(uid, coin):
            added.append(coin)
            price_cache[coin] = price_data
            logger.info(f"✅ Đã thêm {coin} cho user {uid}")
        else:
            existed.append(coin)
            logger.info(f"ℹ️ {coin} đã tồn tại cho user {uid}")
    
    # Kiểm tra lại database
    current_subs = get_subscriptions(uid)
    logger.info(f"User {uid} hiện đang theo dõi: {current_subs}")
    
    if added:
        results.append(f"✅ Đã thêm: {', '.join(added)}")
    if existed:
        results.append(f"ℹ️ Đã có: {', '.join(existed)}")
    if failed:
        results.append(f"❌ Không tìm thấy: {', '.join(failed)}")
    
    total = len(current_subs)
    results.append(f"\n📊 Tổng số đang theo dõi: {total}")
    
    await msg.delete()
    await update.message.reply_text("\n".join(results), parse_mode='Markdown')

async def uns_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    if not ctx.args:
        subs = get_subscriptions(uid)
        if not subs:
            return await update.message.reply_text("📭 Bạn chưa theo dõi coin nào!")
        
        keyboard = []
        row = []
        for i, coin in enumerate(sorted(subs)):
            row.append(InlineKeyboardButton(f"❌ {coin}", callback_data=f"uns_{coin}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        
        keyboard.append([
            InlineKeyboardButton("🗑 Xóa tất cả", callback_data="uns_all"),
            InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
        ])
        
        await update.message.reply_text(
            "📋 *CHỌN COIN ĐỂ XÓA*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if ctx.args[0].lower() == 'all':
        subs = get_subscriptions(uid)
        if not subs:
            return await update.message.reply_text("📭 Bạn chưa theo dõi coin nào!")
        
        for coin in subs:
            remove_subscription(uid, coin)
        
        await update.message.reply_text(
            f"🗑 Đã xóa *TẤT CẢ* {len(subs)} coin",
            parse_mode='Markdown'
        )
        return
    
    coins = [arg.upper() for arg in ctx.args]
    results = []
    for coin in coins:
        if coin in get_subscriptions(uid):
            remove_subscription(uid, coin)
            results.append(f"✅ Đã xóa *{coin}*")
        else:
            results.append(f"❌ *{coin}*: Không có trong danh sách")
    
    await update.message.reply_text("\n".join(results), parse_mode='Markdown')

async def list_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    subs = get_subscriptions(uid)
    
    if not subs:
        await update.message.reply_text("📭 Chưa theo dõi coin nào!\nDùng /su [coin]")
        return
    
    msg = "📋 *DANH SÁCH THEO DÕI*\n━━━━━━━━━━━━\n\n"
    
    for s in sorted(subs):
        d = get_price(s)
        if d:
            price_cache[s] = d
            emoji = "📈" if d['c'] > 0 else "📉" if d['c'] < 0 else "➡️"
            msg += f"• *{s}*: `{fmt_price(d['p'])}` {emoji} `{d['c']:+.1f}%`\n"
        else:
            msg += f"• *{s}*: `Đang cập nhật...`\n"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def buy_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(ctx.args) < 3:
        return await update.message.reply_text("❌ /buy btc 0.5 40000")
    
    symbol = ctx.args[0].upper()
    
    try:
        amount = float(ctx.args[1])
        buy_price = float(ctx.args[2])
    except ValueError:
        return await update.message.reply_text("❌ Số lượng/giá không hợp lệ!")
    
    if amount <= 0 or buy_price <= 0:
        return await update.message.reply_text("❌ Số lượng và giá phải > 0")
    
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Không thể lấy giá *{symbol}*", parse_mode='Markdown')
    
    add_transaction(uid, symbol, amount, buy_price)
    
    current_price = price_data['p']
    profit = (current_price - buy_price) * amount
    profit_percent = ((current_price - buy_price) / buy_price) * 100
    
    msg = (
        f"✅ *ĐÃ MUA {symbol}*\n━━━━━━━━━━━━━━━━\n\n"
        f"📊 SL: `{amount:.4f}`\n"
        f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
        f"💵 Vốn: `{fmt_price(amount * buy_price)}`\n"
        f"📈 Giá hiện: `{fmt_price(current_price)}`\n"
        f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def sell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        return await update.message.reply_text("❌ /sell btc 0.2")
    
    symbol = ctx.args[0].upper()
    
    try:
        sell_amount = float(ctx.args[1])
    except ValueError:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    if sell_amount <= 0:
        return await update.message.reply_text("❌ Số lượng phải > 0")
    
    portfolio_data = get_portfolio(uid)
    if not portfolio_data:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    portfolio = []
    for row in portfolio_data:
        portfolio.append({
            'symbol': row[0], 'amount': row[1], 'buy_price': row[2],
            'buy_date': row[3], 'total_cost': row[4]
        })
    
    symbol_txs = [tx for tx in portfolio if tx['symbol'] == symbol]
    if not symbol_txs:
        return await update.message.reply_text(f"❌ Không có *{symbol}*", parse_mode='Markdown')
    
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(f"❌ Chỉ có {total_amount:.4f} {symbol}")
    
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
        f"✅ *ĐÃ BÁN {sell_amount:.4f} {symbol}*\n━━━━━━━━━━━━━━━━\n\n"
        f"💰 Giá bán: `{fmt_price(current_price)}`\n"
        f"💵 Giá trị: `{fmt_price(sold_value)}`\n"
        f"📊 Vốn: `{fmt_price(sold_cost)}`\n"
        f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def edit_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    if not ctx.args:
        transactions = get_transaction_detail(uid)
        if not transactions:
            await update.message.reply_text("📭 Danh mục trống!")
            return
        
        msg = "📝 *CHỌN GIAO DỊCH*\n━━━━━━━━━━━━\n\n"
        keyboard = []
        row = []
        
        for i, tx in enumerate(transactions, 1):
            tx_id, symbol, amount, price, date, total = tx
            short_date = date.split()[0]
            msg += f"*{i}.* {symbol} - {amount:.4f} @ {fmt_price(price)} - {short_date}\n"
            
            row.append(InlineKeyboardButton(f"✏️ #{tx_id}", callback_data=f"edit_{tx_id}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
        
        await update.message.reply_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if len(ctx.args) == 1:
        try:
            tx_id = int(ctx.args[0])
            transactions = get_transaction_detail(uid)
            
            tx = next((t for t in transactions if t[0] == tx_id), None)
            if not tx:
                await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
                return
            
            tx_id, symbol, amount, price, date, total = tx
            price_data = get_price(symbol)
            current_price = price_data['p'] if price_data else 0
            profit = (current_price - price) * amount if current_price else 0
            profit_percent = ((current_price - price) / price) * 100 if price and current_price else 0
            
            msg = (
                f"📝 *GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                f"*{symbol}*\n📅 {date}\n📊 SL: `{amount:.4f}`\n"
                f"💰 Giá mua: `{fmt_price(price)}`\n💵 Vốn: `{fmt_price(total)}`\n"
                f"📈 Giá hiện: `{fmt_price(current_price)}`\n"
                f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
                f"*Sửa:* `/edit {tx_id} [sl] [giá]`\n*Xóa:* `/del {tx_id}`"
            )
            
            keyboard = [[
                InlineKeyboardButton("✏️ Sửa", callback_data=f"edit_{tx_id}"),
                InlineKeyboardButton("🗑 Xóa", callback_data=f"del_{tx_id}")
            ],[
                InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
            ]]
            
            await update.message.reply_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ")
    
    elif len(ctx.args) == 3:
        try:
            tx_id = int(ctx.args[0])
            new_amount = float(ctx.args[1])
            new_price = float(ctx.args[2])
            
            if new_amount <= 0 or new_price <= 0:
                await update.message.reply_text("❌ SL và giá phải > 0")
                return
            
            if update_transaction(tx_id, uid, new_amount, new_price):
                await update.message.reply_text(
                    f"✅ Đã cập nhật giao dịch #{tx_id}\n"
                    f"📊 SL mới: `{new_amount:.4f}`\n"
                    f"💰 Giá mới: `{fmt_price(new_price)}`",
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
        except ValueError:
            await update.message.reply_text("❌ /edit [id] [sl] [giá]")
    else:
        await update.message.reply_text("❌ /edit - Xem DS\n/edit [id] - Xem chi tiết\n/edit [id] [sl] [giá] - Sửa")

async def delete_tx_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    if not ctx.args:
        await update.message.reply_text("❌ /del [id]")
        return
    
    try:
        tx_id = int(ctx.args[0])
        
        keyboard = [[
            InlineKeyboardButton("✅ Có", callback_data=f"confirm_del_{tx_id}"),
            InlineKeyboardButton("❌ Không", callback_data="show_portfolio")
        ]]
        
        await update.message.reply_text(
            f"⚠️ *Xác nhận xóa giao dịch #{tx_id}?*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except ValueError:
        await update.message.reply_text("❌ ID không hợp lệ")

# ==================== HANDLE MESSAGE ====================

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text(
            "💰 *MENU ĐẦU TƯ COIN*",
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
            "💰 *MENU ĐẦU TƯ COIN*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_menu_keyboard()
        )
    
    elif data == "refresh_usdt":
        rate_data = get_usdt_vnd_rate()
        vnd = rate_data['vnd']
        
        text = (
            "💱 *TỶ GIÁ USDT/VND*\n━━━━━━━━━━━━━━━━\n\n"
            f"🇺🇸 *1 USDT* = `{fmt_vnd(vnd)}`\n"
            f"🇻🇳 *1,000,000 VND* = `{1000000/vnd:.4f} USDT`\n\n"
            f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
            f"📊 *Nguồn:* `{rate_data['source']}`"
        )
        
        keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                    [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
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
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`\n"
                    f"📈 24h: {fmt_percent(d['c'])}"
                )
            else:
                msg = (
                    f"*{d['n']}* #{d['r']}\n"
                    f"💰 Giá: `{fmt_price(d['p'])}`\n"
                    f"📦 Volume: `{fmt_vol(d['v'])}`\n"
                    f"💎 Market Cap: `{fmt_vol(d['m'])}`\n"
                    f"📈 24h: {fmt_percent(d['c'])}"
                )
            price_cache[symbol] = d
        else:
            msg = f"❌ *{symbol}*: Không có dữ liệu"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("sub_"):
        symbol = data.replace("sub_", "")
        uid = query.from_user.id
        
        logger.info(f"User {uid} đang thêm {symbol} từ callback")
        
        # Kiểm tra xem đã theo dõi chưa
        subs = get_subscriptions(uid)
        
        if symbol in subs:
            msg = f"ℹ️ *{symbol}* đã có trong danh sách theo dõi!"
        else:
            price_data = get_price(symbol)
            if not price_data:
                await query.edit_message_text(
                    f"❌ Không thể thêm *{symbol}*",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="show_subscribe")]])
                )
                return
            
            if add_subscription(uid, symbol):
                msg = f"✅ Đã thêm *{symbol}* vào danh sách theo dõi!"
                price_cache[symbol] = price_data
                logger.info(f"✅ Đã thêm {symbol} cho user {uid}")
            else:
                msg = f"❌ Không thể thêm *{symbol}*"
        
        # Lấy danh sách mới
        new_subs = get_subscriptions(uid)
        logger.info(f"User {uid} sau khi thêm: {new_subs}")
        
        if new_subs:
            msg += f"\n\n📋 *Danh sách hiện tại:*\n"
            for coin in sorted(new_subs)[:10]:
                msg += f"• `{coin}`\n"
            if len(new_subs) > 10:
                msg += f"• ... và {len(new_subs)-10} coin khác\n"
            msg += f"\n📊 Tổng: {len(new_subs)} coin"
        
        keyboard = [[
            InlineKeyboardButton("🔔 Tiếp tục quản lý", callback_data="show_subscribe"),
            InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
        ]]
        
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("uns_"):
        coin = data.replace("uns_", "")
        uid = query.from_user.id
        
        if coin == "all":
            subs = get_subscriptions(uid)
            for c in subs:
                remove_subscription(uid, c)
            msg = f"🗑 Đã xóa *TẤT CẢ* {len(subs)} coin khỏi danh sách theo dõi!"
        else:
            if remove_subscription(uid, coin):
                msg = f"✅ Đã xóa *{coin}* khỏi danh sách theo dõi!"
            else:
                msg = f"❌ Không tìm thấy *{coin}* trong danh sách!"
        
        # Lấy danh sách mới
        remaining = get_subscriptions(uid)
        if remaining:
            msg += f"\n\n📋 *Các coin còn lại:*\n"
            for c in sorted(remaining)[:10]:
                msg += f"• `{c}`\n"
            if len(remaining) > 10:
                msg += f"• ... và {len(remaining)-10} coin khác\n"
            msg += f"\n📊 Tổng: {len(remaining)} coin"
        else:
            msg += "\n\n📭 Danh sách theo dõi hiện đang trống."
        
        keyboard = [[
            InlineKeyboardButton("🔔 Quản lý tiếp", callback_data="show_subscribe"),
            InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
        ]]
        
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_subscribe":
        uid = query.from_user.id
        subs = get_subscriptions(uid)
        
        logger.info(f"User {uid} xem menu subscribe, subs: {subs}")
        
        # Tạo keyboard động dựa trên coin đang theo dõi
        keyboard = []
        
        # Nếu có coin đang theo dõi, hiển thị để xóa
        if subs:
            row = []
            for i, coin in enumerate(sorted(subs)):
                row.append(InlineKeyboardButton(f"❌ {coin}", callback_data=f"uns_{coin}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            keyboard.append([])  # Thêm dòng trống để phân cách
        
        # Thêm các nút thêm coin nhanh
        popular_coins = [
            ["➕ BTC", "➕ ETH", "➕ USDT"],
            ["➕ BNB", "➕ SOL", "➕ XRP"],
            ["➕ DOGE", "➕ ADA", "➕ DOT"],
        ]
        
        for row in popular_coins:
            btn_row = []
            for btn in row:
                coin = btn.replace("➕ ", "")
                # Chỉ hiển thị nút thêm nếu chưa có trong danh sách
                if coin not in subs:
                    btn_row.append(InlineKeyboardButton(btn, callback_data=f"sub_{coin}"))
            if btn_row:  # Chỉ thêm row nếu có nút
                keyboard.append(btn_row)
        
        # Nút xóa tất cả và quay lại
        if subs:
            keyboard.append([InlineKeyboardButton("🗑 Xóa tất cả", callback_data="uns_all")])
        keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
        
        # Tạo message hiển thị danh sách đang theo dõi
        msg = "🔔 *QUẢN LÝ THEO DÕI*\n━━━━━━━━━━━━━━━━\n\n"
        
        if subs:
            msg += "📋 *Đang theo dõi:*\n"
            for i, coin in enumerate(sorted(subs), 1):
                msg += f"{i}. `{coin}`\n"
                if i == 15 and len(subs) > 15:
                    msg += f"... và {len(subs)-15} coin khác\n"
                    break
            msg += f"\n📊 Tổng số: {len(subs)} coin\n\n"
        else:
            msg += "📭 Bạn chưa theo dõi coin nào!\n\n"
        
        msg += "👇 *Chọn để thêm hoặc xóa:*"
        
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
                d = get_price(s)
                if d:
                    price_cache[s] = d
                    emoji = "📈" if d['c'] > 0 else "📉" if d['c'] < 0 else "➡️"
                    msg += f"• *{s}*: `{fmt_price(d['p'])}` {emoji} `{d['c']:+.1f}%`\n"
                else:
                    msg += f"• *{s}*: `Đang cập nhật...`\n"
            
            keyboard = []
            row = []
            for i, coin in enumerate(sorted(subs)):
                row.append(InlineKeyboardButton(f"❌ {coin}", callback_data=f"uns_{coin}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([
                InlineKeyboardButton("🗑 Xóa tất cả", callback_data="uns_all"),
                InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
            ])
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
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
                "📭 Danh mục trống!\nDùng /buy",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
            return
        
        summary = {}
        total_invest = 0
        total_value = 0
        
        for row in portfolio_data:
            symbol, amount, price, date, cost = row[0], row[1], row[2], row[3], row[4]
            if symbol not in summary:
                summary[symbol] = {'amount': 0, 'cost': 0}
            summary[symbol]['amount'] += amount
            summary[symbol]['cost'] += cost
        
        msg = "📊 *DANH MỤC*\n━━━━━━━━━━━━\n\n"
        
        for symbol, data in summary.items():
            price_data = get_price(symbol)
            if price_data:
                current = data['amount'] * price_data['p']
                profit = current - data['cost']
                profit_percent = (profit / data['cost']) * 100 if data['cost'] > 0 else 0
                total_invest += data['cost']
                total_value += current
                
                avg = data['cost'] / data['amount']
                
                msg += f"*{symbol}*\n"
                msg += f"📊 SL: `{data['amount']:.4f}`\n"
                msg += f"💰 TB: `{fmt_price(avg)}`\n"
                msg += f"💎 TT: `{fmt_price(current)}`\n"
                msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
        
        total_profit = total_value - total_invest
        total_profit_percent = (total_profit / total_invest) * 100 if total_invest > 0 else 0
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
        msg += f"💰 GT: `{fmt_price(total_value)}`\n"
        msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
        
        keyboard = [
            [InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_transactions")],
            [InlineKeyboardButton("➕ Mua", callback_data="show_buy"),
             InlineKeyboardButton("➖ Bán", callback_data="show_sell")],
            [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]
        ]
        
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "edit_transactions":
        uid = query.from_user.id
        transactions = get_transaction_detail(uid)
        
        if not transactions:
            await query.edit_message_text(
                "📭 Không có giao dịch!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
            return
        
        msg = "✏️ *CHỌN GIAO DỊCH*\n━━━━━━━━━━━━\n\n"
        keyboard = []
        row = []
        
        for tx in transactions:
            tx_id, symbol, amount, price, date, total = tx
            short_date = date.split()[0]
            msg += f"• #{tx_id}: {symbol} {amount:.4f} @ {fmt_price(price)} ({short_date})\n"
            
            row.append(InlineKeyboardButton(f"#{tx_id}", callback_data=f"edit_{tx_id}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("🔙 Về danh mục", callback_data="show_portfolio")])
        
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("edit_"):
        tx_id = data.replace("edit_", "")
        uid = query.from_user.id
        
        transactions = get_transaction_detail(uid)
        tx = next((t for t in transactions if str(t[0]) == tx_id), None)
        
        if not tx:
            await query.edit_message_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
            return
        
        tx_id, symbol, amount, price, date, total = tx
        
        msg = (
            f"✏️ *SỬA GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
            f"*{symbol}*\n📅 {date}\n"
            f"📊 SL: `{amount:.4f}`\n"
            f"💰 Giá: `{fmt_price(price)}`\n\n"
            f"*Nhập lệnh:*\n`/edit {tx_id} [sl] [giá]`"
        )
        
        keyboard = [[
            InlineKeyboardButton("🗑 Xóa", callback_data=f"del_{tx_id}"),
            InlineKeyboardButton("🔙 Quay lại", callback_data="edit_transactions")
        ]]
        
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("del_"):
        tx_id = data.replace("del_", "")
        
        msg = f"⚠️ *Xác nhận xóa giao dịch #{tx_id}?*"
        keyboard = [[
            InlineKeyboardButton("✅ Có", callback_data=f"confirm_del_{tx_id}"),
            InlineKeyboardButton("❌ Không", callback_data="edit_transactions")
        ]]
        
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("confirm_del_"):
        tx_id = data.replace("confirm_del_", "")
        uid = query.from_user.id
        
        if delete_transaction(int(tx_id), uid):
            msg = f"✅ Đã xóa giao dịch #{tx_id}"
        else:
            msg = f"❌ Không thể xóa giao dịch #{tx_id}"
        
        keyboard = [[InlineKeyboardButton("🔙 Về danh mục", callback_data="show_portfolio")]]
        
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_profit":
        uid = query.from_user.id
        transactions = get_transaction_detail(uid)
        
        if not transactions:
            await query.edit_message_text(
                "📭 Danh mục trống!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )
            return
        
        msg = "📈 *CHI TIẾT LỢI NHUẬN*\n━━━━━━━━━━━━\n\n"
        total_invest = 0
        total_value = 0
        
        for tx in transactions:
            tx_id, symbol, amount, price, date, cost = tx
            price_data = get_price(symbol)
            
            if price_data:
                current = amount * price_data['p']
                profit = current - cost
                profit_percent = (profit / cost) * 100
                
                total_invest += cost
                total_value += current
                
                short_date = date.split()[0]
                msg += f"*#{tx_id}: {symbol}*\n"
                msg += f"📅 {short_date}\n"
                msg += f"📊 SL: `{amount:.4f}`\n"
                msg += f"💰 Mua: `{fmt_price(price)}`\n"
                msg += f"💎 TT: `{fmt_price(current)}`\n"
                msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
        
        total_profit = total_value - total_invest
        total_profit_percent = (total_profit / total_invest) * 100 if total_invest > 0 else 0
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
        msg += f"💰 GT: `{fmt_price(total_value)}`\n"
        msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "show_buy":
        await query.edit_message_text(
            "➕ *MUA COIN*\n\n"
            "Dùng lệnh: `/buy [coin] [sl] [giá]`\n\n"
            "*Ví dụ:*\n"
            "• `/buy btc 0.5 40000`\n"
            "• `/buy eth 5 2500`\n"
            "• `/buy doge 1000 0.3`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_sell":
        await query.edit_message_text(
            "➖ *BÁN COIN*\n\n"
            "Dùng lệnh: `/sell [coin] [sl]`\n\n"
            "*Ví dụ:*\n"
            "• `/sell btc 0.2`\n"
            "• `/sell eth 2`\n"
            "• `/sell doge 500`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_top10":
        await query.edit_message_text("🔄 Đang tải...")
        
        try:
            headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
            res = requests.get(
                f"{CMC_API_URL}/cryptocurrency/listings/latest",
                headers=headers, params={'limit': 10, 'convert': 'USD'},
                timeout=10
            )
            
            if res.status_code == 200:
                data = res.json()['data']
                msg = "📊 *TOP 10 COIN*\n━━━━━━━━━━━━\n\n"
                
                for i, coin in enumerate(data, 1):
                    quote = coin['quote']['USD']
                    change = quote['percent_change_24h']
                    emoji = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                    
                    msg += f"{i}. *{coin['symbol']}* - {coin['name']}\n"
                    msg += f"   💰 `{fmt_price(quote['price'])}` {emoji} `{change:+.2f}%`\n"
            else:
                msg = "❌ Không thể lấy dữ liệu"
        except Exception as e:
            logger.error(f"Lỗi top10: {e}")
            msg = "❌ Lỗi kết nối"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        await query.edit_message_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ==================== AUTO UPDATE ====================

def auto_update():
    global app
    last_update = {}
    
    while True:
        try:
            time.sleep(60)
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT DISTINCT user_id FROM subscriptions")
            users = c.fetchall()
            conn.close()
            
            for (uid,) in users:
                try:
                    now = time.time()
                    if uid in last_update and now - last_update[uid] < 300:
                        continue
                    
                    subs = get_subscriptions(uid)
                    if not subs:
                        continue
                    
                    updates = []
                    for s in subs[:10]:
                        d = get_price(s)
                        if d:
                            price_cache[s] = d
                            emoji = "📈" if d['c'] > 0 else "📉" if d['c'] < 0 else "➡️"
                            updates.append(f"• *{d['n']}*: `{fmt_price(d['p'])}` {emoji} `{d['c']:+.1f}%`")
                    
                    if updates and app:
                        try:
                            msg = "🔄 *CẬP NHẬT GIÁ*\n" + "\n".join(updates)
                            app.bot.send_message(uid, msg, parse_mode='Markdown')
                            last_update[uid] = now
                        except Exception as e:
                            logger.error(f"Lỗi gửi tin cho user {uid}: {e}")
                            
                except Exception as e:
                    logger.error(f"Lỗi xử lý user {uid}: {e}")
                    
        except Exception as e:
            logger.error(f"Lỗi auto_update: {e}")
            time.sleep(10)

# ==================== MAIN ====================

if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        logger.error("❌ Thiếu TELEGRAM_TOKEN")
        exit(1)
    
    if not CMC_API_KEY:
        logger.warning("⚠️ Thiếu CMC_API_KEY")
    
    try:
        init_database()
        test_file = os.path.join(DATA_DIR, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        logger.info("✅ Disk có quyền ghi")
    except Exception as e:
        logger.error(f"❌ Lỗi database: {e}")
        exit(1)
    
    logger.info("🚀 Khởi động bot...")
    logger.info(f"💾 Database: {DB_PATH}")
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("usdt", usdt_command))
    app.add_handler(CommandHandler("s", s_command))
    app.add_handler(CommandHandler("su", su_command))
    app.add_handler(CommandHandler("uns", uns_command))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("ds", list_command))
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("sell", sell_command))
    app.add_handler(CommandHandler("edit", edit_command))
    app.add_handler(CommandHandler("del", delete_tx_command))
    app.add_handler(CommandHandler("delete", delete_tx_command))
    app.add_handler(CommandHandler("xoa", delete_tx_command))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Threads
    threading.Thread(target=auto_update, daemon=True).start()
    threading.Thread(target=schedule_backup, daemon=True).start()
    threading.Thread(target=run_health_server, daemon=True).start()
    
    logger.info("✅ Bot sẵn sàng!")
    app.run_polling()

import os
import threading
import time
import requests
import sqlite3
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode

# Cấu hình logging ĐƠN GIẢN
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

# ==================== CẤU HÌNH DATABASE ====================
DATA_DIR = '/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, 'crypto_bot.db')
BACKUP_DIR = os.path.join(DATA_DIR, 'backups')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

logger.info(f"📁 Database: {DB_PATH}")

# Cache
price_cache = {}
usdt_cache = {'rate': None, 'time': None}
app = None

# ==================== HEALTH CHECK SERVER ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is running")
    def log_message(self, *args): pass

def run_health_server():
    port = int(os.environ.get('PORT', 10000))
    HTTPServer(('0.0.0.0', port), HealthCheckHandler).serve_forever()

# ==================== DATABASE ====================
def init_database():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER, symbol TEXT, PRIMARY KEY (user_id, symbol))''')
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER, symbol TEXT, amount REAL,
                      buy_price REAL, buy_date TEXT, total_cost REAL)''')
        conn.commit()
        conn.close()
        logger.info("✅ Database OK")
        return True
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        return False

def add_subscription(user_id, symbol):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO subscriptions (user_id, symbol) VALUES (?, ?)",
                  (user_id, symbol.upper()))
        conn.commit()
        conn.close()
        return True
    except:
        return False

def remove_subscription(user_id, symbol):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM subscriptions WHERE user_id = ? AND symbol = ?",
                  (user_id, symbol.upper()))
        conn.commit()
        conn.close()
        return True
    except:
        return False

def get_subscriptions(user_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT symbol FROM subscriptions WHERE user_id = ?", (user_id,))
        result = [row[0] for row in c.fetchall()]
        conn.close()
        return result
    except:
        return []

def add_transaction(user_id, symbol, amount, buy_price):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_cost = amount * buy_price
        c.execute('''INSERT INTO portfolio (user_id, symbol, amount, buy_price, buy_date, total_cost)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (user_id, symbol.upper(), amount, buy_price, buy_date, total_cost))
        conn.commit()
        conn.close()
        return True
    except:
        return False

def get_portfolio(user_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost 
                     FROM portfolio WHERE user_id = ?''', (user_id,))
        result = c.fetchall()
        conn.close()
        return result
    except:
        return []

def get_transaction_detail(user_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost 
                     FROM portfolio WHERE user_id = ?''', (user_id,))
        result = c.fetchall()
        conn.close()
        return result
    except:
        return []

def delete_transaction(tx_id, user_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM portfolio WHERE id = ? AND user_id = ?", (tx_id, user_id))
        conn.commit()
        conn.close()
        return True
    except:
        return False

# ==================== HÀM LẤY GIÁ ====================
def get_price(symbol):
    if not CMC_API_KEY:
        return None
    try:
        symbol = symbol.upper()
        clean = 'USDT' if symbol == 'USDT' else symbol.replace('USDT', '').replace('USD', '')
        headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
        res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest",
                          headers=headers, params={'symbol': clean, 'convert': 'USD'},
                          timeout=5)
        if res.status_code == 200:
            data = res.json()['data'][clean]['quote']['USD']
            return {'p': data['price'], 'c': data['percent_change_24h']}
    except:
        return None
    return None

def get_usdt_vnd_rate():
    try:
        res = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=vnd", timeout=3)
        if res.status_code == 200:
            return {'vnd': float(res.json()['tether']['vnd']), 'source': 'CoinGecko'}
    except:
        pass
    return {'vnd': 25000, 'source': 'Fallback'}

# ==================== ĐỊNH DẠNG ====================
def fmt_price(p):
    try:
        p = float(p)
        if p < 0.01: return f"${p:.6f}"
        elif p < 1: return f"${p:.4f}"
        else: return f"${p:,.2f}"
    except: return f"${p}"

def fmt_vnd(p):
    try: return f"₫{float(p):,.0f}"
    except: return f"₫{p}"

def fmt_percent(c):
    try:
        c = float(c)
        return f"{'📈' if c>0 else '📉'} {c:+.2f}%"
    except: return str(c)

# ==================== KEYBOARD ====================
def get_main_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("💰 ĐẦU TƯ COIN")], [KeyboardButton("❓ HƯỚNG DẪN")]], resize_keyboard=True)

def get_invest_menu():
    keyboard = [
        [InlineKeyboardButton("BTC", callback_data="price_BTC"),
         InlineKeyboardButton("ETH", callback_data="price_ETH"),
         InlineKeyboardButton("USDT", callback_data="price_USDT")],
        [InlineKeyboardButton("📊 Top 10", callback_data="top10"),
         InlineKeyboardButton("📋 DS theo dõi", callback_data="mylist")],
        [InlineKeyboardButton("💼 Danh mục", callback_data="portfolio"),
         InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_list")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMANDS ====================
async def start(update: Update, context):
    await update.message.reply_text(
        "🚀 *ĐẦU TƯ COIN BOT*\n\nBấm nút bên dưới:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help_command(update: Update, context):
    await update.message.reply_text(
        "📘 *HƯỚNG DẪN*\n\n"
        "/s btc - Xem giá\n"
        "/su btc - Theo dõi\n"
        "/uns - Xóa theo dõi\n"
        "/list - DS theo dõi\n"
        "/buy btc 0.5 40000 - Mua\n"
        "/sell btc 0.2 - Bán\n"
        "/edit - Sửa/Xóa giao dịch",
        parse_mode=ParseMode.MARKDOWN
    )

async def s_command(update: Update, context):
    if not context.args:
        await update.message.reply_text("❌ /s btc eth")
        return
    
    results = []
    for arg in context.args[:3]:
        d = get_price(arg)
        if d:
            results.append(f"*{arg.upper()}*: `{fmt_price(d['p'])}` {fmt_percent(d['c'])}")
        else:
            results.append(f"❌ *{arg.upper()}*")
    
    await update.message.reply_text("\n".join(results), parse_mode='Markdown')

async def su_command(update: Update, context):
    if not context.args:
        await update.message.reply_text("❌ /su btc eth")
        return
    
    uid = update.effective_user.id
    added = []
    for arg in context.args[:5]:
        if get_price(arg) and add_subscription(uid, arg):
            added.append(arg.upper())
    
    if added:
        await update.message.reply_text(f"✅ Đã thêm: {', '.join(added)}")
    else:
        await update.message.reply_text("❌ Không thêm được")

async def uns_command(update: Update, context):
    uid = update.effective_user.id
    subs = get_subscriptions(uid)
    
    if not subs:
        await update.message.reply_text("📭 Chưa có coin nào")
        return
    
    if not context.args:
        # Hiển thị menu xóa
        keyboard = []
        row = []
        for coin in sorted(subs):
            row.append(InlineKeyboardButton(f"❌ {coin}", callback_data=f"uns_{coin}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("🗑 Xóa tất cả", callback_data="uns_all")])
        await update.message.reply_text("Chọn coin để xóa:", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    # Xóa theo lệnh
    removed = []
    for arg in context.args:
        if arg.upper() in subs and remove_subscription(uid, arg):
            removed.append(arg.upper())
    
    if removed:
        await update.message.reply_text(f"✅ Đã xóa: {', '.join(removed)}")

async def list_command(update: Update, context):
    uid = update.effective_user.id
    subs = get_subscriptions(uid)
    
    if not subs:
        await update.message.reply_text("📭 Chưa có coin nào")
        return
    
    msg = "📋 *DS THEO DÕI*\n"
    for s in sorted(subs):
        d = get_price(s)
        msg += f"• {s}: `{fmt_price(d['p']) if d else '...'}`\n"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def buy_command(update: Update, context):
    if len(context.args) < 3:
        await update.message.reply_text("❌ /buy btc 0.5 40000")
        return
    
    try:
        uid = update.effective_user.id
        symbol = context.args[0].upper()
        amount = float(context.args[1])
        price = float(context.args[2])
        
        if add_transaction(uid, symbol, amount, price):
            await update.message.reply_text(f"✅ Đã mua {amount} {symbol}")
        else:
            await update.message.reply_text("❌ Lỗi")
    except:
        await update.message.reply_text("❌ Sai định dạng")

async def sell_command(update: Update, context):
    await update.message.reply_text("⚠️ Đang cập nhật")

async def edit_command(update: Update, context):
    uid = update.effective_user.id
    txs = get_transaction_detail(uid)
    
    if not txs:
        await update.message.reply_text("📭 Không có giao dịch")
        return
    
    if not context.args:
        # Hiển thị danh sách
        msg = "📝 *CHỌN GIAO DỊCH*\n"
        keyboard = []
        for tx in txs[:5]:
            tx_id, symbol, amount, price, date, _ = tx
            msg += f"#{tx_id}: {symbol} {amount:.4f} @ {fmt_price(price)}\n"
            keyboard.append([InlineKeyboardButton(f"✏️ #{tx_id}", callback_data=f"edit_{tx_id}")])
        keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back")])
        
        await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    elif len(context.args) == 1:
        # Xem chi tiết
        try:
            tx_id = int(context.args[0])
            tx = next((t for t in txs if t[0] == tx_id), None)
            if tx:
                await update.message.reply_text(f"#{tx[0]}: {tx[1]} {tx[2]:.4f} @ {fmt_price(tx[3])}")
            else:
                await update.message.reply_text("❌ Không tìm thấy")
        except:
            await update.message.reply_text("❌ ID không hợp lệ")

# ==================== MESSAGE HANDLER ====================
async def handle_message(update: Update, context):
    text = update.message.text
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text("💰 *MENU*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_invest_menu())
    elif text == "❓ HƯỚNG DẪN":
        await help_command(update, context)

# ==================== CALLBACK HANDLER ====================
async def handle_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "back":
        await query.edit_message_text("💰 *MENU*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_invest_menu())
    
    elif data.startswith("price_"):
        symbol = data.replace("price_", "")
        d = get_price(symbol)
        if d:
            msg = f"*{symbol}*: `{fmt_price(d['p'])}` {fmt_percent(d['c'])}"
        else:
            msg = f"❌ *{symbol}*"
        await query.edit_message_text(msg, parse_mode='Markdown')
    
    elif data == "top10":
        await query.edit_message_text("📊 *TOP 10*\nĐang cập nhật...", parse_mode='Markdown')
    
    elif data == "mylist":
        uid = query.from_user.id
        subs = get_subscriptions(uid)
        if subs:
            await query.edit_message_text(f"📋 DS: {', '.join(subs)}")
        else:
            await query.edit_message_text("📭 Chưa có")
    
    elif data == "portfolio":
        uid = query.from_user.id
        pf = get_portfolio(uid)
        if pf:
            total = sum(t[4] for t in pf)
            await query.edit_message_text(f"💼 {len(pf)} giao dịch, tổng: {fmt_price(total)}")
        else:
            await query.edit_message_text("📭 Danh mục trống")
    
    elif data == "edit_list":
        uid = query.from_user.id
        txs = get_transaction_detail(uid)
        if txs:
            keyboard = [[InlineKeyboardButton(f"#{tx[0]} {tx[1]}", callback_data=f"edit_{tx[0]}")] for tx in txs[:5]]
            keyboard.append([InlineKeyboardButton("🔙", callback_data="back")])
            await query.edit_message_text("Chọn giao dịch:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text("📭 Không có giao dịch")
    
    elif data.startswith("uns_"):
        coin = data.replace("uns_", "")
        uid = query.from_user.id
        if coin == "all":
            subs = get_subscriptions(uid)
            for c in subs:
                remove_subscription(uid, c)
            await query.edit_message_text("🗑 Đã xóa tất cả")
        else:
            remove_subscription(uid, coin)
            await query.edit_message_text(f"✅ Đã xóa {coin}")
    
    elif data.startswith("edit_"):
        tx_id = data.replace("edit_", "")
        await query.edit_message_text(f"✏️ Sửa #{tx_id}\nDùng lệnh: /edit {tx_id}")

# ==================== MAIN ====================
if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        logger.error("❌ Thiếu TELEGRAM_TOKEN")
        exit(1)
    
    if not init_database():
        logger.error("❌ Lỗi database")
        exit(1)
    
    # Khởi tạo bot
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("s", s_command))
    app.add_handler(CommandHandler("su", su_command))
    app.add_handler(CommandHandler("uns", uns_command))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("sell", sell_command))
    app.add_handler(CommandHandler("edit", edit_command))
    
    # Message & Callback
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Threads
    threading.Thread(target=run_health_server, daemon=True).start()
    
    logger.info("✅ Bot sẵn sàng!")
    app.run_polling()

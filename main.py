import os
import threading
import time
import requests
import json
import sqlite3
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

# Cache
price_cache = {}
usdt_cache = {'rate': None, 'time': None}

# ==================== DATABASE SETUP ====================

def init_database():
    """Khởi tạo database và các bảng"""
    conn = sqlite3.connect('crypto_bot.db')
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
    print("✅ Database initialized")

# ==================== DATABASE FUNCTIONS ====================

def add_subscription(user_id, symbol):
    """Thêm theo dõi"""
    conn = sqlite3.connect('crypto_bot.db')
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
    conn = sqlite3.connect('crypto_bot.db')
    c = conn.cursor()
    c.execute("DELETE FROM subscriptions WHERE user_id = ? AND symbol = ?",
              (user_id, symbol))
    conn.commit()
    conn.close()

def get_subscriptions(user_id):
    """Lấy danh sách theo dõi"""
    conn = sqlite3.connect('crypto_bot.db')
    c = conn.cursor()
    c.execute("SELECT symbol FROM subscriptions WHERE user_id = ? ORDER BY symbol",
              (user_id,))
    result = [row[0] for row in c.fetchall()]
    conn.close()
    return result

def add_transaction(user_id, symbol, amount, buy_price):
    """Thêm giao dịch mua"""
    conn = sqlite3.connect('crypto_bot.db')
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
    conn = sqlite3.connect('crypto_bot.db')
    c = conn.cursor()
    c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost 
                 FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
              (user_id,))
    result = c.fetchall()
    conn.close()
    return result

def delete_sold_transactions(user_id, kept_transactions):
    """Xóa các giao dịch đã bán và cập nhật lại"""
    # kept_transactions là list các transaction ID còn lại
    conn = sqlite3.connect('crypto_bot.db')
    c = conn.cursor()
    
    # Xóa tất cả transactions của user
    c.execute("DELETE FROM portfolio WHERE user_id = ?", (user_id,))
    
    # Thêm lại các transaction còn lại
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
        
        res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest", 
                          headers={'X-CMC_PRO_API_KEY': CMC_API_KEY},
                          params={'symbol': clean, 'convert': 'USD'}, timeout=10)
        
        if res.status_code == 200:
            data = res.json()['data'][clean]['quote']['USD']
            return {
                'p': data['price'], 
                'v': data['volume_24h'], 
                'c': data['percent_change_24h'], 
                'm': data['market_cap'],
                'n': res.json()['data'][clean]['name'],
                'r': res.json()['data'][clean].get('cmc_rank', 'N/A')
            }
    except Exception as e:
        print(f"Lỗi get_price {symbol}: {e}")
        return None

# ==================== HÀM LẤY TỶ GIÁ USDT/VND ====================

def get_usdt_vnd_rate():
    """Lấy tỷ giá USDT/VND từ nhiều nguồn"""
    
    if usdt_cache['rate'] and usdt_cache['time']:
        time_diff = (datetime.now() - usdt_cache['time']).total_seconds()
        if time_diff < 180:
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
    except:
        pass
    
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
    except:
        pass
    
    # Fallback
    result = {
        'source': 'Fallback',
        'vnd': 25500,
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

def fmt_percent(value):
    try:
        value = float(value)
        emoji = "📈" if value > 0 else "📉" if value < 0 else "➡️"
        return f"{emoji} {value:+.2f}%"
    except:
        return str(value)

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

async def start(update, ctx):
    welcome_msg = (
        "🚀 *ĐẦU TƯ COIN BOT*\n\n"
        "🤖 Bot hỗ trợ:\n"
        "• Xem giá BTC/ETH/USDT (có USDT/VND)\n"
        "• Xem tỷ giá USDT/VND\n"
        "• Top 10 coin\n"
        "• Theo dõi biến động giá\n"
        "• Quản lý danh mục đầu tư (lưu vĩnh viễn)\n"
        "• Tính lợi nhuận\n\n"
        "👇 *Bấm ĐẦU TƯ COIN để bắt đầu*"
    )
    await update.message.reply_text(
        welcome_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help(update, ctx):
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
        "*Lưu ý:* Dữ liệu được lưu vĩnh viễn"
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

async def usdt_command(update, ctx):
    msg = await update.message.reply_text("🔄 Đang tra cứu...")
    
    rate_data = get_usdt_vnd_rate()
    vnd = rate_data['vnd']
    
    text = (
        "💱 *TỶ GIÁ USDT/VND*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"🇺🇸 *1 USDT* = `{fmt_vnd(vnd)}`\n"
        f"🇻🇳 *1,000,000 VND* = `{1000000/vnd:.4f} USDT`\n\n"
        f"⏱ *Thời gian:* `{rate_data['update_time']}`\n"
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

async def s(update, ctx):
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
            msg = f"❌ *{arg.upper()}*: Ko có data"
        await update.message.reply_text(msg, parse_mode='Markdown')

async def su(update, ctx):
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /su btc")
    
    s = ctx.args[0].upper()
    if s not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    if not get_price(s): 
        return await update.message.reply_text(f"❌ *{s}* ko tồn tại", parse_mode='Markdown')
    
    if add_subscription(uid, s):
        await update.message.reply_text(f"✅ Đã theo dõi *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo *{s}* rồi", parse_mode='Markdown')

async def uns(update, ctx):
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /uns btc")
    
    s = ctx.args[0].upper()
    remove_subscription(uid, s)
    await update.message.reply_text(f"✅ Đã hủy *{s}*", parse_mode='Markdown')

async def my(update, ctx):
    uid = update.effective_user.id
    subs = get_subscriptions(uid)
    
    if subs:
        msg = "📋 *DS theo dõi:*\n"
        for s in sorted(subs):
            c = price_cache.get(s, {})
            msg += f"• *{s}*: `{fmt_price(c.get('p', '?'))}`\n"
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text("📭 Chưa theo dõi coin nào!")

async def buy(update, ctx):
    uid = update.effective_user.id
    if len(ctx.args) < 3:
        return await update.message.reply_text("❌ /buy btc 0.5 40000")
    
    symbol = ctx.args[0].upper()
    if symbol not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    try:
        amount = float(ctx.args[1])
        buy_price = float(ctx.args[2])
    except:
        return await update.message.reply_text("❌ Số lượng/giá không hợp lệ!")
    
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Coin *{symbol}* không tồn tại!", parse_mode='Markdown')
    
    add_transaction(uid, symbol, amount, buy_price)
    
    current_price = price_data['p']
    profit_loss = (current_price - buy_price) * amount
    profit_loss_percent = ((current_price - buy_price) / buy_price) * 100
    
    msg = (
        f"✅ Đã mua *{symbol}*\n"
        f"📊 SL: `{amount}`\n"
        f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
        f"💵 Vốn: `{fmt_price(amount * buy_price)}`\n"
        f"📈 Giá hiện: `{fmt_price(current_price)}`\n"
        f"📊 LN: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def sell(update, ctx):
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        return await update.message.reply_text("❌ /sell btc 0.2")
    
    symbol = ctx.args[0].upper()
    if symbol not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    try:
        sell_amount = float(ctx.args[1])
    except:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    # Lấy portfolio từ database
    portfolio_data = get_portfolio(uid)
    if not portfolio_data:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    # Chuyển về format cũ
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
        return await update.message.reply_text(f"❌ Không có *{symbol}*", parse_mode='Markdown')
    
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(f"❌ Chỉ có {total_amount} {symbol}")
    
    remaining_sell = sell_amount
    new_portfolio = []
    sold_value = 0
    sold_cost = 0
    
    for tx in portfolio:
        if tx['symbol'] == symbol and remaining_sell > 0:
            if tx['amount'] <= remaining_sell:
                sold_cost += tx['total_cost']
                sold_value += tx['amount'] * get_price(symbol)['p']
                remaining_sell -= tx['amount']
            else:
                sell_part = remaining_sell
                sold_cost += sell_part * tx['buy_price']
                sold_value += sell_part * get_price(symbol)['p']
                tx['amount'] -= sell_part
                tx['total_cost'] = tx['amount'] * tx['buy_price']
                new_portfolio.append(tx)
                remaining_sell = 0
        else:
            new_portfolio.append(tx)
    
    # Cập nhật database
    delete_sold_transactions(uid, new_portfolio)
    
    profit = sold_value - sold_cost
    profit_percent = (profit / sold_cost) * 100 if sold_cost > 0 else 0
    
    msg = (
        f"✅ Đã bán {sell_amount} {symbol}\n"
        f"💰 Giá trị: `{fmt_price(sold_value)}`\n"
        f"📊 Vốn: `{fmt_price(sold_cost)}`\n"
        f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

# ==================== HANDLE MESSAGE ====================

async def handle_message(update, ctx):
    text = update.message.text
    
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text(
            "💰 *MENU ĐẦU TƯ COIN*\nChọn chức năng bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_menu_keyboard()
        )
    elif text == "❓ HƯỚNG DẪN":
        await help(update, ctx)

# ==================== HANDLE CALLBACK ====================

async def handle_callback(update, ctx):
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
            f"⏱ *Thời gian:* `{rate_data['update_time']}`\n"
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
            msg = f"ℹ️ Đang theo *{symbol}* rồi"
        
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
            msg = "📋 *DS THEO DÕI*\n━━━━━━━━━━━━\n\n"
            for s in sorted(subs):
                c = price_cache.get(s, {})
                msg += f"• *{s}*: `{fmt_price(c.get('p', '?'))}`\n"
            
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
                "📭 Danh mục trống!",
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
        
        msg = "📊 *DANH MỤC*\n━━━━━━━━━━━━\n\n"
        
        for symbol, data in portfolio_summary.items():
            price_data = get_price(symbol)
            if price_data:
                current_value = data['amount'] * price_data['p']
                profit = current_value - data['cost']
                profit_percent = (profit / data['cost']) * 100 if data['cost'] > 0 else 0
                
                total_investment += data['cost']
                total_current_value += current_value
                
                msg += f"*{symbol}*\n"
                msg += f"📊 SL: `{data['amount']:.4f}`\n"
                msg += f"💰 TB: `{fmt_price(data['cost']/data['amount'])}`\n"
                msg += f"💎 TT: `{fmt_price(current_value)}`\n"
                msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
        
        total_profit = total_current_value - total_investment
        total_profit_percent = (total_profit / total_investment) * 100 if total_investment > 0 else 0
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Vốn: `{fmt_price(total_investment)}`\n"
        msg += f"💰 GT: `{fmt_price(total_current_value)}`\n"
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
                "📭 Danh mục trống!",
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
        total_profit_percent = (total_profit / total_investment) * 100
        
        msg += "━━━━━━━━━━━━\n"
        msg += f"💵 Vốn: `{fmt_price(total_investment)}`\n"
        msg += f"💰 GT: `{fmt_price(total_current_value)}`\n"
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
            "Dùng lệnh: /buy <symbol> <số lượng> <giá>\n\n"
            "Ví dụ:\n"
            "/buy btc 0.5 40000\n"
            "/buy eth 5 2500\n"
            "/buy usdt 1000 1.00\n\n"
            "Chỉ hỗ trợ BTC, ETH, USDT",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_sell":
        await query.edit_message_text(
            "➖ *BÁN COIN*\n\n"
            "Dùng lệnh: /sell <symbol> <số lượng>\n\n"
            "Ví dụ:\n"
            "/sell btc 0.2\n"
            "/sell eth 2\n"
            "/sell usdt 500\n\n"
            "Chỉ hỗ trợ BTC, ETH, USDT",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )
    
    elif data == "show_top10":
        try:
            res = requests.get(
                f"{CMC_API_URL}/cryptocurrency/listings/latest",
                headers={'X-CMC_PRO_API_KEY': CMC_API_KEY},
                params={'limit': 10, 'convert': 'USD'}
            )
            
            if res.status_code == 200:
                data = res.json()['data']
                msg = "📊 *TOP 10 COIN*\n━━━━━━━━━━━━\n\n"
                
                for i, coin in enumerate(data, 1):
                    quote = coin['quote']['USD']
                    msg += (
                        f"{i}. *{coin['symbol']}* - {coin['name']}\n"
                        f"   💰 {fmt_price(quote['price'])}\n"
                        f"   📈 {quote['percent_change_24h']:+.2f}%\n"
                    )
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await query.edit_message_text(
                    "❌ Không thể lấy dữ liệu",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                )
        except:
            await query.edit_message_text(
                "❌ Lỗi kết nối",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
            )

# ==================== AUTO UPDATE ====================

def auto_update():
    """Tự động cập nhật giá cho người theo dõi"""
    while True:
        time.sleep(60)
        
        # Lấy tất cả user có theo dõi
        conn = sqlite3.connect('crypto_bot.db')
        c = conn.cursor()
        c.execute("SELECT DISTINCT user_id FROM subscriptions")
        users = c.fetchall()
        conn.close()
        
        for (uid,) in users:
            subs = get_subscriptions(uid)
            updates = []
            for s in subs:
                d = get_price(s)
                if d:
                    price_cache[s] = d
                    updates.append(f"• *{d['n']}*: `{fmt_price(d['p'])}` ({d['c']:.1f}%)")
            if updates:
                try:
                    app.bot.send_message(uid, "🔄 *Cập nhật:*\n" + "\n".join(updates), parse_mode='Markdown')
                except: 
                    pass

# ==================== MAIN ====================

if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        print("❌ Thiếu TELEGRAM_TOKEN")
        exit()
    
    if not CMC_API_KEY:
        print("⚠️ Cảnh báo: Thiếu CMC_API_KEY")
    
    # Khởi tạo database
    init_database()
    
    print("🚀 Khởi động bot ĐẦU TƯ COIN...")
    print("✅ Database: SQLite (lưu vĩnh viễn)")
    print("✅ Keyboard: [ĐẦU TƯ COIN] [HƯỚNG DẪN]")
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help))
    app.add_handler(CommandHandler("usdt", usdt_command))
    app.add_handler(CommandHandler("s", s))
    app.add_handler(CommandHandler("su", su))
    app.add_handler(CommandHandler("uns", uns))
    app.add_handler(CommandHandler("my", my))
    app.add_handler(CommandHandler("buy", buy))
    app.add_handler(CommandHandler("sell", sell))
    
    # Message handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Callback handler
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Auto update
    threading.Thread(target=auto_update, daemon=True).start()
    
    print("✅ Bot đã sẵn sàng!")
    print("💰 Bấm 'ĐẦU TƯ COIN' để xem menu đầy đủ")
    print("💾 Dữ liệu được lưu trong file crypto_bot.db")
    
    app.run_polling()

import os
import threading
import time
import requests
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

price_cache = {}
user_subs = {}
user_portfolios = {}

# Health check server
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is running!')
    def log_message(self, format, *args): pass

def run_health_server():
    port = int(os.environ.get('PORT', 10000))
    HTTPServer(('0.0.0.0', port), HealthCheckHandler).serve_forever()

def get_price(symbol):
    try:
        clean = symbol.upper().replace('USDT', '').replace('USD', '')
        res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest", 
                          headers={'X-CMC_PRO_API_KEY': CMC_API_KEY},
                          params={'symbol': clean, 'convert': 'USD'}, timeout=10)
        
        if res.status_code == 200:
            data = res.json()['data'][clean]['quote']['USD']
            return {
                'p': data['price'], 'v': data['volume_24h'], 
                'c': data['percent_change_24h'], 'm': data['market_cap'],
                'n': res.json()['data'][clean]['name'],
                'r': res.json()['data'][clean].get('cmc_rank', 'N/A')
            }
    except: return None

def fmt_price(p):
    try:
        p = float(p)
        return f"${p:.6f}" if p < 0.01 else f"${p:.4f}" if p < 1 else f"${p:,.2f}"
    except: return f"${p}"

def fmt_vol(v):
    try:
        v = float(v)
        return f"${v/1e9:.2f}B" if v > 1e9 else f"${v/1e6:.2f}M" if v > 1e6 else f"${v/1e3:.2f}K" if v > 1e3 else f"${v:,.2f}"
    except: return str(v)

# ==================== KEYBOARD FUNCTIONS ====================

def get_main_keyboard():
    """Tạo main keyboard"""
    keyboard = [
        [KeyboardButton("💰 Giá coin"), KeyboardButton("📊 Top 10")],
        [KeyboardButton("🔔 Theo dõi"), KeyboardButton("📋 DS theo dõi")],
        [KeyboardButton("💼 Danh mục"), KeyboardButton("📈 Lợi nhuận")],
        [KeyboardButton("➕ Mua coin"), KeyboardButton("➖ Bán coin")],
        [KeyboardButton("❓ Hướng dẫn")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_price_keyboard():
    """Keyboard cho xem giá"""
    keyboard = [
        [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
         InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
         InlineKeyboardButton("✴️ BNB", callback_data="price_BNB")],
        [InlineKeyboardButton("◎ SOL", callback_data="price_SOL"),
         InlineKeyboardButton("❌ XRP", callback_data="price_XRP"),
         InlineKeyboardButton("💎 ADA", callback_data="price_ADA")],
        [InlineKeyboardButton("🐕 DOGE", callback_data="price_DOGE"),
         InlineKeyboardButton("⚡ DOT", callback_data="price_DOT"),
         InlineKeyboardButton("🔷 MATIC", callback_data="price_MATIC")],
        [InlineKeyboardButton("🏠 Về menu", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_subscribe_keyboard():
    """Keyboard cho theo dõi"""
    keyboard = [
        [InlineKeyboardButton("➕ Theo BTC", callback_data="sub_BTC"),
         InlineKeyboardButton("➕ Theo ETH", callback_data="sub_ETH")],
        [InlineKeyboardButton("➕ Theo BNB", callback_data="sub_BNB"),
         InlineKeyboardButton("➕ Theo SOL", callback_data="sub_SOL")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_portfolio_keyboard():
    """Keyboard cho danh mục"""
    keyboard = [
        [InlineKeyboardButton("📊 Xem danh mục", callback_data="view_portfolio"),
         InlineKeyboardButton("📈 Chi tiết LN", callback_data="view_profit")],
        [InlineKeyboardButton("➕ Thêm coin", callback_data="add_coin"),
         InlineKeyboardButton("➖ Bán coin", callback_data="sell_coin")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_coin_list_keyboard(action, coins):
    """Tạo keyboard danh sách coin động"""
    keyboard = []
    row = []
    for i, coin in enumerate(coins):
        btn = InlineKeyboardButton(coin, callback_data=f"{action}_{coin}")
        row.append(btn)
        if (i + 1) % 3 == 0:  # 3 nút mỗi hàng
            keyboard.append(row)
            row = []
    if row:  # Thêm hàng cuối nếu còn
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS ====================

async def start(update, ctx):
    """Start command với keyboard"""
    welcome_msg = (
        "🚀 *Chào mừng bạn đến với Crypto Bot!*\n\n"
        "🤖 Bot hỗ trợ:\n"
        "• Xem giá coin real-time\n"
        "• Theo dõi biến động giá\n"
        "• Quản lý danh mục đầu tư\n"
        "• Tính lợi nhuận đầu tư\n\n"
        "👇 *Sử dụng keyboard bên dưới để thao tác*"
    )
    await update.message.reply_text(
        welcome_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help(update, ctx):
    """Help command"""
    help_msg = (
        "📘 *HƯỚNG DẪN SỬ DỤNG*\n\n"
        "*🔹 Các nút chức năng:*\n"
        "💰 *Giá coin* - Xem giá các coin phổ biến\n"
        "📊 *Top 10* - Top 10 coin theo vốn hóa\n"
        "🔔 *Theo dõi* - Theo dõi biến động giá\n"
        "📋 *DS theo dõi* - Danh sách coin đang theo\n"
        "💼 *Danh mục* - Quản lý danh mục đầu tư\n"
        "📈 *Lợi nhuận* - Xem chi tiết lợi nhuận\n"
        "➕ *Mua coin* - Thêm giao dịch mua\n"
        "➖ *Bán coin* - Bán coin trong danh mục\n\n"
        
        "*🔸 Hoặc dùng lệnh:*\n"
        "/s btc - Xem giá BTC\n"
        "/su btc - Theo dõi BTC\n"
        "/portfolio - Xem danh mục\n"
        "/buy btc 0.5 40000 - Mua BTC"
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

async def handle_message(update, ctx):
    """Xử lý tin nhắn từ keyboard"""
    text = update.message.text
    user_id = update.effective_user.id
    
    if text == "💰 Giá coin":
        await update.message.reply_text(
            "Chọn coin để xem giá:",
            reply_markup=get_price_keyboard()
        )
    
    elif text == "📊 Top 10":
        await show_top10(update)
    
    elif text == "🔔 Theo dõi":
        await update.message.reply_text(
            "Chọn coin để theo dõi:",
            reply_markup=get_subscribe_keyboard()
        )
    
    elif text == "📋 DS theo dõi":
        await my(update, ctx)
    
    elif text == "💼 Danh mục":
        await update.message.reply_text(
            "Quản lý danh mục đầu tư:",
            reply_markup=get_portfolio_keyboard()
        )
    
    elif text == "📈 Lợi nhuận":
        await profit_detail(update, ctx)
    
    elif text == "➕ Mua coin":
        await update.message.reply_text(
            "📝 *Hướng dẫn mua coin:*\n"
            "Gõ lệnh: /buy <symbol> <số lượng> <giá>\n"
            "VD: /buy btc 0.5 40000\n\n"
            "Hoặc chọn coin nhanh bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_coin_list_keyboard("quick_buy", ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA"])
        )
    
    elif text == "➖ Bán coin":
        if user_id in user_portfolios and user_portfolios[user_id]:
            coins = list(set([tx['symbol'] for tx in user_portfolios[user_id]]))
            await update.message.reply_text(
                "Chọn coin muốn bán:",
                reply_markup=get_coin_list_keyboard("quick_sell", coins[:9])  # Tối đa 9 coin
            )
        else:
            await update.message.reply_text("📭 Bạn chưa có coin nào trong danh mục!")
    
    elif text == "❓ Hướng dẫn":
        await help(update, ctx)

async def handle_callback(update, ctx):
    """Xử lý callback từ inline keyboard"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "back_to_menu":
        await query.edit_message_text(
            "🏠 *Menu chính*\nChọn chức năng bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_main_keyboard()
        )
    
    elif data.startswith("price_"):
        symbol = data.replace("price_", "")
        await show_price(query, symbol)
    
    elif data.startswith("sub_"):
        symbol = data.replace("sub_", "")
        await do_subscribe(query, symbol)
    
    elif data == "view_portfolio":
        await show_portfolio(query)
    
    elif data == "view_profit":
        await show_profit_detail(query)
    
    elif data.startswith("quick_buy_"):
        symbol = data.replace("quick_buy_", "")
        await query.edit_message_text(
            f"📝 *Mua {symbol}*\n"
            f"Gõ lệnh: /buy {symbol} <số lượng> <giá>\n"
            f"VD: /buy {symbol} 0.5 40000",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data.startswith("quick_sell_"):
        symbol = data.replace("quick_sell_", "")
        await query.edit_message_text(
            f"📝 *Bán {symbol}*\n"
            f"Gõ lệnh: /sell {symbol} <số lượng>\n"
            f"VD: /sell {symbol} 0.2",
            parse_mode=ParseMode.MARKDOWN
        )

async def show_price(query, symbol):
    """Hiển thị giá coin"""
    data = get_price(symbol)
    if data:
        msg = (
            f"*{data['n']}* #{data['r']}\n"
            f"💰 Giá: `{fmt_price(data['p'])}`\n"
            f"📈 24h: `{data['c']:.2f}%`\n"
            f"📦 Volume: `{fmt_vol(data['v'])}`\n"
            f"💎 Market Cap: `{fmt_vol(data['m'])}`"
        )
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    else:
        msg = f"❌ Không có dữ liệu cho {symbol}"
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    
    await query.edit_message_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def do_subscribe(query, symbol):
    """Theo dõi coin"""
    user_id = query.from_user.id
    
    if user_id not in user_subs:
        user_subs[user_id] = []
    
    if symbol not in user_subs[user_id]:
        user_subs[user_id].append(symbol)
        msg = f"✅ Đã theo dõi *{symbol}*"
    else:
        msg = f"ℹ️ Bạn đang theo dõi *{symbol}* rồi"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    await query.edit_message_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_top10(update):
    """Hiển thị top 10 coin"""
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
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Không thể lấy dữ liệu top 10")
    except:
        await update.message.reply_text("❌ Lỗi khi lấy dữ liệu")

async def show_portfolio(query):
    """Hiển thị danh mục"""
    user_id = query.from_user.id
    
    if user_id not in user_portfolios or not user_portfolios[user_id]:
        await query.edit_message_text("📭 Danh mục trống!")
        return
    
    # Tính toán danh mục
    portfolio_summary = {}
    total_investment = 0
    total_current_value = 0
    
    for tx in user_portfolios[user_id]:
        symbol = tx['symbol']
        if symbol not in portfolio_summary:
            portfolio_summary[symbol] = {
                'amount': 0,
                'cost': 0
            }
        portfolio_summary[symbol]['amount'] += tx['amount']
        portfolio_summary[symbol]['cost'] += tx['total_cost']
    
    msg = "📊 *DANH MỤC*\n━━━━━━━━━━━━\n\n"
    
    for symbol, data in portfolio_summary.items():
        price_data = get_price(symbol)
        if price_data:
            current_value = data['amount'] * price_data['p']
            profit = current_value - data['cost']
            profit_percent = (profit / data['cost']) * 100
            
            total_investment += data['cost']
            total_current_value += current_value
            
            msg += f"*{symbol}*\n"
            msg += f"📊 SL: `{data['amount']:.4f}`\n"
            msg += f"💰 TB: `{fmt_price(data['cost']/data['amount'])}`\n"
            msg += f"💎 TT: `{fmt_price(current_value)}`\n"
            msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
    
    total_profit = total_current_value - total_investment
    total_profit_percent = (total_profit / total_investment) * 100
    
    msg += "━━━━━━━━━━━━\n"
    msg += f"💵 Vốn: `{fmt_price(total_investment)}`\n"
    msg += f"💰 GT: `{fmt_price(total_current_value)}`\n"
    msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    await query.edit_message_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== EXISTING FUNCTIONS ====================

async def s(update, ctx):
    if not ctx.args:
        return await update.message.reply_text("❌ /s btc eth")
    
    for arg in ctx.args:
        d = get_price(arg)
        if d:
            msg = f"*{d['n']}* #{d['r']}\n💰 `{fmt_price(d['p'])}`\n📈 `{d['c']:.2f}%`\n📦 `{fmt_vol(d['v'])}`\n💎 `{fmt_vol(d['m'])}`"
            price_cache[arg.upper()] = d
        else:
            msg = f"❌ *{arg.upper()}*: Ko có data"
        await update.message.reply_text(msg, parse_mode='Markdown')

async def su(update, ctx):
    uid = update.effective_user.id
    if not ctx.args: return await update.message.reply_text("❌ /su btc")
    
    s = ctx.args[0].upper()
    if not get_price(s): return await update.message.reply_text(f"❌ *{s}* ko tồn tại", parse_mode='Markdown')
    
    if uid not in user_subs: user_subs[uid] = []
    if s not in user_subs[uid]:
        user_subs[uid].append(s)
        await update.message.reply_text(f"✅ Đã theo dõi *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo *{s}* rồi", parse_mode='Markdown')

async def uns(update, ctx):
    uid = update.effective_user.id
    if not ctx.args: return await update.message.reply_text("❌ /uns btc")
    
    s = ctx.args[0].upper()
    if uid in user_subs and s in user_subs[uid]:
        user_subs[uid].remove(s)
        await update.message.reply_text(f"✅ Đã hủy *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"❌ Ko theo *{s}*", parse_mode='Markdown')

async def my(update, ctx):
    uid = update.effective_user.id
    if uid in user_subs and user_subs[uid]:
        msg = "📋 *DS theo dõi:*\n"
        for s in sorted(user_subs[uid]):
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
    try:
        amount = float(ctx.args[1])
        buy_price = float(ctx.args[2])
    except:
        return await update.message.reply_text("❌ Số lượng/giá không hợp lệ!")
    
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Coin *{symbol}* không tồn tại!", parse_mode='Markdown')
    
    if uid not in user_portfolios:
        user_portfolios[uid] = []
    
    user_portfolios[uid].append({
        'symbol': symbol,
        'amount': amount,
        'buy_price': buy_price,
        'buy_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_cost': amount * buy_price
    })
    
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
    try:
        sell_amount = float(ctx.args[1])
    except:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    # Xử lý bán (FIFO)
    symbol_txs = [tx for tx in user_portfolios[uid] if tx['symbol'] == symbol]
    if not symbol_txs:
        return await update.message.reply_text(f"❌ Không có *{symbol}*", parse_mode='Markdown')
    
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(f"❌ Chỉ có {total_amount} {symbol}")
    
    # Bán FIFO
    remaining_sell = sell_amount
    new_portfolio = []
    sold_value = 0
    sold_cost = 0
    
    for tx in user_portfolios[uid]:
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
    
    user_portfolios[uid] = new_portfolio
    
    profit = sold_value - sold_cost
    profit_percent = (profit / sold_cost) * 100
    
    msg = (
        f"✅ Đã bán {sell_amount} {symbol}\n"
        f"💰 Giá trị: `{fmt_price(sold_value)}`\n"
        f"📊 Vốn: `{fmt_price(sold_cost)}`\n"
        f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def portfolio(update, ctx):
    uid = update.effective_user.id
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    # Tính toán danh mục
    portfolio_summary = {}
    total_investment = 0
    total_current_value = 0
    
    for tx in user_portfolios[uid]:
        symbol = tx['symbol']
        if symbol not in portfolio_summary:
            portfolio_summary[symbol] = {
                'amount': 0,
                'cost': 0
            }
        portfolio_summary[symbol]['amount'] += tx['amount']
        portfolio_summary[symbol]['cost'] += tx['total_cost']
    
    msg = "📊 *DANH MỤC*\n━━━━━━━━━━━━\n\n"
    
    for symbol, data in portfolio_summary.items():
        price_data = get_price(symbol)
        if price_data:
            current_value = data['amount'] * price_data['p']
            profit = current_value - data['cost']
            profit_percent = (profit / data['cost']) * 100
            
            total_investment += data['cost']
            total_current_value += current_value
            
            msg += f"*{symbol}*\n"
            msg += f"📊 SL: `{data['amount']:.4f}`\n"
            msg += f"💰 TB: `{fmt_price(data['cost']/data['amount'])}`\n"
            msg += f"💎 TT: `{fmt_price(current_value)}`\n"
            msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
    
    total_profit = total_current_value - total_investment
    total_profit_percent = (total_profit / total_investment) * 100
    
    msg += "━━━━━━━━━━━━\n"
    msg += f"💵 Vốn: `{fmt_price(total_investment)}`\n"
    msg += f"💰 GT: `{fmt_price(total_current_value)}`\n"
    msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def profit_detail(update, ctx):
    uid = update.effective_user.id
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    msg = "📈 *CHI TIẾT LỢI NHUẬN*\n━━━━━━━━━━━━\n\n"
    
    total_investment = 0
    total_current_value = 0
    
    for i, tx in enumerate(user_portfolios[uid], 1):
        symbol = tx['symbol']
        price_data = get_price(symbol)
        
        if price_data:
            current_value = tx['amount'] * price_data['p']
            profit = current_value - tx['total_cost']
            profit_percent = (profit / tx['total_cost']) * 100
            
            total_investment += tx['total_cost']
            total_current_value += current_value
            
            msg += f"*GD #{i}: {symbol}*\n"
            msg += f"📅 {tx['buy_date']}\n"
            msg += f"📊 SL: `{tx['amount']:.4f}`\n"
            msg += f"💰 Giá mua: `{fmt_price(tx['buy_price'])}`\n"
            msg += f"💎 Giá trị: `{fmt_price(current_value)}`\n"
            msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
    
    total_profit = total_current_value - total_investment
    total_profit_percent = (total_profit / total_investment) * 100
    
    msg += "━━━━━━━━━━━━\n"
    msg += f"💵 Vốn: `{fmt_price(total_investment)}`\n"
    msg += f"💰 GT: `{fmt_price(total_current_value)}`\n"
    msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

def auto_update():
    while True:
        time.sleep(60)
        for uid, symbols in user_subs.items():
            updates = []
            for s in symbols:
                d = get_price(s)
                if d:
                    price_cache[s] = d
                    updates.append(f"• *{d['n']}*: `{fmt_price(d['p'])}` ({d['c']:.1f}%)")
            if updates:
                try:
                    app.bot.send_message(uid, "🔄 *Cập nhật:*\n" + "\n".join(updates), parse_mode='Markdown')
                except: pass

if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        print("❌ Thiếu TELEGRAM_TOKEN")
        exit()
    
    if not CMC_API_KEY:
        print("⚠️ Cảnh báo: Thiếu CMC_API_KEY, một số chức năng có thể không hoạt động")
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help))
    app.add_handler(CommandHandler("s", s))
    app.add_handler(CommandHandler("su", su))
    app.add_handler(CommandHandler("uns", uns))
    app.add_handler(CommandHandler("my", my))
    app.add_handler(CommandHandler("buy", buy))
    app.add_handler(CommandHandler("sell", sell))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("profit", profit_detail))
    
    # Message handler cho keyboard
    from telegram.ext import MessageHandler, filters
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Callback handler cho inline keyboard
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    threading.Thread(target=auto_update, daemon=True).start()
    print("🚀 Bot đang chạy với Keyboard...")
    app.run_polling()

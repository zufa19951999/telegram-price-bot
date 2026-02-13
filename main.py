import os
import threading
import time
import requests
import json
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

price_cache = {}
user_subs = {}
user_portfolios = {}
usdt_cache = {'rate': None, 'time': None}

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

# ==================== HÀM LẤY GIÁ COIN ====================

def get_price(symbol):
    """Lấy giá coin từ CoinMarketCap"""
    try:
        # Xử lý symbol
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
    
    # Kiểm tra cache (3 phút)
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
    
    # Nguồn 3: Exchange Rate
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        res = requests.get(url, timeout=5)
        if res.status_code == 200:
            usd_vnd = float(res.json()['rates']['VND'])
            
            result = {
                'source': 'ExchangeRate',
                'vnd': usd_vnd,
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
    """Định dạng giá USD"""
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
    """Định dạng giá VND"""
    try:
        p = float(p)
        return f"₫{p:,.0f}"
    except:
        return f"₫{p}"

def fmt_vol(v):
    """Định dạng volume"""
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
    """Định dạng phần trăm"""
    try:
        value = float(value)
        emoji = "📈" if value > 0 else "📉" if value < 0 else "➡️"
        return f"{emoji} {value:+.2f}%"
    except:
        return str(value)

# ==================== KEYBOARD ====================

def get_main_keyboard():
    """Tạo main keyboard - ĐẦU TƯ COIN"""
    keyboard = [
        [KeyboardButton("💰 ĐẦU TƯ COIN")],
        [KeyboardButton("📊 Top 10"), KeyboardButton("🔔 Theo dõi")],
        [KeyboardButton("📋 DS theo dõi"), KeyboardButton("💼 Danh mục")],
        [KeyboardButton("📈 Lợi nhuận"), KeyboardButton("➕ Mua coin")],
        [KeyboardButton("➖ Bán coin"), KeyboardButton("❓ Hướng dẫn")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_invest_keyboard():
    """Keyboard cho đầu tư coin - chỉ BTC ETH USDT"""
    keyboard = [
        [InlineKeyboardButton("₿ BITCOIN (BTC)", callback_data="price_BTC"),
         InlineKeyboardButton("Ξ ETHEREUM (ETH)", callback_data="price_ETH")],
        [InlineKeyboardButton("💵 TETHER (USDT)", callback_data="price_USDT")],
        [InlineKeyboardButton("🏠 Về menu", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS ====================

async def start(update, ctx):
    """Start command"""
    welcome_msg = (
        "🚀 *ĐẦU TƯ COIN BOT*\n\n"
        "🤖 Bot hỗ trợ:\n"
        "• Xem giá BTC/ETH/USDT (có USDT/VND)\n"
        "• Xem tỷ giá USDT/VND (/usdt)\n"
        "• Theo dõi biến động giá\n"
        "• Quản lý danh mục đầu tư\n"
        "• Tính lợi nhuận\n\n"
        "👇 *Sử dụng keyboard bên dưới*"
    )
    await update.message.reply_text(
        welcome_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help(update, ctx):
    """Help command"""
    help_msg = (
        "📘 *HƯỚNG DẪN*\n\n"
        "*Lệnh:*\n"
        "/usdt - Xem tỷ giá USDT/VND\n"
        "/s btc - Xem giá BTC\n"
        "/su btc - Theo dõi BTC\n"
        "/uns btc - Hủy theo dõi\n"
        "/my - DS theo dõi\n"
        "/portfolio - Xem danh mục\n"
        "/profit - Chi tiết lợi nhuận\n"
        "/buy btc 0.5 40000 - Mua BTC\n"
        "/sell btc 0.2 - Bán BTC\n\n"
        "*Nút bấm:* Dùng keyboard bên dưới"
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

async def usdt_command(update, ctx):
    """Lệnh /usdt - Xem tỷ giá USDT/VND"""
    
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
    
    keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")]]
    
    await msg.delete()
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def s(update, ctx):
    """Xem giá coin bằng lệnh"""
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
    """Theo dõi coin"""
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /su btc")
    
    s = ctx.args[0].upper()
    if s not in ['BTC', 'ETH', 'USDT']:
        return await update.message.reply_text("❌ Chỉ hỗ trợ BTC, ETH, USDT")
    
    if not get_price(s): 
        return await update.message.reply_text(f"❌ *{s}* ko tồn tại", parse_mode='Markdown')
    
    if uid not in user_subs: 
        user_subs[uid] = []
    
    if s not in user_subs[uid]:
        user_subs[uid].append(s)
        await update.message.reply_text(f"✅ Đã theo dõi *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"ℹ️ Đang theo *{s}* rồi", parse_mode='Markdown')

async def uns(update, ctx):
    """Hủy theo dõi coin"""
    uid = update.effective_user.id
    if not ctx.args: 
        return await update.message.reply_text("❌ /uns btc")
    
    s = ctx.args[0].upper()
    if uid in user_subs and s in user_subs[uid]:
        user_subs[uid].remove(s)
        await update.message.reply_text(f"✅ Đã hủy *{s}*", parse_mode='Markdown')
    else:
        await update.message.reply_text(f"❌ Ko theo *{s}*", parse_mode='Markdown')

async def my(update, ctx):
    """Danh sách theo dõi"""
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
    """Mua coin"""
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
    """Bán coin"""
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
    
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    symbol_txs = [tx for tx in user_portfolios[uid] if tx['symbol'] == symbol]
    if not symbol_txs:
        return await update.message.reply_text(f"❌ Không có *{symbol}*", parse_mode='Markdown')
    
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(f"❌ Chỉ có {total_amount} {symbol}")
    
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
    profit_percent = (profit / sold_cost) * 100 if sold_cost > 0 else 0
    
    msg = (
        f"✅ Đã bán {sell_amount} {symbol}\n"
        f"💰 Giá trị: `{fmt_price(sold_value)}`\n"
        f"📊 Vốn: `{fmt_price(sold_cost)}`\n"
        f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def portfolio(update, ctx):
    """Xem danh mục"""
    uid = update.effective_user.id
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục trống!")
    
    portfolio_summary = {}
    total_investment = 0
    total_current_value = 0
    
    for tx in user_portfolios[uid]:
        symbol = tx['symbol']
        if symbol not in portfolio_summary:
            portfolio_summary[symbol] = {'amount': 0, 'cost': 0}
        portfolio_summary[symbol]['amount'] += tx['amount']
        portfolio_summary[symbol]['cost'] += tx['total_cost']
    
    msg = "📊 *DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━\n\n"
    
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
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def profit(update, ctx):
    """Chi tiết lợi nhuận"""
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
    except Exception as e:
        await update.message.reply_text(f"❌ Lỗi: {e}")

# ==================== HANDLE MESSAGE ====================

async def handle_message(update, ctx):
    """Xử lý tin nhắn từ keyboard"""
    text = update.message.text
    
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text(
            "💰 *ĐẦU TƯ COIN*\nChọn coin để xem giá:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_keyboard()
        )
    elif text == "📊 Top 10":
        await show_top10(update)
    elif text == "🔔 Theo dõi":
        await update.message.reply_text("Dùng lệnh /su btc để theo dõi (BTC/ETH/USDT)")
    elif text == "📋 DS theo dõi":
        await my(update, ctx)
    elif text == "💼 Danh mục":
        await portfolio(update, ctx)
    elif text == "📈 Lợi nhuận":
        await profit(update, ctx)
    elif text == "➕ Mua coin":
        await update.message.reply_text("Dùng lệnh /buy btc 0.5 40000 (BTC/ETH/USDT)")
    elif text == "➖ Bán coin":
        await update.message.reply_text("Dùng lệnh /sell btc 0.2 (BTC/ETH/USDT)")
    elif text == "❓ Hướng dẫn":
        await help(update, ctx)

# ==================== HANDLE CALLBACK ====================

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
                    [InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
        
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
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ==================== AUTO UPDATE ====================

def auto_update():
    """Tự động cập nhật giá"""
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
                except: 
                    pass

# ==================== MAIN ====================

if __name__ == '__main__':
    if not TELEGRAM_TOKEN:
        print("❌ Thiếu TELEGRAM_TOKEN")
        exit()
    
    if not CMC_API_KEY:
        print("⚠️ Cảnh báo: Thiếu CMC_API_KEY")
    
    print("🚀 Khởi động bot ĐẦU TƯ COIN...")
    print("✅ Hỗ trợ: BTC, ETH, USDT")
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
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
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("profit", profit))
    
    # Message handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Callback handler
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Auto update
    threading.Thread(target=auto_update, daemon=True).start()
    
    print("✅ Bot đã sẵn sàng!")
    print("💰 Bấm 'ĐẦU TƯ COIN' để xem BTC/ETH/USDT")
    print("📝 Gõ /usdt để xem tỷ giá")
    
    app.run_polling()

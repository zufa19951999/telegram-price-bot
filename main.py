import os
import threading
import time
import requests
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

price_cache = {}
user_subs = {}
user_portfolios = {}  # Lưu danh mục đầu tư của user

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

def fmt_percent(value):
    try:
        value = float(value)
        emoji = "📈" if value > 0 else "📉" if value < 0 else "➡️"
        return f"{emoji} {value:+.2f}%"
    except:
        return str(value)

async def start(update, ctx):
    await update.message.reply_text(
        "🚀 *Crypto Bot*\n\n"
        "📊 *Giá cả:*\n"
        "💰 /s btc - Giá BTC\n"
        "🔔 /su btc - Theo dõi giá\n"
        "❌ /uns btc - Hủy theo dõi\n"
        "📋 /my - DS theo dõi\n\n"
        "💼 *Đầu tư:*\n"
        "➕ /buy btc 0.5 40000 - Mua 0.5 BTC giá $40,000\n"
        "➖ /sell btc 0.2 - Bán 0.2 BTC\n"
        "📊 /portfolio - Xem tổng danh mục\n"
        "📝 /add btc 0.5 - Thêm coin vào danh mục (không cần giá)\n"
        "📈 /profit - Xem lợi nhuận chi tiết\n\n"
        "ℹ️ /help - HD chi tiết",
        parse_mode='Markdown'
    )

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

async def add_to_portfolio(update, ctx):
    """Thêm coin vào danh mục mà không cần giá mua"""
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        return await update.message.reply_text(
            "❌ Cú pháp: /add <symbol> <số lượng>\n"
            "VD: /add btc 0.5"
        )
    
    symbol = ctx.args[0].upper()
    try:
        amount = float(ctx.args[1])
    except:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    # Kiểm tra coin có tồn tại không
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Coin *{symbol}* không tồn tại!", parse_mode='Markdown')
    
    # Khởi tạo portfolio cho user nếu chưa có
    if uid not in user_portfolios:
        user_portfolios[uid] = []
    
    # Thêm coin vào danh mục
    current_price = price_data['p']
    user_portfolios[uid].append({
        'symbol': symbol,
        'amount': amount,
        'buy_price': current_price,  # Lưu giá hiện tại làm giá mua
        'buy_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_cost': amount * current_price
    })
    
    msg = (
        f"✅ Đã thêm *{symbol}* vào danh mục\n"
        f"📊 Số lượng: `{amount}`\n"
        f"💰 Giá hiện tại: `{fmt_price(current_price)}`\n"
        f"💵 Tổng giá trị: `{fmt_price(amount * current_price)}`"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def buy(update, ctx):
    """Mua coin (thêm vào danh mục với giá mua cụ thể)"""
    uid = update.effective_user.id
    if len(ctx.args) < 3:
        return await update.message.reply_text(
            "❌ Cú pháp: /buy <symbol> <số lượng> <giá mua>\n"
            "VD: /buy btc 0.5 40000"
        )
    
    symbol = ctx.args[0].upper()
    try:
        amount = float(ctx.args[1])
        buy_price = float(ctx.args[2])
    except:
        return await update.message.reply_text("❌ Số lượng hoặc giá không hợp lệ!")
    
    # Kiểm tra coin có tồn tại không
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Coin *{symbol}* không tồn tại!", parse_mode='Markdown')
    
    # Khởi tạo portfolio cho user nếu chưa có
    if uid not in user_portfolios:
        user_portfolios[uid] = []
    
    # Thêm giao dịch mua vào danh mục
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
        f"📊 Số lượng: `{amount}`\n"
        f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
        f"💵 Tổng vốn: `{fmt_price(amount * buy_price)}`\n"
        f"📈 Giá hiện tại: `{fmt_price(current_price)}`\n"
        f"📊 Lợi nhuận: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def sell(update, ctx):
    """Bán coin (xóa khỏi danh mục)"""
    uid = update.effective_user.id
    if len(ctx.args) < 2:
        return await update.message.reply_text(
            "❌ Cú pháp: /sell <symbol> <số lượng>\n"
            "VD: /sell btc 0.2"
        )
    
    symbol = ctx.args[0].upper()
    try:
        sell_amount = float(ctx.args[1])
    except:
        return await update.message.reply_text("❌ Số lượng không hợp lệ!")
    
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục của bạn đang trống!")
    
    # Lọc các giao dịch của coin cần bán
    symbol_txs = [tx for tx in user_portfolios[uid] if tx['symbol'] == symbol]
    if not symbol_txs:
        return await update.message.reply_text(f"❌ Bạn không có *{symbol}* trong danh mục!", parse_mode='Markdown')
    
    # Tính tổng số lượng coin đang có
    total_amount = sum(tx['amount'] for tx in symbol_txs)
    if sell_amount > total_amount:
        return await update.message.reply_text(
            f"❌ Bạn chỉ có {total_amount} {symbol}, không thể bán {sell_amount}!",
            parse_mode='Markdown'
        )
    
    # Bán theo FIFO (First In First Out)
    remaining_sell = sell_amount
    sold_txs = []
    new_portfolio = []
    
    for tx in user_portfolios[uid]:
        if tx['symbol'] == symbol and remaining_sell > 0:
            if tx['amount'] <= remaining_sell:
                # Bán toàn bộ giao dịch này
                sold_txs.append({
                    'amount': tx['amount'],
                    'buy_price': tx['buy_price'],
                    'buy_date': tx['buy_date']
                })
                remaining_sell -= tx['amount']
            else:
                # Bán một phần
                sold_txs.append({
                    'amount': remaining_sell,
                    'buy_price': tx['buy_price'],
                    'buy_date': tx['buy_date']
                })
                # Giữ lại phần còn lại
                tx['amount'] -= remaining_sell
                new_portfolio.append(tx)
                remaining_sell = 0
        else:
            new_portfolio.append(tx)
    
    user_portfolios[uid] = new_portfolio
    
    # Tính toán kết quả bán
    current_price = get_price(symbol)['p']
    total_sold_amount = sum(tx['amount'] for tx in sold_txs)
    total_cost = sum(tx['amount'] * tx['buy_price'] for tx in sold_txs)
    total_revenue = total_sold_amount * current_price
    profit_loss = total_revenue - total_cost
    profit_loss_percent = (profit_loss / total_cost) * 100 if total_cost > 0 else 0
    
    msg = (
        f"✅ Đã bán *{sell_amount} {symbol}*\n"
        f"💰 Giá bán: `{fmt_price(current_price)}`\n"
        f"💵 Giá trị bán: `{fmt_price(total_revenue)}`\n"
        f"📊 Vốn gốc: `{fmt_price(total_cost)}`\n"
        f"📈 Lợi nhuận: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)"
    )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def portfolio(update, ctx):
    """Xem tổng danh mục đầu tư"""
    uid = update.effective_user.id
    
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text(
            "📭 Danh mục của bạn đang trống!\n"
            "Thêm coin: /add btc 0.5 hoặc /buy btc 0.5 40000"
        )
    
    # Nhóm các giao dịch theo coin
    portfolio_summary = {}
    total_investment = 0
    total_current_value = 0
    
    for tx in user_portfolios[uid]:
        symbol = tx['symbol']
        if symbol not in portfolio_summary:
            portfolio_summary[symbol] = {
                'total_amount': 0,
                'total_cost': 0,
                'tx_count': 0
            }
        
        portfolio_summary[symbol]['total_amount'] += tx['amount']
        portfolio_summary[symbol]['total_cost'] += tx['total_cost']
        portfolio_summary[symbol]['tx_count'] += 1
    
    # Tính giá trị hiện tại và lợi nhuận
    msg = "📊 *DANH MỤC ĐẦU TƯ*\n"
    msg += "━━━━━━━━━━━━━━━━\n\n"
    
    for symbol, data in portfolio_summary.items():
        price_data = get_price(symbol)
        if price_data:
            current_price = price_data['p']
            current_value = data['total_amount'] * current_price
            profit_loss = current_value - data['total_cost']
            profit_loss_percent = (profit_loss / data['total_cost']) * 100 if data['total_cost'] > 0 else 0
            
            total_investment += data['total_cost']
            total_current_value += current_value
            
            # Emoji cho lợi nhuận
            profit_emoji = "✅" if profit_loss >= 0 else "❌"
            
            msg += f"*{symbol}* {price_data['n']}\n"
            msg += f"📊 SL: `{data['total_amount']:.4f}`\n"
            msg += f"💰 TB giá: `{fmt_price(data['total_cost'] / data['total_amount'])}`\n"
            msg += f"💵 Hiện tại: `{fmt_price(current_price)}`\n"
            msg += f"💎 Giá trị: `{fmt_price(current_value)}`\n"
            msg += f"{profit_emoji} LN: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)\n"
            msg += f"📝 {data['tx_count']} GD\n\n"
    
    # Tổng kết
    total_profit_loss = total_current_value - total_investment
    total_profit_loss_percent = (total_profit_loss / total_investment) * 100 if total_investment > 0 else 0
    total_emoji = "✅" if total_profit_loss >= 0 else "❌"
    
    msg += "━━━━━━━━━━━━━━━━\n"
    msg += f"💵 *Tổng vốn:* `{fmt_price(total_investment)}`\n"
    msg += f"💰 *Tổng giá trị:* `{fmt_price(total_current_value)}`\n"
    msg += f"{total_emoji} *Tổng LN:* `{fmt_price(total_profit_loss)}` ({total_profit_loss_percent:+.2f}%)\n"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def profit_detail(update, ctx):
    """Xem chi tiết lợi nhuận từng giao dịch"""
    uid = update.effective_user.id
    
    if uid not in user_portfolios or not user_portfolios[uid]:
        return await update.message.reply_text("📭 Danh mục của bạn đang trống!")
    
    msg = "📈 *CHI TIẾT LỢI NHUẬN*\n"
    msg += "━━━━━━━━━━━━━━━━\n\n"
    
    total_investment = 0
    total_current_value = 0
    
    for i, tx in enumerate(user_portfolios[uid], 1):
        symbol = tx['symbol']
        price_data = get_price(symbol)
        
        if price_data:
            current_price = price_data['p']
            current_value = tx['amount'] * current_price
            profit_loss = current_value - tx['total_cost']
            profit_loss_percent = (profit_loss / tx['total_cost']) * 100
            
            total_investment += tx['total_cost']
            total_current_value += current_value
            
            profit_emoji = "✅" if profit_loss >= 0 else "❌"
            
            msg += f"*GD #{i}: {symbol}*\n"
            msg += f"📅 Ngày: `{tx['buy_date']}`\n"
            msg += f"📊 SL: `{tx['amount']:.4f}`\n"
            msg += f"💰 Giá mua: `{fmt_price(tx['buy_price'])}`\n"
            msg += f"💵 Giá hiện tại: `{fmt_price(current_price)}`\n"
            msg += f"💎 Giá trị: `{fmt_price(current_value)}`\n"
            msg += f"{profit_emoji} LN: `{fmt_price(profit_loss)}` ({profit_loss_percent:+.2f}%)\n\n"
    
    # Tổng kết
    total_profit_loss = total_current_value - total_investment
    total_profit_loss_percent = (total_profit_loss / total_investment) * 100
    total_emoji = "✅" if total_profit_loss >= 0 else "❌"
    
    msg += "━━━━━━━━━━━━━━━━\n"
    msg += f"💵 *Tổng vốn:* `{fmt_price(total_investment)}`\n"
    msg += f"💰 *Tổng giá trị:* `{fmt_price(total_current_value)}`\n"
    msg += f"{total_emoji} *Tổng LN:* `{fmt_price(total_profit_loss)}` ({total_profit_loss_percent:+.2f}%)\n"
    
    await update.message.reply_text(msg, parse_mode='Markdown')

async def help(update, ctx):
    await update.message.reply_text(
        "📘 *HƯỚNG DẪN CHI TIẾT*\n\n"
        "🔍 *LỆNH GIÁ:*\n"
        "/s btc eth - Xem giá nhiều coin\n"
        "/su btc - Theo dõi giá coin\n"
        "/uns btc - Hủy theo dõi\n"
        "/my - Xem danh sách theo dõi\n\n"
        
        "💼 *QUẢN LÝ DANH MỤC:*\n"
        "/add btc 0.5 - Thêm 0.5 BTC (giá hiện tại)\n"
        "/buy btc 0.5 40000 - Mua 0.5 BTC giá $40k\n"
        "/sell btc 0.2 - Bán 0.2 BTC\n"
        "/portfolio - Xem tổng danh mục\n"
        "/profit - Xem chi tiết lợi nhuận\n\n"
        
        "📊 *CÁC CHỈ SỐ:*\n"
        "• Giá hiện tại\n"
        "• Giá mua trung bình\n"
        "• Tổng vốn đầu tư\n"
        "• Lợi nhuận (tuyệt đối và %)\n"
        "• Số lượng giao dịch\n\n"
        
        "Nguồn dữ liệu: CoinMarketCap",
        parse_mode='Markdown'
    )

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
    if not TELEGRAM_TOKEN or not CMC_API_KEY:
        print("❌ Thiếu token/api key")
        exit()
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help))
    app.add_handler(CommandHandler("s", s))
    app.add_handler(CommandHandler("su", su))
    app.add_handler(CommandHandler("uns", uns))
    app.add_handler(CommandHandler("my", my))
    
    # Portfolio commands
    app.add_handler(CommandHandler("add", add_to_portfolio))
    app.add_handler(CommandHandler("buy", buy))
    app.add_handler(CommandHandler("sell", sell))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("profit", profit_detail))
    
    threading.Thread(target=auto_update, daemon=True).start()
    print("🚀 Bot đang chạy với tính năng quản lý danh mục đầu tư...")
    app.run_polling()

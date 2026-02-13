import os
import threading
import time
import requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CMC_API_KEY = os.getenv('CMC_API_KEY')
CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

price_cache = {}
user_subs = {}

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

async def start(update, ctx):
    await update.message.reply_text(
        "🚀 *Price Bot*\n\n"
        "💰 /s btc - Giá BTC\n"
        "📌 /su btc - Theo dõi\n"
        "❌ /uns btc - Hủy\n"
        "📋 /my - DS theo dõi\n"
        "ℹ️ /help - HD",
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

async def help(update, ctx):
    await update.message.reply_text(
        "📘 *HD*\n"
        "/s btc - Giá\n"
        "/su btc - Theo dõi\n"
        "/uns btc - Hủy\n"
        "/my - DS của tôi\n\n"
        "Nguồn: CoinMarketCap",
        parse_mode='Markdown'
    )

def auto_update():
    while 1:
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
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help))
    app.add_handler(CommandHandler("s", s))
    app.add_handler(CommandHandler("su", su))
    app.add_handler(CommandHandler("uns", uns))
    app.add_handler(CommandHandler("my", my))
    
    threading.Thread(target=auto_update, daemon=True).start()
    print("🚀 Bot đang chạy...")
    app.run_polling()

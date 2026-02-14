import os
import threading
import time
import requests
import json
import sqlite3
import logging
import shutil
import re
import pandas as pd
import io
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
EXPORT_DIR = os.path.join(DATA_DIR, 'exports')

# Tạo thư mục nếu chưa có
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

logger.info(f"📁 Dữ liệu sẽ được lưu tại: {DB_PATH}")
logger.info(f"💾 Backup sẽ được lưu tại: {BACKUP_DIR}")
logger.info(f"📊 File export sẽ được lưu tại: {EXPORT_DIR}")

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
    
    # Bảng portfolio
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

def clean_old_exports(hours=24):
    """Xóa file export cũ hơn 24 giờ"""
    now = time.time()
    for f in os.listdir(EXPORT_DIR):
        if f.startswith('portfolio_') and f.endswith('.xlsx'):
            filepath = os.path.join(EXPORT_DIR, f)
            if os.path.getmtime(filepath) < now - hours * 3600:
                os.remove(filepath)
                logger.info(f"🗑 Đã xóa file export cũ: {f}")

def schedule_cleanup():
    """Chạy dọn dẹp mỗi 6 giờ"""
    while True:
        try:
            clean_old_exports()
            time.sleep(21600)  # 6 giờ
        except Exception as e:
            logger.error(f"Lỗi trong schedule_cleanup: {e}")
            time.sleep(3600)

def schedule_backup():
    """Chạy backup mỗi ngày"""
    while True:
        try:
            backup_database()
            time.sleep(86400)  # 24 giờ
        except Exception as e:
            logger.error(f"Lỗi trong schedule_backup: {e}")
            time.sleep(3600)

# ==================== DATABASE FUNCTIONS ====================

def add_transaction(user_id, symbol, amount, buy_price):
    """Thêm giao dịch mua"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_cost = amount * buy_price
        symbol_upper = symbol.upper()
        
        c.execute('''INSERT INTO portfolio 
                     (user_id, symbol, amount, buy_price, buy_date, total_cost)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (user_id, symbol_upper, amount, buy_price, buy_date, total_cost))
        conn.commit()
        logger.info(f"✅ User {user_id} đã mua {amount} {symbol_upper} giá {buy_price}")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi khi thêm transaction: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_portfolio(user_id):
    """Lấy toàn bộ danh mục"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost 
                     FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
                  (user_id,))
        result = c.fetchall()
        return result
    except Exception as e:
        logger.error(f"❌ Lỗi khi lấy portfolio: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_transaction_detail(user_id):
    """Lấy chi tiết từng giao dịch kèm ID"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost 
                     FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
                  (user_id,))
        result = c.fetchall()
        return result
    except Exception as e:
        logger.error(f"❌ Lỗi khi lấy transaction detail: {e}")
        return []
    finally:
        if conn:
            conn.close()

def update_transaction(transaction_id, user_id, new_amount, new_price):
    """Cập nhật thông tin giao dịch"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''SELECT symbol, amount, buy_price, total_cost 
                     FROM portfolio WHERE id = ? AND user_id = ?''',
                  (transaction_id, user_id))
        old_tx = c.fetchone()
        
        if not old_tx:
            return False
        
        new_total = new_amount * new_price
        
        c.execute('''UPDATE portfolio 
                     SET amount = ?, buy_price = ?, total_cost = ?
                     WHERE id = ? AND user_id = ?''',
                  (new_amount, new_price, new_total, transaction_id, user_id))
        
        conn.commit()
        logger.info(f"✅ Đã cập nhật giao dịch #{transaction_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi khi update transaction: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_transaction(transaction_id, user_id):
    """Xóa một giao dịch"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM portfolio 
                     WHERE id = ? AND user_id = ?''',
                  (transaction_id, user_id))
        conn.commit()
        affected = c.rowcount
        if affected > 0:
            logger.info(f"✅ Đã xóa giao dịch #{transaction_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Lỗi khi xóa transaction: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_sold_transactions(user_id, kept_transactions):
    """Xóa các giao dịch đã bán và cập nhật lại"""
    conn = None
    try:
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
        logger.info(f"✅ Đã cập nhật portfolio cho user {user_id}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi xóa sold transactions: {e}")
    finally:
        if conn:
            conn.close()

# ==================== HÀM XUẤT EXCEL ====================

def export_portfolio_to_excel(user_id):
    """Xuất danh mục đầu tư ra file Excel"""
    try:
        # Lấy dữ liệu
        transactions = get_transaction_detail(user_id)
        
        if not transactions:
            return None, "📭 Không có dữ liệu để xuất!"
        
        # Lấy giá hiện tại
        portfolio_data = []
        summary = {}
        
        for tx in transactions:
            tx_id, symbol, amount, price, date, cost = tx
            
            # Lấy giá hiện tại
            price_data = get_price(symbol)
            current_price = price_data['p'] if price_data else 0
            current_value = amount * current_price
            profit = current_value - cost
            profit_percent = (profit / cost) * 100 if cost > 0 else 0
            
            portfolio_data.append({
                'ID': tx_id,
                'Mã coin': symbol,
                'Số lượng': amount,
                'Giá mua (USD)': price,
                'Ngày mua': date,
                'Tổng vốn (USD)': cost,
                'Giá hiện tại (USD)': current_price,
                'Giá trị hiện tại (USD)': current_value,
                'Lợi nhuận (USD)': profit,
                'Lợi nhuận %': profit_percent
            })
            
            # Tính tổng hợp theo coin
            if symbol not in summary:
                summary[symbol] = {
                    'total_amount': 0,
                    'total_cost': 0,
                    'total_value': 0
                }
            summary[symbol]['total_amount'] += amount
            summary[symbol]['total_cost'] += cost
            summary[symbol]['total_value'] += current_value
        
        # Tạo DataFrame cho chi tiết giao dịch
        df_details = pd.DataFrame(portfolio_data)
        
        # Định dạng số
        pd.options.display.float_format = '{:,.2f}'.format
        
        # Tạo DataFrame cho tổng hợp theo coin
        summary_data = []
        total_invest = 0
        total_value = 0
        
        for symbol, data in summary.items():
            profit = data['total_value'] - data['total_cost']
            profit_percent = (profit / data['total_cost']) * 100 if data['total_cost'] > 0 else 0
            avg_price = data['total_cost'] / data['total_amount'] if data['total_amount'] > 0 else 0
            
            summary_data.append({
                'Mã coin': symbol,
                'Tổng số lượng': data['total_amount'],
                'Giá vốn TB (USD)': avg_price,
                'Tổng vốn (USD)': data['total_cost'],
                'Giá trị hiện tại (USD)': data['total_value'],
                'Lợi nhuận (USD)': profit,
                'Lợi nhuận %': profit_percent
            })
            
            total_invest += data['total_cost']
            total_value += data['total_value']
        
        df_summary = pd.DataFrame(summary_data)
        
        # Tính tổng kết
        total_profit = total_value - total_invest
        total_profit_percent = (total_profit / total_invest) * 100 if total_invest > 0 else 0
        
        summary_total = pd.DataFrame([{
            'Tổng vốn (USD)': total_invest,
            'Tổng giá trị (USD)': total_value,
            'Tổng lợi nhuận (USD)': total_profit,
            'Tỷ suất lợi nhuận %': total_profit_percent
        }])
        
        # Tạo file Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"portfolio_{user_id}_{timestamp}.xlsx"
        filepath = os.path.join(EXPORT_DIR, filename)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df_details.to_excel(writer, sheet_name='Chi tiết giao dịch', index=False)
            df_summary.to_excel(writer, sheet_name='Tổng hợp theo coin', index=False)
            summary_total.to_excel(writer, sheet_name='Tổng kết', index=False)
            
            # Định dạng các sheet
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"✅ Đã tạo file Excel cho user {user_id}: {filename}")
        return filepath, None
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi xuất Excel: {e}")
        return None, f"❌ Lỗi khi xuất file: {str(e)}"

# ==================== HÀM LẤY GIÁ COIN ====================

def get_price(symbol):
    """Lấy giá coin từ CoinMarketCap"""
    try:
        if not CMC_API_KEY:
            logger.error("❌ Thiếu CMC_API_KEY")
            return None
            
        clean_symbol = symbol.upper()
        if clean_symbol == 'USDT':
            clean = 'USDT'
        else:
            clean = clean_symbol.replace('USDT', '').replace('USD', '')
        
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
            if 'data' not in data or clean not in data['data']:
                logger.error(f"Không tìm thấy dữ liệu cho {clean}")
                return None
                
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
    
    try:
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
    except Exception as e:
        logger.error(f"Lỗi get_usdt_vnd_rate: {e}")
        return {'source': 'Error', 'vnd': 25000, 'update_time': datetime.now().strftime('%H:%M:%S %d/%m/%Y')}

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

# ==================== HÀM TÍNH TOÁN ẨN ====================

def tinh_toan(expression):
    """Tính toán biểu thức toán học đơn giản"""
    try:
        # Loại bỏ khoảng trắng
        expr = expression.replace(' ', '')
        
        # Kiểm tra ký tự hợp lệ (chỉ số, dấu cộng, trừ, nhân, chia, mũ, ngoặc)
        if not re.match(r'^[0-9+\-*/%.()]+$', expr):
            return None, "❌ Biểu thức chứa ký tự không hợp lệ!"
        
        # Thay % thành /100
        expr = expr.replace('%', '/100')
        
        # Tính toán an toàn
        result = eval(expr)
        
        # Định dạng kết quả
        if isinstance(result, float):
            if result.is_integer():
                result = int(result)
            else:
                result = round(result, 10)
        
        return result, None
    except ZeroDivisionError:
        return None, "❌ Lỗi: Chia cho 0!"
    except Exception as e:
        return None, f"❌ Lỗi: {str(e)}"

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
         InlineKeyboardButton("💼 Danh mục", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 Lợi nhuận", callback_data="show_profit"),
         InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_transactions")],
        [InlineKeyboardButton("➖ Bán coin", callback_data="show_sell"),
         InlineKeyboardButton("➕ Mua coin", callback_data="show_buy")],
        [InlineKeyboardButton("📥 Xuất Excel", callback_data="export_excel")]
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
        "• Quản lý danh mục đầu tư\n"
        "• ✏️ Sửa/Xóa giao dịch\n"
        "• Tính lợi nhuận chi tiết\n"
        "• 📥 Xuất báo cáo Excel\n\n"
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
        "• `/usdt` - Xem tỷ giá USDT/VND\n\n"
        "*QUẢN LÝ ĐẦU TƯ:*\n"
        "• `/buy btc 0.5 40000` - Mua coin\n"
        "• `/sell btc 0.2` - Bán coin\n"
        "• `/edit` - Xem/sửa giao dịch\n"
        "• `/edit 5` - Xem chi tiết giao dịch #5\n"
        "• `/edit 5 0.8 42000` - Sửa giao dịch #5\n"
        "• `/del 5` - Xóa giao dịch #5\n"
        "• `/export` - Xuất báo cáo Excel\n\n"
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
    
    if add_transaction(uid, symbol, amount, buy_price):
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
    else:
        await update.message.reply_text(f"❌ Lỗi khi thêm giao dịch *{symbol}*", parse_mode='Markdown')

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

async def export_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Command xuất Excel"""
    uid = update.effective_user.id
    msg = await update.message.reply_text("🔄 Đang tạo file Excel...")
    
    filepath, error = export_portfolio_to_excel(uid)
    
    if error:
        await msg.edit_text(error)
        return
    
    try:
        # Gửi file
        with open(filepath, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=os.path.basename(filepath),
                caption="📊 *BÁO CÁO DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!",
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Xóa file sau khi gửi
        os.remove(filepath)
        logger.info(f"🗑 Đã xóa file {filepath}")
        
    except Exception as e:
        logger.error(f"Lỗi khi gửi file: {e}")
        await msg.edit_text("❌ Lỗi khi gửi file. Vui lòng thử lại sau.")
    
    await msg.delete()

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
    else:
        # TÍNH NĂNG ẨN - Kiểm tra xem có phải phép tính không
        if any(op in text for op in ['+', '-', '*', '/', '%']) and not text.startswith('/'):
            result, error = tinh_toan(text)
            if error:
                await update.message.reply_text(error)
            else:
                # CHỈ HIỂN THỊ KẾT QUẢ ĐƠN GIẢN
                if isinstance(result, int):
                    await update.message.reply_text(f"{text} = {result:,}")
                else:
                    formatted_result = f"{result:,.10f}".rstrip('0').rstrip('.') if '.' in str(result) else str(result)
                    await update.message.reply_text(f"{text} = {formatted_result}")

# ==================== HANDLE CALLBACK ====================

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    try:
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
                [InlineKeyboardButton("📥 Xuất Excel", callback_data="export_excel")],
                [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]
            ]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "export_excel":
            uid = query.from_user.id
            await query.edit_message_text("🔄 Đang tạo file Excel...")
            
            filepath, error = export_portfolio_to_excel(uid)
            
            if error:
                await query.edit_message_text(error)
                return
            
            try:
                # Gửi file
                with open(filepath, 'rb') as f:
                    await query.message.reply_document(
                        document=f,
                        filename=os.path.basename(filepath),
                        caption="📊 *BÁO CÁO DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                # Xóa file sau khi gửi
                os.remove(filepath)
                logger.info(f"🗑 Đã xóa file {filepath}")
                
                # Quay lại menu
                await query.edit_message_text(
                    "💰 *MENU ĐẦU TƯ COIN*",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_invest_menu_keyboard()
                )
                
            except Exception as e:
                logger.error(f"Lỗi khi gửi file: {e}")
                await query.edit_message_text(
                    "❌ Lỗi khi gửi file. Vui lòng thử lại sau.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
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
    except Exception as e:
        logger.error(f"Lỗi trong handle_callback: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Có lỗi xảy ra. Vui lòng thử lại sau.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
        )

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
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("sell", sell_command))
    app.add_handler(CommandHandler("edit", edit_command))
    app.add_handler(CommandHandler("del", delete_tx_command))
    app.add_handler(CommandHandler("delete", delete_tx_command))
    app.add_handler(CommandHandler("xoa", delete_tx_command))
    app.add_handler(CommandHandler("export", export_command))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Threads
    threading.Thread(target=schedule_backup, daemon=True).start()
    threading.Thread(target=schedule_cleanup, daemon=True).start()
    threading.Thread(target=run_health_server, daemon=True).start()
    
    logger.info("✅ Bot sẵn sàng!")
    app.run_polling()

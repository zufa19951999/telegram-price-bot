import os
import threading
import time
import requests
import json
import sqlite3
import logging
import shutil
import re
import csv
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode

# THIẾT LẬP MÚI GIỜ VIỆT NAM (UTC+7)
def get_vn_time():
    """Lấy thời gian Việt Nam hiện tại (UTC+7)"""
    return datetime.utcnow() + timedelta(hours=7)

def format_vn_time(format_str="%H:%M:%S %d/%m/%Y"):
    """Format thời gian Việt Nam"""
    return get_vn_time().strftime(format_str)
    
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

DATA_DIR = '/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, 'crypto_bot.db')
BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
EXPORT_DIR = os.path.join(DATA_DIR, 'exports')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

logger.info(f"📁 Dữ liệu sẽ được lưu tại: {DB_PATH}")

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
        
        current_time = get_vn_time().strftime('%Y-%m-%d %H:%M:%S')
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
    
    # Bảng portfolio (ĐẦU TƯ COIN)
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  symbol TEXT,
                  amount REAL,
                  buy_price REAL,
                  buy_date TEXT,
                  total_cost REAL)''')
    
    # Bảng cảnh báo giá (ĐẦU TƯ COIN)
    c.execute('''CREATE TABLE IF NOT EXISTS alerts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  symbol TEXT,
                  target_price REAL,
                  condition TEXT,
                  is_active INTEGER DEFAULT 1,
                  created_at TEXT,
                  triggered_at TEXT)''')
    
    # Bảng danh mục chi tiêu (QUẢN LÝ CHI TIÊU)
    c.execute('''CREATE TABLE IF NOT EXISTS expense_categories
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  name TEXT,
                  budget REAL,
                  created_at TEXT)''')
    
    # Bảng ghi chép chi tiêu (QUẢN LÝ CHI TIÊU) - có hỗ trợ đa tiền tệ
    c.execute('''CREATE TABLE IF NOT EXISTS expenses
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  category_id INTEGER,
                  amount REAL,
                  currency TEXT DEFAULT 'VND',
                  note TEXT,
                  expense_date TEXT,
                  created_at TEXT,
                  FOREIGN KEY (category_id) REFERENCES expense_categories(id))''')
    
    # Bảng thu nhập (QUẢN LÝ CHI TIÊU) - có hỗ trợ đa tiền tệ
    c.execute('''CREATE TABLE IF NOT EXISTS incomes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  amount REAL,
                  currency TEXT DEFAULT 'VND',
                  source TEXT,
                  income_date TEXT,
                  note TEXT,
                  created_at TEXT)''')
    
    conn.commit()
    conn.close()
    logger.info(f"✅ Database initialized at {DB_PATH}")

def backup_database():
    """Tự động backup database"""
    try:
        if os.path.exists(DB_PATH):
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')
            shutil.copy2(DB_PATH, backup_path)
            logger.info(f"✅ Đã backup: {backup_path}")
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
        if f.startswith('portfolio_') and f.endswith('.csv'):
            filepath = os.path.join(EXPORT_DIR, f)
            if os.path.getmtime(filepath) < now - hours * 3600:
                os.remove(filepath)
                logger.info(f"🗑 Đã xóa file export cũ: {f}")

def schedule_cleanup():
    """Chạy dọn dẹp mỗi 6 giờ"""
    while True:
        try:
            clean_old_exports()
            time.sleep(21600)
        except Exception as e:
            logger.error(f"Lỗi trong schedule_cleanup: {e}")
            time.sleep(3600)

def schedule_backup():
    """Chạy backup mỗi ngày"""
    while True:
        try:
            backup_database()
            time.sleep(86400)
        except Exception as e:
            logger.error(f"Lỗi trong schedule_backup: {e}")
            time.sleep(3600)

# ==================== PORTFOLIO DATABASE FUNCTIONS (GIỮ NGUYÊN) ====================

def add_transaction(user_id, symbol, amount, buy_price):
    """Thêm giao dịch mua"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        buy_date = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
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

# ==================== ALERTS FUNCTIONS (GIỮ NGUYÊN) ====================

def add_alert(user_id, symbol, target_price, condition):
    """Thêm cảnh báo giá"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
        symbol_upper = symbol.upper()
        
        c.execute('''INSERT INTO alerts 
                     (user_id, symbol, target_price, condition, created_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (user_id, symbol_upper, target_price, condition, created_at))
        conn.commit()
        logger.info(f"✅ User {user_id} tạo alert {symbol} {condition} {target_price}")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi thêm alert: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_user_alerts(user_id):
    """Lấy danh sách cảnh báo của user"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, symbol, target_price, condition, created_at 
                     FROM alerts 
                     WHERE user_id = ? AND is_active = 1 
                     ORDER BY created_at''', (user_id,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi lấy alerts: {e}")
        return []
    finally:
        if conn:
            conn.close()

def delete_alert(alert_id, user_id):
    """Xóa cảnh báo"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM alerts WHERE id = ? AND user_id = ?", (alert_id, user_id))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        logger.error(f"❌ Lỗi xóa alert: {e}")
        return False
    finally:
        if conn:
            conn.close()

def check_alerts():
    """Kiểm tra cảnh báo giá (chạy background)"""
    global app
    while True:
        try:
            time.sleep(60)
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, user_id, symbol, target_price, condition 
                         FROM alerts WHERE is_active = 1''')
            alerts = c.fetchall()
            conn.close()
            
            for alert in alerts:
                alert_id, user_id, symbol, target_price, condition = alert
                
                price_data = get_price(symbol)
                if not price_data:
                    continue
                
                current_price = price_data['p']
                should_trigger = False
                
                if condition == 'above' and current_price >= target_price:
                    should_trigger = True
                elif condition == 'below' and current_price <= target_price:
                    should_trigger = True
                
                if should_trigger and app:
                    msg = (
                        f"🔔 *CẢNH BÁO GIÁ*\n━━━━━━━━━━━━━━━━\n\n"
                        f"• Coin: *{symbol}*\n"
                        f"• Giá hiện tại: `{fmt_price(current_price)}`\n"
                        f"• Mốc cảnh báo: `{fmt_price(target_price)}`\n"
                        f"• Điều kiện: {'📈 Lên trên' if condition == 'above' else '📉 Xuống dưới'}\n\n"
                        f"🕐 {get_vn_time().strftime('%H:%M:%S %d/%m/%Y')}"
                    )
                    
                    try:
                        app.bot.send_message(user_id, msg, parse_mode='Markdown')
                        
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute('''UPDATE alerts SET is_active = 0, triggered_at = ? 
                                     WHERE id = ?''', 
                                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), alert_id))
                        conn.commit()
                        conn.close()
                        logger.info(f"✅ Đã gửi alert {alert_id} cho user {user_id}")
                    except Exception as e:
                        logger.error(f"❌ Lỗi gửi alert {alert_id}: {e}")
                        
        except Exception as e:
            logger.error(f"❌ Lỗi check_alerts: {e}")
            time.sleep(10)

# ==================== HÀM LẤY GIÁ COIN (GIỮ NGUYÊN) ====================

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
                logger.error(f"❌ Không tìm thấy dữ liệu cho {clean}")
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
            logger.error(f"❌ CMC API error: {res.status_code} - {res.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Lỗi get_price {symbol}: {e}")
        return None

# ==================== HÀM LẤY TỶ GIÁ USDT/VND (GIỮ NGUYÊN) ====================

def get_usdt_vnd_rate():
    """Lấy tỷ giá USDT/VND từ nhiều nguồn"""
    global usdt_cache
    
    try:
        if usdt_cache['rate'] and usdt_cache['time']:
            time_diff = (datetime.now() - usdt_cache['time']).total_seconds()
            if time_diff < 180:
                return usdt_cache['rate']
        
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
            logger.warning(f"⚠️ CoinGecko error: {e}")
        
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
            logger.warning(f"⚠️ Coinbase error: {e}")
        
        result = {
            'source': 'Fallback (25000)',
            'vnd': 25000,
            'update_time': datetime.now().strftime('%H:%M:%S %d/%m/%Y')
        }
        usdt_cache['rate'] = result
        usdt_cache['time'] = datetime.now()
        return result
    except Exception as e:
        logger.error(f"❌ Lỗi get_usdt_vnd_rate: {e}")
        return {'source': 'Error', 'vnd': 25000, 'update_time': datetime.now().strftime('%H:%M:%S %d/%m/%Y')}

# ==================== HÀM ĐỊNH DẠNG (GIỮ NGUYÊN + THÊM CHO ĐA TIỀN TỆ) ====================

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

def fmt_number(n):
    try:
        n = float(n)
        if n.is_integer():
            return f"{int(n):,}"
        else:
            return f"{n:,.2f}"
    except:
        return str(n)

# ==================== HÀM HỖ TRỢ ĐA TIỀN TỆ (CẬP NHẬT) ====================

# Danh sách các loại tiền hỗ trợ (ĐÃ SỬA HKR THÀNH KHR)
SUPPORTED_CURRENCIES = {
    'VND': '🇻🇳 Việt Nam Đồng',
    'USD': '🇺🇸 US Dollar',
    'USDT': '💵 Tether (USDT)',
    'LKR': '🇱🇰 Sri Lanka Rupee',
    'KHR': '🇰🇭 Riel Campuchia',  # Đã sửa từ HKR thành KHR
    'HKD': '🇭🇰 Hong Kong Dollar',
    'SGD': '🇸🇬 Singapore Dollar',
    'JPY': '🇯🇵 Japanese Yen',
    'EUR': '🇪🇺 Euro',
    'GBP': '🇬🇧 British Pound',
    'CNY': '🇨🇳 Chinese Yuan',
    'KRW': '🇰🇷 South Korean Won',
    'THB': '🇹🇭 Thai Baht',
    'MYR': '🇲🇾 Malaysian Ringgit',
    'IDR': '🇮🇩 Indonesian Rupiah',
    'PHP': '🇵🇭 Philippine Peso'
}

def format_currency_amount(amount, currency='VND'):
    """Định dạng số tiền theo loại tiền"""
    try:
        amount = float(amount)
        if currency == 'VND':
            if amount >= 1e6:
                return f"{amount/1e6:.1f}M {currency}"
            elif amount >= 1e3:
                return f"{amount/1e3:.0f}K {currency}"
            else:
                return f"{amount:,.0f} {currency}"
        elif currency in ['USD', 'USDT', 'SGD', 'HKD']:
            return f"${amount:,.2f}"
        elif currency == 'JPY':
            return f"¥{amount:,.0f}"
        elif currency == 'EUR':
            return f"€{amount:,.2f}"
        elif currency == 'GBP':
            return f"£{amount:,.2f}"
        elif currency == 'CNY':
            return f"¥{amount:,.2f}"
        elif currency == 'KRW':
            return f"₩{amount:,.0f}"
        elif currency == 'THB':
            return f"฿{amount:,.2f}"
        elif currency == 'LKR':
            return f"Rs {amount:,.2f}"
        elif currency == 'KHR':
            return f"៛{amount:,.0f}"  # Ký hiệu Riel Campuchia
        else:
            return f"{amount:,.2f} {currency}"
    except:
        return f"{amount} {currency}"

# ==================== HÀM TÍNH TOÁN ẨN (GIỮ NGUYÊN) ====================

def tinh_toan(expression):
    """Tính toán biểu thức toán học đơn giản"""
    try:
        expr = expression.replace(' ', '')
        
        if not re.match(r'^[0-9+\-*/%.()]+$', expr):
            return None, "❌ Biểu thức chứa ký tự không hợp lệ!"
        
        expr = expr.replace('%', '/100')
        
        result = eval(expr)
        
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

# ==================== HÀM THỐNG KÊ PORTFOLIO (GIỮ NGUYÊN) ====================

def get_portfolio_stats(user_id):
    """Lấy thống kê danh mục"""
    try:
        portfolio_data = get_portfolio(user_id)
        
        if not portfolio_data:
            return None
        
        total_invest = 0
        total_value = 0
        coins = {}
        
        for row in portfolio_data:
            symbol, amount, price, date, cost = row[0], row[1], row[2], row[3], row[4]
            
            if symbol not in coins:
                coins[symbol] = {'amount': 0, 'cost': 0}
            coins[symbol]['amount'] += amount
            coins[symbol]['cost'] += cost
            
            total_invest += cost
            
            price_data = get_price(symbol)
            current_price = price_data['p'] if price_data else price
            total_value += amount * current_price
        
        total_profit = total_value - total_invest
        total_profit_percent = (total_profit / total_invest * 100) if total_invest > 0 else 0
        
        coin_profits = []
        for symbol, data in coins.items():
            price_data = get_price(symbol)
            current_price = price_data['p'] if price_data else 0
            current_value = data['amount'] * current_price
            profit = current_value - data['cost']
            profit_pct = (profit / data['cost'] * 100) if data['cost'] > 0 else 0
            coin_profits.append((symbol, profit, profit_pct, current_value, data['cost']))
        
        coin_profits.sort(key=lambda x: x[1], reverse=True)
        
        return {
            'total_invest': total_invest,
            'total_value': total_value,
            'total_profit': total_profit,
            'total_profit_percent': total_profit_percent,
            'coins': coins,
            'coin_profits': coin_profits
        }
    except Exception as e:
        logger.error(f"❌ Lỗi get_portfolio_stats: {e}")
        return None

# ==================== HÀM XUẤT CSV (GIỮ NGUYÊN) ====================

def export_portfolio_to_csv(user_id):
    """Xuất danh mục đầu tư ra file CSV"""
    try:
        transactions = get_transaction_detail(user_id)
        
        if not transactions:
            return None, "📭 Không có dữ liệu để xuất!"
        
        timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
        filename = f"portfolio_{user_id}_{timestamp}.csv"
        filepath = os.path.join(EXPORT_DIR, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            
            writer.writerow(['ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 'Ngày mua', 
                           'Tổng vốn (USD)', 'Giá hiện tại (USD)', 'Giá trị hiện tại (USD)', 
                           'Lợi nhuận (USD)', 'Lợi nhuận %'])
            
            total_invest = 0
            total_value = 0
            
            for tx in transactions:
                tx_id, symbol, amount, price, date, cost = tx
                
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else 0
                current_value = amount * current_price
                profit = current_value - cost
                profit_percent = (profit / cost) * 100 if cost > 0 else 0
                
                writer.writerow([
                    tx_id, 
                    symbol, 
                    f"{amount:.8f}", 
                    f"{price:.2f}", 
                    date,
                    f"{cost:.2f}", 
                    f"{current_price:.2f}", 
                    f"{current_value:.2f}",
                    f"{profit:.2f}", 
                    f"{profit_percent:.2f}"
                ])
                
                total_invest += cost
                total_value += current_value
            
            writer.writerow([])
            writer.writerow(['TỔNG KẾT'])
            writer.writerow(['Tổng vốn (USD)', f"{total_invest:.2f}"])
            writer.writerow(['Tổng giá trị (USD)', f"{total_value:.2f}"])
            writer.writerow(['Tổng lợi nhuận (USD)', f"{total_value - total_invest:.2f}"])
            writer.writerow(['Tỷ suất lợi nhuận %', f"{((total_value - total_invest)/total_invest*100):.2f}" if total_invest > 0 else "0"])
        
        logger.info(f"✅ Đã tạo file CSV cho user {user_id}: {filename}")
        return filepath, None
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi xuất CSV: {e}")
        return None, f"❌ Lỗi khi xuất file: {str(e)}"

# ==================== EXPENSE DATABASE FUNCTIONS (CẬP NHẬT ĐA TIỀN TỆ) ====================

def add_expense_category(user_id, name, budget=0):
    """Thêm danh mục chi tiêu"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute('''INSERT INTO expense_categories 
                     (user_id, name, budget, created_at)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, name.upper(), budget, created_at))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi thêm category: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_expense_categories(user_id):
    """Lấy danh sách category"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, name, budget, created_at 
                     FROM expense_categories WHERE user_id = ? 
                     ORDER BY name''', (user_id,))
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi lấy categories: {e}")
        return []
    finally:
        if conn:
            conn.close()

def delete_expense_category(category_id, user_id):
    """Xóa danh mục chi tiêu"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM expenses 
                     WHERE category_id = ? AND user_id = ?''', (category_id, user_id))
        count = c.fetchone()[0]
        
        if count > 0:
            return False, "Không thể xóa danh mục đã có chi tiêu!"
        
        c.execute('''DELETE FROM expense_categories 
                     WHERE id = ? AND user_id = ?''', (category_id, user_id))
        conn.commit()
        return True, "Đã xóa danh mục"
    except Exception as e:
        logger.error(f"❌ Lỗi xóa category: {e}")
        return False, str(e)
    finally:
        if conn:
            conn.close()

def update_category_budget(category_id, user_id, new_budget):
    """Cập nhật ngân sách danh mục"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''UPDATE expense_categories SET budget = ? 
                     WHERE id = ? AND user_id = ?''',
                  (new_budget, category_id, user_id))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        logger.error(f"❌ Lỗi update budget: {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_income(user_id, amount, source, currency='VND', note=""):
    """Thêm thu nhập với đa tiền tệ"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now = get_vn_time()
        income_date = now.strftime("%Y-%m-%d")
        created_at = now.strftime("%Y-%m-%d %H:%M:%S")
        currency = currency.upper()
        
        c.execute('''INSERT INTO incomes 
                     (user_id, amount, source, income_date, note, created_at, currency)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, amount, source, income_date, note, created_at, currency))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi thêm income: {e}")
        return False
    finally:
        if conn:
            conn.close()

def add_expense(user_id, category_id, amount, currency='VND', note=""):
    """Thêm chi tiêu với đa tiền tệ"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now = get_vn_time()
        expense_date = now.strftime("%Y-%m-%d")
        created_at = now.strftime("%Y-%m-%d %H:%M:%S")
        currency = currency.upper()
        
        c.execute('''INSERT INTO expenses 
                     (user_id, category_id, amount, note, expense_date, created_at, currency)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, category_id, amount, note, expense_date, created_at, currency))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi thêm expense: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_recent_incomes(user_id, limit=10):
    """Lấy thu nhập gần đây (có currency)"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT id, amount, source, note, income_date, currency
                     FROM incomes 
                     WHERE user_id = ?
                     ORDER BY income_date DESC, created_at DESC
                     LIMIT ?''', (user_id, limit))
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi recent incomes: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_recent_expenses(user_id, limit=10):
    """Lấy chi tiêu gần đây (có currency)"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT e.id, ec.name, e.amount, e.note, e.expense_date, e.currency
                     FROM expenses e
                     JOIN expense_categories ec ON e.category_id = ec.id
                     WHERE e.user_id = ?
                     ORDER BY e.expense_date DESC, e.created_at DESC
                     LIMIT ?''', (user_id, limit))
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi recent expenses: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_income_by_period(user_id, period='month'):
    """Lấy thu nhập theo kỳ (có currency)"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        now = get_vn_time()
        
        if period == 'day':
            date_filter = now.strftime("%Y-%m-%d")
            query = '''SELECT source, SUM(amount), COUNT(id), currency
                      FROM incomes 
                      WHERE user_id = ? AND income_date = ?
                      GROUP BY source, currency'''
            c.execute(query, (user_id, date_filter))
        
        elif period == 'month':
            month_filter = now.strftime("%Y-%m")
            query = '''SELECT source, SUM(amount), COUNT(id), currency
                      FROM incomes 
                      WHERE user_id = ? AND strftime('%Y-%m', income_date) = ?
                      GROUP BY source, currency'''
            c.execute(query, (user_id, month_filter))
        
        else:
            year_filter = now.strftime("%Y")
            query = '''SELECT source, SUM(amount), COUNT(id), currency
                      FROM incomes 
                      WHERE user_id = ? AND strftime('%Y', income_date) = ?
                      GROUP BY source, currency'''
            c.execute(query, (user_id, year_filter))
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi income summary: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_expenses_by_period(user_id, period='month'):
    """Lấy chi tiêu theo kỳ (có currency)"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        now = get_vn_time()
        
        if period == 'day':
            date_filter = now.strftime("%Y-%m-%d")
            query = '''SELECT ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                      FROM expenses e
                      JOIN expense_categories ec ON e.category_id = ec.id
                      WHERE e.user_id = ? AND e.expense_date = ?
                      GROUP BY ec.name, ec.budget, e.currency'''
            c.execute(query, (user_id, date_filter))
        
        elif period == 'week':
            start_of_week = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
            end_of_week = (now + timedelta(days=6-now.weekday())).strftime("%Y-%m-%d")
            query = '''SELECT ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                      FROM expenses e
                      JOIN expense_categories ec ON e.category_id = ec.id
                      WHERE e.user_id = ? AND e.expense_date BETWEEN ? AND ?
                      GROUP BY ec.name, ec.budget, e.currency'''
            c.execute(query, (user_id, start_of_week, end_of_week))
        
        elif period == 'month':
            month_filter = now.strftime("%Y-%m")
            query = '''SELECT ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                      FROM expenses e
                      JOIN expense_categories ec ON e.category_id = ec.id
                      WHERE e.user_id = ? AND strftime('%Y-%m', e.expense_date) = ?
                      GROUP BY ec.name, ec.budget, e.currency'''
            c.execute(query, (user_id, month_filter))
        
        else:  # year
            year_filter = now.strftime("%Y")
            query = '''SELECT ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                      FROM expenses e
                      JOIN expense_categories ec ON e.category_id = ec.id
                      WHERE e.user_id = ? AND strftime('%Y', e.expense_date) = ?
                      GROUP BY ec.name, ec.budget, e.currency'''
            c.execute(query, (user_id, year_filter))
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"❌ Lỗi expenses summary: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_total_income_by_period(user_id, period='month'):
    """Tổng thu nhập theo kỳ"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        now = get_vn_time()
        
        if period == 'day':
            date_filter = now.strftime("%Y-%m-%d")
            query = '''SELECT SUM(amount) FROM incomes 
                      WHERE user_id = ? AND income_date = ?'''
            c.execute(query, (user_id, date_filter))
        
        elif period == 'month':
            month_filter = now.strftime("%Y-%m")
            query = '''SELECT SUM(amount) FROM incomes 
                      WHERE user_id = ? AND strftime('%Y-%m', income_date) = ?'''
            c.execute(query, (user_id, month_filter))
        
        else:
            year_filter = now.strftime("%Y")
            query = '''SELECT SUM(amount) FROM incomes 
                      WHERE user_id = ? AND strftime('%Y', income_date) = ?'''
            c.execute(query, (user_id, year_filter))
        
        result = c.fetchone()[0]
        return result or 0
    except Exception as e:
        logger.error(f"❌ Lỗi total income: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def delete_expense(expense_id, user_id):
    """Xóa một khoản chi"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM expenses 
                     WHERE id = ? AND user_id = ?''', (expense_id, user_id))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        logger.error(f"❌ Lỗi xóa expense: {e}")
        return False
    finally:
        if conn:
            conn.close()

def delete_income(income_id, user_id):
    """Xóa một khoản thu"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM incomes 
                     WHERE id = ? AND user_id = ?''', (income_id, user_id))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        logger.error(f"❌ Lỗi xóa income: {e}")
        return False
    finally:
        if conn:
            conn.close()

# ==================== KEYBOARD (GIỮ NGUYÊN + THÊM CHO QUẢN LÝ CHI TIÊU) ====================

def get_main_keyboard():
    """Keyboard chính"""
    keyboard = [
        [KeyboardButton("💰 ĐẦU TƯ COIN"), 
         KeyboardButton("💸 QUẢN LÝ CHI TIÊU")],
        [KeyboardButton("❓ HƯỚNG DẪN")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_invest_menu_keyboard():
    """Keyboard menu đầu tư coin (GIỮ NGUYÊN)"""
    keyboard = [
        [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
         InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
         InlineKeyboardButton("💵 USDT", callback_data="price_USDT")],
        [InlineKeyboardButton("📊 Top 10", callback_data="show_top10"),
         InlineKeyboardButton("💼 Danh mục", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 Lợi nhuận", callback_data="show_profit"),
         InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_transactions")],
        [InlineKeyboardButton("🔔 Cảnh báo giá", callback_data="show_alerts"),
         InlineKeyboardButton("📊 Thống kê", callback_data="show_stats")],
        [InlineKeyboardButton("📥 Xuất CSV", callback_data="export_csv"),
         InlineKeyboardButton("➖ Bán coin", callback_data="show_sell")],
        [InlineKeyboardButton("➕ Mua coin", callback_data="show_buy")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_expense_main_keyboard():
    """Keyboard chính cho quản lý chi tiêu"""
    keyboard = [
        [KeyboardButton("💰 Thu nhập"), KeyboardButton("💸 Chi tiêu")],
        [KeyboardButton("📊 Báo cáo"), KeyboardButton("📋 Danh mục")],
        [KeyboardButton("🔄 Gần đây"), KeyboardButton("🔙 Về menu chính")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_expense_inline_keyboard():
    """Inline keyboard cho quản lý chi tiêu"""
    keyboard = [
        [InlineKeyboardButton("➕ Thêm thu nhập", callback_data="expense_add_income"),
         InlineKeyboardButton("💸 Thêm chi tiêu", callback_data="expense_add_expense")],
        [InlineKeyboardButton("📊 Hôm nay", callback_data="expense_today"),
         InlineKeyboardButton("📅 Tháng này", callback_data="expense_month")],
        [InlineKeyboardButton("📋 Quản lý danh mục", callback_data="expense_manage_cats"),
         InlineKeyboardButton("📈 Báo cáo", callback_data="expense_report")],
        [InlineKeyboardButton("🔄 Xem gần đây", callback_data="expense_recent"),
         InlineKeyboardButton("🔙 Về menu chính", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS (GIỮ NGUYÊN) ====================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    welcome_msg = (
        "🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*\n\n"
        "🤖 Bot hỗ trợ:\n\n"
        "*💎 ĐẦU TƯ COIN:*\n"
        "• Xem giá bất kỳ coin nào\n"
        "• Top 10 coin\n"
        "• Quản lý danh mục đầu tư\n"
        "• Tính lợi nhuận chi tiết\n"
        "• Cảnh báo giá\n\n"
        "*💰 QUẢN LÝ CHI TIÊU:*\n"
        "• Ghi chép thu nhập/chi tiêu\n"
        "• Hỗ trợ đa tiền tệ (VND, USD, LKR, KHR, HKD...)\n"
        "• Quản lý ngân sách theo danh mục\n"
        "• Báo cáo theo ngày/tuần/tháng\n\n"
        "👇 *Chọn chức năng bên dưới*"
    )
    
    await update.message.reply_text(
        welcome_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard()
    )

async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    help_msg = (
        "📘 *HƯỚNG DẪN*\n\n"
        "*ĐẦU TƯ COIN:*\n"
        "• `/s btc eth` - Xem giá coin\n"
        "• `/usdt` - Tỷ giá USDT/VND\n"
        "• `/buy btc 0.5 40000` - Mua coin\n"
        "• `/sell btc 0.2` - Bán coin\n"
        "• `/edit` - Xem/sửa giao dịch\n"
        "• `/alert BTC above 50000` - Cảnh báo giá\n\n"
        
        "*QUẢN LÝ CHI TIÊU:*\n"
        "• `thu nhập 5000000VND Lương` - Thêm thu nhập\n"
        "• `thu nhập 100USD Freelance` - Thêm thu nhập USD\n"
        "• `thu nhập 50000KHR` - Thêm thu nhập Riel Campuchia\n"
        "• `thu nhập 50000` - Thêm 50,000 VND (mặc định)\n"
        "• `danh mục Ăn uống 3000000` - Tạo danh mục\n"
        "• `chi tiêu 1 50000VND Cà phê` - Thêm chi tiêu (1 là mã danh mục)\n"
        "• `chi tiêu 2 20USD Xăng` - Thêm chi tiêu USD\n"
        "• `xóa chi [id]` - Xóa khoản chi\n"
        "• `xóa thu [id]` - Xóa khoản thu\n"
        "• `sửa budget [id] [số tiền]` - Sửa ngân sách\n\n"
        
        "*TÍNH NĂNG ẨN:*\n"
        "• Gõ phép tính: `(5+3)*2`"
    )
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

# ==================== PORTFOLIO COMMANDS (GIỮ NGUYÊN) ====================

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

# ==================== ALERT COMMANDS (GIỮ NGUYÊN) ====================

async def alert_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 3:
        await update.message.reply_text(
            "❌ *HƯỚNG DẪN CẢNH BÁO*\n\n"
            "• `/alert BTC above 50000` - Báo khi BTC trên 50k\n"
            "• `/alert ETH below 3000` - Báo khi ETH dưới 3k\n\n"
            "• `/alerts` - Xem danh sách cảnh báo\n"
            "• `/alert_del 5` - Xóa cảnh báo số 5",
            parse_mode='Markdown'
        )
        return
    
    symbol = ctx.args[0].upper()
    condition = ctx.args[1].lower()
    try:
        target_price = float(ctx.args[2])
    except ValueError:
        return await update.message.reply_text("❌ Giá không hợp lệ!")
    
    if condition not in ['above', 'below']:
        return await update.message.reply_text("❌ Điều kiện phải là 'above' hoặc 'below'")
    
    uid = update.effective_user.id
    
    price_data = get_price(symbol)
    if not price_data:
        return await update.message.reply_text(f"❌ Không tìm thấy coin *{symbol}*", parse_mode='Markdown')
    
    if add_alert(uid, symbol, target_price, condition):
        msg = (
            f"✅ *ĐÃ TẠO CẢNH BÁO*\n━━━━━━━━━━━━━━━━\n\n"
            f"• Coin: *{symbol}*\n"
            f"• Mốc giá: `{fmt_price(target_price)}`\n"
            f"• Giá hiện tại: `{fmt_price(price_data['p'])}`\n"
            f"• Điều kiện: {'📈 Lên trên' if condition == 'above' else '📉 Xuống dưới'}\n\n"
            f"Bot sẽ báo cho bạn khi giá chạm mốc này!"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')
    else:
        await update.message.reply_text("❌ Lỗi khi tạo cảnh báo!")

async def alerts_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    alerts = get_user_alerts(uid)
    
    if not alerts:
        await update.message.reply_text("📭 Bạn chưa có cảnh báo nào!")
        return
    
    msg = "🔔 *DANH SÁCH CẢNH BÁO*\n━━━━━━━━━━━━━━━━\n\n"
    for alert in alerts:
        alert_id, symbol, target, condition, created = alert
        created_date = created.split()[0]
        
        price_data = get_price(symbol)
        current_price = price_data['p'] if price_data else 0
        
        status = "🟢" if (condition == 'above' and current_price < target) or (condition == 'below' and current_price > target) else "🔴"
        
        msg += f"{status} *#{alert_id}*: {symbol} {condition} `{fmt_price(target)}`\n"
        msg += f"   Giá hiện: `{fmt_price(current_price)}` (tạo {created_date})\n\n"
    
    msg += "*Xóa:* `/alert_del [số]`"
    await update.message.reply_text(msg, parse_mode='Markdown')

async def alert_del_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        return await update.message.reply_text("❌ /alert_del [số]")
    
    try:
        alert_id = int(ctx.args[0])
        uid = update.effective_user.id
        
        if delete_alert(alert_id, uid):
            await update.message.reply_text(f"✅ Đã xóa cảnh báo #{alert_id}")
        else:
            await update.message.reply_text(f"❌ Không tìm thấy cảnh báo #{alert_id}")
    except ValueError:
        await update.message.reply_text("❌ ID không hợp lệ")

# ==================== STATS COMMAND (GIỮ NGUYÊN) ====================

async def stats_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    msg = await update.message.reply_text("🔄 Đang tính toán thống kê...")
    
    stats = get_portfolio_stats(uid)
    
    if not stats:
        await msg.edit_text("📭 Danh mục trống!")
        return
    
    total_invest = stats['total_invest']
    total_value = stats['total_value']
    total_profit = stats['total_profit']
    total_profit_percent = stats['total_profit_percent']
    coin_profits = stats['coin_profits']
    
    stats_msg = (
        f"📊 *THỐNG KÊ DANH MỤC*\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"*TỔNG QUAN*\n"
        f"• Vốn: `{fmt_price(total_invest)}`\n"
        f"• Giá trị: `{fmt_price(total_value)}`\n"
        f"• Lợi nhuận: `{fmt_price(total_profit)}`\n"
        f"• Tỷ suất: `{total_profit_percent:+.2f}%`\n\n"
    )
    
    stats_msg += "*📈 TOP COIN LỜI NHẤT*\n"
    count = 0
    for symbol, profit, profit_pct, value, cost in coin_profits:
        if profit > 0:
            count += 1
            stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
        if count >= 3:
            break
    
    if count == 0:
        stats_msg += "Không có coin lời\n"
    
    stats_msg += f"\n*📉 TOP COIN LỖ NHẤT*\n"
    count = 0
    for symbol, profit, profit_pct, value, cost in reversed(coin_profits):
        if profit < 0:
            count += 1
            stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
        if count >= 3:
            break
    
    if count == 0:
        stats_msg += "Không có coin lỗ\n"
    
    stats_msg += f"\n*📊 PHÂN BỔ VỐN*\n"
    for symbol, data in stats['coins'].items():
        percent = (data['cost'] / total_invest * 100) if total_invest > 0 else 0
        stats_msg += f"• {symbol}: `{percent:.1f}%`\n"
    
    stats_msg += f"\n📅 Cập nhật: {get_vn_time().strftime('%H:%M %d/%m/%Y')}"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="show_stats"),
        InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
    ]]
    
    await msg.edit_text(
        stats_msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== EXPORT COMMAND (GIỮ NGUYÊN) ====================

async def export_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = await update.message.reply_text("🔄 Đang tạo file CSV...")
    
    filepath, error = export_portfolio_to_csv(uid)
    
    if error:
        await msg.edit_text(error)
        return
    
    try:
        with open(filepath, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=os.path.basename(filepath),
                caption="📊 *BÁO CÁO DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công! (Định dạng CSV)",
                parse_mode=ParseMode.MARKDOWN
            )
        
        os.remove(filepath)
        logger.info(f"🗑 Đã xóa file {filepath}")
        
    except Exception as e:
        logger.error(f"Lỗi khi gửi file: {e}")
        await msg.edit_text("❌ Lỗi khi gửi file. Vui lòng thử lại sau.")
    
    await msg.delete()

# ==================== EXPENSE COMMAND HANDLERS (CẬP NHẬT ĐA TIỀN TỆ) ====================

async def expense_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Menu quản lý chi tiêu"""
    await update.message.reply_text(
        "💰 *QUẢN LÝ CHI TIÊU CÁ NHÂN*\n\n"
        "Chọn chức năng bên dưới:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_expense_inline_keyboard()
    )

async def expense_add_income_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Hướng dẫn thêm thu nhập đa tiền tệ"""
    currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
    currency_detail = "\n".join([f"• {code}: {name}" for code, name in SUPPORTED_CURRENCIES.items()])
    
    await update.message.reply_text(
        "💰 *THÊM THU NHẬP*\n\n"
        "*Cú pháp:* `thu nhập [số tiền][loại tiền] [nguồn] [ghi chú]`\n\n"
        "*Ví dụ:*\n"
        "• `thu nhập 5000000VND Lương Tháng 3`\n"
        "• `thu nhập 100USD Freelance`\n"
        "• `thu nhập 50000KHR`\n"
        "• `thu nhập 2000HKD Bán hàng`\n"
        "• `thu nhập 50000` (mặc định VND)\n\n"
        f"*Các loại tiền hỗ trợ:*\n{currency_detail}",
        parse_mode=ParseMode.MARKDOWN
    )

async def expense_add_expense_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Hướng dẫn thêm chi tiêu đa tiền tệ"""
    uid = update.effective_user.id
    categories = get_expense_categories(uid)
    
    currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
    
    if not categories:
        await update.message.reply_text(
            "❌ Bạn chưa có danh mục chi tiêu nào!\n"
            "Tạo danh mục: `danh mục [tên] [ngân sách]`\n\n"
            "Ví dụ: `danh mục Ăn uống 3000000`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    msg = "💸 *THÊM CHI TIÊU*\n\n"
    msg += "*Cú pháp:* `chi tiêu [mã] [số tiền][loại tiền] [ghi chú]`\n\n"
    msg += "*Các danh mục:*\n"
    for cat in categories:
        cat_id, name, budget, _ = cat
        budget_str = format_currency_amount(budget, 'VND') if budget > 0 else "Không có"
        msg += f"• `{cat_id}`: {name} (Budget: {budget_str})\n"
    
    msg += f"\n*Các loại tiền hỗ trợ:* {currency_list}\n\n"
    msg += "*Ví dụ:*\n"
    msg += "• `chi tiêu 1 50000VND Cà phê sáng`\n"
    msg += "• `chi tiêu 2 20USD Xăng xe`\n"
    msg += "• `chi tiêu 3 1000KHR Mua sắm`\n"
    msg += "• `chi tiêu 4 50000` (mặc định VND)"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def expense_report_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Xem báo cáo chi tiêu"""
    uid = update.effective_user.id
    
    msg = await update.message.reply_text("🔄 Đang tổng hợp...")
    
    # Báo cáo tháng này
    expenses = get_expenses_by_period(uid, 'month')
    
    report = (
        f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n"
        f"━━━━━━━━━━━━━━━━\n\n"
    )
    
    if expenses:
        report += "*📋 CHI TIÊU THEO LOẠI TIỀN:*\n"
        expense_by_currency = {}
        for exp in expenses:
            cat_name, amount, count, budget, currency = exp
            if currency not in expense_by_currency:
                expense_by_currency[currency] = 0
            expense_by_currency[currency] += amount
        
        for currency, total in expense_by_currency.items():
            report += f"• {currency}: {format_currency_amount(total, currency)}\n"
        
        report += "\n*📋 CHI TIẾT DANH MỤC:*\n"
        for exp in expenses:
            cat_name, amount, count, budget, currency = exp
            report += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
            
            if currency == 'VND' and budget > 0:
                percent = (amount / budget * 100)
                status = "🔴" if amount > budget else "🟢"
                report += f"  {status} Budget: {format_currency_amount(budget, 'VND')} ({percent:.1f}%)\n"
    else:
        report += "📭 Chưa có chi tiêu trong tháng này.\n"
    
    # Thu nhập tháng này
    incomes = get_income_by_period(uid, 'month')
    if incomes:
        report += "\n*💰 THU NHẬP THÁNG NÀY:*\n"
        income_by_currency = {}
        for inc in incomes:
            source, amount, count, currency = inc
            if currency not in income_by_currency:
                income_by_currency[currency] = 0
            income_by_currency[currency] += amount
        
        for currency, total in income_by_currency.items():
            report += f"• {currency}: {format_currency_amount(total, currency)}\n"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="expense_report"),
        InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
    ]]
    
    await msg.edit_text(
        report,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def expense_today_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Xem chi tiêu hôm nay (hiển thị đa tiền tệ)"""
    uid = update.effective_user.id
    
    expenses = get_expenses_by_period(uid, 'day')
    incomes = get_income_by_period(uid, 'day')
    
    if not expenses and not incomes:
        await update.message.reply_text(f"📭 Hôm nay chưa có giao dịch nào!")
        return
    
    msg = f"📅 *GIAO DỊCH HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
    
    if incomes:
        msg += "*💰 THU NHẬP:*\n"
        for inc in incomes:
            source, amount, count, currency = inc
            msg += f"• {source}: {format_currency_amount(amount, currency)} ({count} lần)\n"
        msg += "\n"
    
    if expenses:
        msg += "*💸 CHI TIÊU:*\n"
        for exp in expenses:
            cat_name, amount, count, budget, currency = exp
            msg += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="expense_today"),
        InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
    ]]
    
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def expense_month_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Xem chi tiêu tháng này"""
    uid = update.effective_user.id
    
    expenses = get_expenses_by_period(uid, 'month')
    incomes = get_income_by_period(uid, 'month')
    
    if not expenses and not incomes:
        await update.message.reply_text(f"📭 Tháng {get_vn_time().strftime('%m/%Y')} chưa có giao dịch nào!")
        return
    
    msg = f"📅 *GIAO DỊCH THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
    
    if incomes:
        msg += "*💰 THU NHẬP:*\n"
        income_by_currency = {}
        for inc in incomes:
            source, amount, count, currency = inc
            if currency not in income_by_currency:
                income_by_currency[currency] = 0
            income_by_currency[currency] += amount
            msg += f"• {source}: {format_currency_amount(amount, currency)} ({count} lần)\n"
        
        msg += "\n*Tổng thu theo loại tiền:*\n"
        for currency, total in income_by_currency.items():
            msg += f"  {format_currency_amount(total, currency)}\n"
        msg += "\n"
    
    if expenses:
        msg += "*💸 CHI TIÊU:*\n"
        expense_by_currency = {}
        for exp in expenses:
            cat_name, amount, count, budget, currency = exp
            if currency not in expense_by_currency:
                expense_by_currency[currency] = 0
            expense_by_currency[currency] += amount
            msg += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
        
        msg += "\n*Tổng chi theo loại tiền:*\n"
        for currency, total in expense_by_currency.items():
            msg += f"  {format_currency_amount(total, currency)}\n"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="expense_month"),
        InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
    ]]
    
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def expense_recent_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Xem giao dịch gần đây (hiển thị cả loại tiền)"""
    uid = update.effective_user.id
    
    recent_expenses = get_recent_expenses(uid, 5)
    recent_incomes = get_recent_incomes(uid, 5)
    
    if not recent_expenses and not recent_incomes:
        await update.message.reply_text("📭 Chưa có giao dịch nào!")
        return
    
    msg = "🔄 *GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
    
    if recent_incomes:
        msg += "*💰 THU NHẬP:*\n"
        for inc in recent_incomes:
            inc_id, amount, source, note, date, currency = inc
            note_str = f" - {note}" if note else ""
            msg += f"• #{inc_id} {date}: {format_currency_amount(amount, currency)} ({source}{note_str})\n"
        msg += "\n"
    
    if recent_expenses:
        msg += "*💸 CHI TIÊU:*\n"
        for exp in recent_expenses:
            exp_id, cat_name, amount, note, date, currency = exp
            note_str = f" - {note}" if note else ""
            msg += f"• #{exp_id} {date}: {format_currency_amount(amount, currency)} ({cat_name}{note_str})\n"
    
    msg += "\n*Xóa:* `xóa chi [id]` hoặc `xóa thu [id]`"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="expense_recent"),
        InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
    ]]
    
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def expense_manage_categories_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Quản lý danh mục chi tiêu"""
    uid = update.effective_user.id
    
    categories = get_expense_categories(uid)
    
    if not categories:
        msg = (
            "📋 *QUẢN LÝ DANH MỤC*\n\n"
            "Chưa có danh mục nào.\n\n"
            "*Tạo mới:* `danh mục [tên] [ngân sách]`\n"
            "Ví dụ: `danh mục Ăn uống 3000000`"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return
    
    msg = "📋 *DANH SÁCH DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
    
    for cat in categories:
        cat_id, name, budget, created = cat
        msg += f"*{cat_id}.* {name}\n"
        msg += f"   Budget: {format_currency_amount(budget, 'VND')}\n"
        msg += f"   Tạo: {created.split()[0]}\n\n"
    
    msg += "*Thao tác:*\n"
    msg += "• `danh mục [tên] [budget]` - Thêm mới\n"
    msg += "• `sửa budget [id] [số tiền]` - Sửa ngân sách\n"
    msg += "• `xóa danh mục [id]` - Xóa danh mục"
    
    keyboard = [[
        InlineKeyboardButton("🔄 Làm mới", callback_data="expense_manage_cats"),
        InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
    ]]
    
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ==================== HANDLE MESSAGE (CẬP NHẬT PHẦN CHI TIÊU) ====================

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    # Xử lý menu chính
    if text == "💰 ĐẦU TƯ COIN":
        await update.message.reply_text(
            "💰 *MENU ĐẦU TƯ COIN*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_invest_menu_keyboard()
        )
    elif text == "💸 QUẢN LÝ CHI TIÊU":
        await expense_command(update, ctx)
    elif text == "❓ HƯỚNG DẪN":
        await help_command(update, ctx)
    
    # Xử lý menu chi tiêu (keyboard buttons)
    elif text == "💰 Thu nhập":
        await expense_add_income_handler(update, ctx)
    elif text == "💸 Chi tiêu":
        await expense_add_expense_handler(update, ctx)
    elif text == "📊 Báo cáo":
        await expense_report_handler(update, ctx)
    elif text == "📋 Danh mục":
        await expense_manage_categories_handler(update, ctx)
    elif text == "🔄 Gần đây":
        await expense_recent_handler(update, ctx)
    elif text == "🔙 Về menu chính":
        await start(update, ctx)
    
    # Xử lý các lệnh nhập liệu (CẬP NHẬT PHẦN NÀY)
    elif text.startswith("thu nhập"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                # Kiểm tra xem có chỉ định loại tiền không
                currency = 'VND'  # Mặc định
                amount_str = parts[1]
                
                # Kiểm tra nếu amount có kèm currency code (ví dụ: 100USD, 5000KHR)
                import re
                # Pattern: số (có thể có dấu chấm) + chữ cái (2-4 ký tự)
                match = re.match(r'^(\d+(?:\.\d+)?)([A-Za-z]{2,4})$', amount_str)
                if match:
                    amount = float(match.group(1))
                    currency = match.group(2).upper()
                    # Kiểm tra currency có hỗ trợ không
                    if currency not in SUPPORTED_CURRENCIES:
                        currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
                        await update.message.reply_text(
                            f"❌ Loại tiền '{currency}' không hỗ trợ!\n"
                            f"Các loại tiền hỗ trợ: {currency_list}"
                        )
                        return
                else:
                    # Nếu không có currency code, thử parse như số bình thường
                    try:
                        amount = float(amount_str)
                    except ValueError:
                        await update.message.reply_text(
                            "❌ Số tiền không hợp lệ!\n"
                            "Ví dụ: `thu nhập 100USD Lương` hoặc `thu nhập 5000000VND` hoặc `thu nhập 50000`",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        return
                
                source = parts[2] if len(parts) > 2 else "Khác"
                note = " ".join(parts[3:]) if len(parts) > 3 else ""
                
                uid = update.effective_user.id
                if add_income(uid, amount, source, currency, note):
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: {format_currency_amount(amount, currency)}\n"
                        f"📌 Nguồn: {source}\n"
                        f"📝 Ghi chú: {note if note else 'Không có'}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi ghi nhận thu nhập!")
            except ValueError:
                await update.message.reply_text(
                    "❌ Số tiền không hợp lệ!\n"
                    "Ví dụ: `thu nhập 100USD Lương` hoặc `thu nhập 5000000VND` hoặc `thu nhập 50000`",
                    parse_mode=ParseMode.MARKDOWN
                )
    
    elif text.startswith("chi tiêu"):
        parts = text.split()
        if len(parts) >= 3:
            try:
                category_id = int(parts[1])
                
                # Kiểm tra xem có chỉ định loại tiền không
                currency = 'VND'  # Mặc định
                amount_str = parts[2]
                
                # Kiểm tra nếu amount có kèm currency code
                import re
                match = re.match(r'^(\d+(?:\.\d+)?)([A-Za-z]{2,4})$', amount_str)
                if match:
                    amount = float(match.group(1))
                    currency = match.group(2).upper()
                    # Kiểm tra currency có hỗ trợ không
                    if currency not in SUPPORTED_CURRENCIES:
                        currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
                        await update.message.reply_text(
                            f"❌ Loại tiền '{currency}' không hỗ trợ!\n"
                            f"Các loại tiền hỗ trợ: {currency_list}"
                        )
                        return
                else:
                    # Nếu không có currency code, thử parse như số bình thường
                    try:
                        amount = float(amount_str)
                    except ValueError:
                        await update.message.reply_text(
                            "❌ Số tiền không hợp lệ!\n"
                            "Ví dụ: `chi tiêu 1 50000VND Cà phê` hoặc `chi tiêu 2 100USD`",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        return
                
                note = " ".join(parts[3:]) if len(parts) > 3 else ""
                
                uid = update.effective_user.id
                
                # Kiểm tra category tồn tại
                categories = get_expense_categories(uid)
                category = next((c for c in categories if c[0] == category_id), None)
                
                if not category:
                    await update.message.reply_text("❌ Mã danh mục không tồn tại!")
                    return
                
                if add_expense(uid, category_id, amount, currency, note):
                    cat_name = category[1]
                    budget = category[2]
                    
                    # Kiểm tra vượt budget (chỉ tính cùng loại tiền VND)
                    msg = (
                        f"✅ *ĐÃ THÊM CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💸 Số tiền: {format_currency_amount(amount, currency)}\n"
                        f"📌 Danh mục: {cat_name}\n"
                        f"📝 Ghi chú: {note if note else 'Không có'}"
                    )
                    
                    # Chỉ kiểm tra budget nếu là VND
                    if currency == 'VND' and budget > 0:
                        expenses = get_expenses_by_period(uid, 'month')
                        total_spent = 0
                        for exp in expenses:
                            if exp[0] == cat_name and exp[4] == 'VND':
                                total_spent = exp[1]
                                break
                        
                        msg += f"\n\n"
                        if total_spent > budget:
                            percent = (total_spent / budget * 100)
                            msg += f"⚠️ *CẢNH BÁO:* Đã vượt budget!\n"
                            msg += f"Budget: {format_currency_amount(budget, 'VND')}\n"
                            msg += f"Đã chi: {format_currency_amount(total_spent, 'VND')} ({percent:.1f}%)"
                        else:
                            msg += f"Budget còn: {format_currency_amount(budget - total_spent, 'VND')}"
                    
                    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.message.reply_text("❌ Lỗi khi ghi nhận chi tiêu!")
            except ValueError:
                await update.message.reply_text(
                    "❌ Mã danh mục hoặc số tiền không hợp lệ!\n"
                    "Ví dụ: `chi tiêu 1 50000VND Cà phê` hoặc `chi tiêu 2 100USD`",
                    parse_mode=ParseMode.MARKDOWN
                )
    
    elif text.startswith("danh mục"):
        parts = text.split()
        if len(parts) >= 2:
            name = parts[1]
            budget = float(parts[2]) if len(parts) > 2 else 0
            
            uid = update.effective_user.id
            if add_expense_category(uid, name, budget):
                await update.message.reply_text(
                    f"✅ *ĐÃ THÊM DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                    f"📋 Tên: *{name.upper()}*\n"
                    f"💰 Budget: {format_currency_amount(budget, 'VND')}",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Lỗi khi thêm danh mục!")
    
    elif text.startswith("xóa chi"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                expense_id = int(parts[1])
                uid = update.effective_user.id
                
                if delete_expense(expense_id, uid):
                    await update.message.reply_text(f"✅ Đã xóa khoản chi #{expense_id}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản chi #{expense_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
    
    elif text.startswith("xóa thu"):
        parts = text.split()
        if len(parts) >= 2:
            try:
                income_id = int(parts[1])
                uid = update.effective_user.id
                
                if delete_income(income_id, uid):
                    await update.message.reply_text(f"✅ Đã xóa khoản thu #{income_id}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản thu #{income_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
    
    elif text.startswith("sửa budget"):
        parts = text.split()
        if len(parts) >= 3:
            try:
                category_id = int(parts[2])
                new_budget = float(parts[3]) if len(parts) > 3 else 0
                uid = update.effective_user.id
                
                if update_category_budget(category_id, uid, new_budget):
                    await update.message.reply_text(
                        f"✅ Đã cập nhật budget cho danh mục #{category_id}\n"
                        f"Budget mới: {format_currency_amount(new_budget, 'VND')}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy danh mục #{category_id}")
            except ValueError:
                await update.message.reply_text("❌ ID hoặc số tiền không hợp lệ!")
    
    elif text.startswith("xóa danh mục"):
        parts = text.split()
        if len(parts) >= 3:
            try:
                category_id = int(parts[2])
                uid = update.effective_user.id
                
                success, message = delete_expense_category(category_id, uid)
                if success:
                    await update.message.reply_text(f"✅ {message}")
                else:
                    await update.message.reply_text(f"❌ {message}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
    
    else:
        # TÍNH NĂNG ẨN - Kiểm tra xem có phải phép tính không
        if any(op in text for op in ['+', '-', '*', '/', '%']) and not text.startswith('/'):
            result, error = tinh_toan(text)
            if error:
                await update.message.reply_text(error)
            else:
                if isinstance(result, int):
                    await update.message.reply_text(f"{text} = {result:,}")
                else:
                    formatted_result = f"{result:,.10f}".rstrip('0').rstrip('.') if '.' in str(result) else str(result)
                    await update.message.reply_text(f"{text} = {formatted_result}")

# ==================== HANDLE CALLBACK (GIỮ NGUYÊN PHẦN COIN, THÊM PHẦN CHI TIÊU) ====================

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    try:
        # ========== CALLBACK MENU CHÍNH ==========
        if data == "back_to_main":
            await query.edit_message_text(
                "💰 *MENU CHÍNH*\nChọn chức năng bên dưới:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=None
            )
            await query.message.reply_text(
                "👇 Chọn chức năng:",
                reply_markup=get_main_keyboard()
            )
        
        # ========== CALLBACK ĐẦU TƯ COIN (GIỮ NGUYÊN) ==========
        elif data == "back_to_invest":
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
                [InlineKeyboardButton("📥 Xuất CSV", callback_data="export_csv")],
                [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]
            ]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "export_csv":
            uid = query.from_user.id
            await query.edit_message_text("🔄 Đang tạo file CSV...")
            
            filepath, error = export_portfolio_to_csv(uid)
            
            if error:
                await query.edit_message_text(error)
                return
            
            try:
                with open(filepath, 'rb') as f:
                    await query.message.reply_document(
                        document=f,
                        filename=os.path.basename(filepath),
                        caption="📊 *BÁO CÁO DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công! (Định dạng CSV)",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                os.remove(filepath)
                logger.info(f"🗑 Đã xóa file {filepath}")
                
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
        
        elif data == "show_alerts":
            uid = query.from_user.id
            alerts = get_user_alerts(uid)
            
            if not alerts:
                await query.edit_message_text(
                    "📭 Bạn chưa có cảnh báo nào!\n\nDùng `/alert BTC above 50000` để tạo mới.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                )
                return
            
            msg = "🔔 *CẢNH BÁO GIÁ*\n━━━━━━━━━━━━━━━━\n\n"
            for alert in alerts:
                alert_id, symbol, target, condition, created = alert
                created_date = created.split()[0]
                
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else 0
                
                status = "🟢" if (condition == 'above' and current_price < target) or (condition == 'below' and current_price > target) else "🔴"
                
                msg += f"{status} *#{alert_id}*: {symbol}\n"
                msg += f"   Mốc: `{fmt_price(target)}` ({condition})\n"
                msg += f"   Hiện: `{fmt_price(current_price)}`\n"
                msg += f"   Tạo: {created_date}\n\n"
            
            keyboard = [[
                InlineKeyboardButton("➕ Thêm", callback_data="show_buy"),
                InlineKeyboardButton("🗑 Xóa", callback_data="edit_transactions"),
                InlineKeyboardButton("🔙 Menu", callback_data="back_to_invest")
            ]]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "show_stats":
            uid = query.from_user.id
            portfolio_data = get_portfolio(uid)
            
            if not portfolio_data:
                await query.edit_message_text(
                    "📭 Danh mục trống!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                )
                return
            
            await query.edit_message_text("🔄 Đang tính toán thống kê...")
            
            stats = get_portfolio_stats(uid)
            
            if not stats:
                await query.edit_message_text("📭 Không thể tính toán thống kê!")
                return
            
            total_invest = stats['total_invest']
            total_value = stats['total_value']
            total_profit = stats['total_profit']
            total_profit_percent = stats['total_profit_percent']
            coin_profits = stats['coin_profits']
            
            stats_msg = (
                f"📊 *THỐNG KÊ DANH MỤC*\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"*TỔNG QUAN*\n"
                f"• Vốn: `{fmt_price(total_invest)}`\n"
                f"• Giá trị: `{fmt_price(total_value)}`\n"
                f"• Lợi nhuận: `{fmt_price(total_profit)}`\n"
                f"• Tỷ suất: `{total_profit_percent:+.2f}%`\n\n"
            )
            
            stats_msg += "*📈 TOP COIN LỜI NHẤT*\n"
            count = 0
            for symbol, profit, profit_pct, value, cost in coin_profits:
                if profit > 0:
                    count += 1
                    stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
                if count >= 3:
                    break
            
            if count == 0:
                stats_msg += "Không có coin lời\n"
            
            stats_msg += f"\n*📉 TOP COIN LỖ NHẤT*\n"
            count = 0
            for symbol, profit, profit_pct, value, cost in reversed(coin_profits):
                if profit < 0:
                    count += 1
                    stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
                if count >= 3:
                    break
            
            if count == 0:
                stats_msg += "Không có coin lỗ\n"
            
            stats_msg += f"\n*📊 PHÂN BỔ VỐN*\n"
            for symbol, data in stats['coins'].items():
                percent = (data['cost'] / total_invest * 100) if total_invest > 0 else 0
                stats_msg += f"• {symbol}: `{percent:.1f}%`\n"
            
            stats_msg += f"\n📅 Cập nhật: {get_vn_time().strftime('%H:%M %d/%m/%Y')}"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="show_stats"),
                InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
            ]]
            
            await query.edit_message_text(
                stats_msg,
                parse_mode=ParseMode.MARKDOWN,
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
        
        # ========== CALLBACK QUẢN LÝ CHI TIÊU (MỚI) ==========
        elif data == "back_to_expense":
            await query.edit_message_text(
                "💰 *QUẢN LÝ CHI TIÊU CÁ NHÂN*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_expense_inline_keyboard()
            )
        
        elif data == "expense_add_income":
            currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
            currency_detail = "\n".join([f"• {code}: {name}" for code, name in SUPPORTED_CURRENCIES.items()])
            
            await query.edit_message_text(
                "💰 *THÊM THU NHẬP*\n\n"
                "*Cú pháp:* `thu nhập [số tiền][loại tiền] [nguồn] [ghi chú]`\n\n"
                "*Ví dụ:*\n"
                "• `thu nhập 5000000VND Lương Tháng 3`\n"
                "• `thu nhập 100USD Freelance`\n"
                "• `thu nhập 50000KHR`\n"
                "• `thu nhập 2000HKD Bán hàng`\n"
                "• `thu nhập 50000` (mặc định VND)\n\n"
                f"*Các loại tiền hỗ trợ:*\n{currency_detail}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
            )
        
        elif data == "expense_add_expense":
            uid = query.from_user.id
            categories = get_expense_categories(uid)
            
            currency_list = ', '.join(SUPPORTED_CURRENCIES.keys())
            
            if not categories:
                await query.edit_message_text(
                    "❌ Bạn chưa có danh mục chi tiêu nào!\n"
                    "Tạo danh mục: `danh mục [tên] [ngân sách]`\n\n"
                    "Ví dụ: `danh mục Ăn uống 3000000`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
                )
                return
            
            msg = "💸 *THÊM CHI TIÊU*\n\n"
            msg += "*Cú pháp:* `chi tiêu [mã] [số tiền][loại tiền] [ghi chú]`\n\n"
            msg += "*Các danh mục:*\n"
            for cat in categories:
                cat_id, name, budget, _ = cat
                budget_str = format_currency_amount(budget, 'VND') if budget > 0 else "Không có"
                msg += f"• `{cat_id}`: {name} (Budget: {budget_str})\n"
            
            msg += f"\n*Các loại tiền hỗ trợ:* {currency_list}\n\n"
            msg += "*Ví dụ:*\n"
            msg += "• `chi tiêu 1 50000VND Cà phê sáng`\n"
            msg += "• `chi tiêu 2 20USD Xăng xe`\n"
            msg += "• `chi tiêu 3 1000KHR Mua sắm`\n"
            msg += "• `chi tiêu 4 50000` (mặc định VND)"
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
            )
        
        elif data == "expense_today":
            uid = query.from_user.id
            
            expenses = get_expenses_by_period(uid, 'day')
            incomes = get_income_by_period(uid, 'day')
            
            if not expenses and not incomes:
                await query.edit_message_text(
                    f"📭 Hôm nay chưa có giao dịch nào!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
                )
                return
            
            msg = f"📅 *GIAO DỊCH HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
            
            if incomes:
                msg += "*💰 THU NHẬP:*\n"
                for inc in incomes:
                    source, amount, count, currency = inc
                    msg += f"• {source}: {format_currency_amount(amount, currency)} ({count} lần)\n"
                msg += "\n"
            
            if expenses:
                msg += "*💸 CHI TIÊU:*\n"
                for exp in expenses:
                    cat_name, amount, count, budget, currency = exp
                    msg += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="expense_today"),
                InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
            ]]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "expense_month":
            uid = query.from_user.id
            
            expenses = get_expenses_by_period(uid, 'month')
            incomes = get_income_by_period(uid, 'month')
            
            if not expenses and not incomes:
                await query.edit_message_text(
                    f"📭 Tháng {get_vn_time().strftime('%m/%Y')} chưa có giao dịch nào!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
                )
                return
            
            msg = f"📅 *GIAO DỊCH THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
            
            if incomes:
                msg += "*💰 THU NHẬP:*\n"
                income_by_currency = {}
                for inc in incomes:
                    source, amount, count, currency = inc
                    if currency not in income_by_currency:
                        income_by_currency[currency] = 0
                    income_by_currency[currency] += amount
                    msg += f"• {source}: {format_currency_amount(amount, currency)} ({count} lần)\n"
                
                msg += "\n*Tổng thu theo loại tiền:*\n"
                for currency, total in income_by_currency.items():
                    msg += f"  {format_currency_amount(total, currency)}\n"
                msg += "\n"
            
            if expenses:
                msg += "*💸 CHI TIÊU:*\n"
                expense_by_currency = {}
                for exp in expenses:
                    cat_name, amount, count, budget, currency = exp
                    if currency not in expense_by_currency:
                        expense_by_currency[currency] = 0
                    expense_by_currency[currency] += amount
                    msg += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
                
                msg += "\n*Tổng chi theo loại tiền:*\n"
                for currency, total in expense_by_currency.items():
                    msg += f"  {format_currency_amount(total, currency)}\n"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="expense_month"),
                InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
            ]]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "expense_report":
            uid = query.from_user.id
            
            await query.edit_message_text("🔄 Đang tổng hợp...")
            
            expenses = get_expenses_by_period(uid, 'month')
            
            report = (
                f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n"
                f"━━━━━━━━━━━━━━━━\n\n"
            )
            
            if expenses:
                report += "*📋 CHI TIÊU THEO LOẠI TIỀN:*\n"
                expense_by_currency = {}
                for exp in expenses:
                    cat_name, amount, count, budget, currency = exp
                    if currency not in expense_by_currency:
                        expense_by_currency[currency] = 0
                    expense_by_currency[currency] += amount
                
                for currency, total in expense_by_currency.items():
                    report += f"• {currency}: {format_currency_amount(total, currency)}\n"
                
                report += "\n*📋 CHI TIẾT DANH MỤC:*\n"
                for exp in expenses:
                    cat_name, amount, count, budget, currency = exp
                    report += f"• {cat_name}: {format_currency_amount(amount, currency)} ({count} lần)\n"
                    
                    if currency == 'VND' and budget > 0:
                        percent = (amount / budget * 100)
                        status = "🔴" if amount > budget else "🟢"
                        report += f"  {status} Budget: {format_currency_amount(budget, 'VND')} ({percent:.1f}%)\n"
            else:
                report += "📭 Chưa có chi tiêu trong tháng này.\n"
            
            incomes = get_income_by_period(uid, 'month')
            if incomes:
                report += "\n*💰 THU NHẬP THÁNG NÀY:*\n"
                income_by_currency = {}
                for inc in incomes:
                    source, amount, count, currency = inc
                    if currency not in income_by_currency:
                        income_by_currency[currency] = 0
                    income_by_currency[currency] += amount
                
                for currency, total in income_by_currency.items():
                    report += f"• {currency}: {format_currency_amount(total, currency)}\n"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="expense_report"),
                InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
            ]]
            
            await query.edit_message_text(
                report,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "expense_recent":
            uid = query.from_user.id
            
            recent_expenses = get_recent_expenses(uid, 5)
            recent_incomes = get_recent_incomes(uid, 5)
            
            if not recent_expenses and not recent_incomes:
                await query.edit_message_text(
                    "📭 Chưa có giao dịch nào!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
                )
                return
            
            msg = "🔄 *GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
            
            if recent_incomes:
                msg += "*💰 THU NHẬP:*\n"
                for inc in recent_incomes:
                    inc_id, amount, source, note, date, currency = inc
                    note_str = f" - {note}" if note else ""
                    msg += f"• #{inc_id} {date}: {format_currency_amount(amount, currency)} ({source}{note_str})\n"
                msg += "\n"
            
            if recent_expenses:
                msg += "*💸 CHI TIÊU:*\n"
                for exp in recent_expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    note_str = f" - {note}" if note else ""
                    msg += f"• #{exp_id} {date}: {format_currency_amount(amount, currency)} ({cat_name}{note_str})\n"
            
            msg += "\n*Xóa:* `xóa chi [id]` hoặc `xóa thu [id]`"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="expense_recent"),
                InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
            ]]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif data == "expense_manage_cats":
            uid = query.from_user.id
            
            categories = get_expense_categories(uid)
            
            if not categories:
                await query.edit_message_text(
                    "📋 *QUẢN LÝ DANH MỤC*\n\n"
                    "Chưa có danh mục nào.\n\n"
                    "*Tạo mới:* `danh mục [tên] [ngân sách]`\n"
                    "Ví dụ: `danh mục Ăn uống 3000000`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")]])
                )
                return
            
            msg = "📋 *DANH SÁCH DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
            
            for cat in categories:
                cat_id, name, budget, created = cat
                msg += f"*{cat_id}.* {name}\n"
                msg += f"   Budget: {format_currency_amount(budget, 'VND')}\n"
                msg += f"   Tạo: {created.split()[0]}\n\n"
            
            msg += "*Thao tác:*\n"
            msg += "• `danh mục [tên] [budget]` - Thêm mới\n"
            msg += "• `sửa budget [id] [số tiền]` - Sửa ngân sách\n"
            msg += "• `xóa danh mục [id]` - Xóa danh mục"
            
            keyboard = [[
                InlineKeyboardButton("🔄 Làm mới", callback_data="expense_manage_cats"),
                InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_expense")
            ]]
            
            await query.edit_message_text(
                msg, parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
    except Exception as e:
        logger.error(f"Lỗi trong handle_callback: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Có lỗi xảy ra. Vui lòng thử lại sau.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_main")]])
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
    
    # Command handlers (GIỮ NGUYÊN)
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
    
    # Alert commands (GIỮ NGUYÊN)
    app.add_handler(CommandHandler("alert", alert_command))
    app.add_handler(CommandHandler("alerts", alerts_command))
    app.add_handler(CommandHandler("alert_del", alert_del_command))
    
    # Stats command (GIỮ NGUYÊN)
    app.add_handler(CommandHandler("stats", stats_command))
    
    # Export command (GIỮ NGUYÊN)
    app.add_handler(CommandHandler("export", export_command))
    
    # Message handler (ĐÃ CẬP NHẬT)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Callback handler (ĐÃ CẬP NHẬT)
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Threads (GIỮ NGUYÊN)
    threading.Thread(target=schedule_backup, daemon=True).start()
    threading.Thread(target=schedule_cleanup, daemon=True).start()
    threading.Thread(target=check_alerts, daemon=True).start()
    threading.Thread(target=run_health_server, daemon=True).start()
    
    logger.info("✅ Bot sẵn sàng!")
    app.run_polling()

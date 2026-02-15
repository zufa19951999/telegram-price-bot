import os
import sys
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
from telegram.error import TelegramError

# THIẾT LẬP LOGGING CHI TIẾT
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# BẮT LỖI KHỞI ĐỘNG
try:
    # THIẾT LẬP MÚI GIỜ VIỆT NAM (UTC+7)
    def get_vn_time():
        """Lấy thời gian Việt Nam hiện tại (UTC+7)"""
        return datetime.utcnow() + timedelta(hours=7)

    def format_vn_time(format_str="%H:%M:%S %d/%m/%Y"):
        """Format thời gian Việt Nam"""
        return get_vn_time().strftime(format_str)

    load_dotenv()

    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
    CMC_API_KEY = os.getenv('CMC_API_KEY')
    CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

    # KIỂM TRA TOKEN
    if not TELEGRAM_TOKEN:
        logger.error("❌ THIẾU TELEGRAM_TOKEN")
        raise ValueError("TELEGRAM_TOKEN không được để trống")
    
    if not CMC_API_KEY:
        logger.warning("⚠️ THIẾU CMC_API_KEY - Một số chức năng sẽ không hoạt động")

    # ==================== CẤU HÌNH DATABASE TRÊN RENDER DISK ====================
    DATA_DIR = '/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(DATA_DIR, 'crypto_bot.db')
    BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
    EXPORT_DIR = os.path.join(DATA_DIR, 'exports')

    # TẠO THƯ MỤC NẾU CHƯA CÓ
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
        try:
            port = int(os.environ.get('PORT', 10000))
            server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
            logger.info(f"✅ Health server running on port {port}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"❌ Health server error: {e}")
            # Không exit, chỉ log lỗi
            time.sleep(10)

    # ==================== DATABASE SETUP ====================
    def init_database():
        """Khởi tạo database và các bảng"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
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
            
            # Bảng ghi chép chi tiêu (QUẢN LÝ CHI TIÊU)
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
            
            # Bảng thu nhập (QUẢN LÝ CHI TIÊU)
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
            logger.info(f"✅ Database initialized at {DB_PATH}")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo database: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def migrate_database():
        """Cập nhật cấu trúc database nếu cần"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            c = conn.cursor()
            
            # Kiểm tra xem bảng incomes có cột currency chưa
            c.execute("PRAGMA table_info(incomes)")
            columns = [column[1] for column in c.fetchall()]
            
            if 'currency' not in columns:
                logger.info("🔄 Đang cập nhật database: thêm cột currency vào bảng incomes")
                c.execute("ALTER TABLE incomes ADD COLUMN currency TEXT DEFAULT 'VND'")
                conn.commit()
                logger.info("✅ Đã cập nhật database thành công")
            
            # Kiểm tra bảng expenses có cột currency chưa
            c.execute("PRAGMA table_info(expenses)")
            columns = [column[1] for column in c.fetchall()]
            
            if 'currency' not in columns:
                logger.info("🔄 Đang cập nhật database: thêm cột currency vào bảng expenses")
                c.execute("ALTER TABLE expenses ADD COLUMN currency TEXT DEFAULT 'VND'")
                conn.commit()
                logger.info("✅ Đã cập nhật database thành công")
                
        except Exception as e:
            logger.error(f"❌ Lỗi khi migrate database: {e}")
        finally:
            if conn:
                conn.close()
                
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
        try:
            now = time.time()
            for f in os.listdir(BACKUP_DIR):
                if f.startswith('backup_') and f.endswith('.db'):
                    filepath = os.path.join(BACKUP_DIR, f)
                    if os.path.getmtime(filepath) < now - days * 86400:
                        os.remove(filepath)
                        logger.info(f"🗑 Đã xóa backup cũ: {f}")
        except Exception as e:
            logger.error(f"Lỗi clean old backups: {e}")

    def clean_old_exports(hours=24):
        """Xóa file export cũ hơn 24 giờ"""
        try:
            now = time.time()
            for f in os.listdir(EXPORT_DIR):
                if f.startswith('portfolio_') and f.endswith('.csv'):
                    filepath = os.path.join(EXPORT_DIR, f)
                    if os.path.getmtime(filepath) < now - hours * 3600:
                        os.remove(filepath)
                        logger.info(f"🗑 Đã xóa file export cũ: {f}")
        except Exception as e:
            logger.error(f"Lỗi clean old exports: {e}")

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

    # ==================== PORTFOLIO DATABASE FUNCTIONS ====================

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

    # ==================== ALERTS FUNCTIONS ====================

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

    # ==================== HÀM LẤY TỶ GIÁ USDT/VND ====================

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

    def fmt_number(n):
        try:
            n = float(n)
            if n.is_integer():
                return f"{int(n):,}"
            else:
                return f"{n:,.2f}"
        except:
            return str(n)

    # ==================== HÀM HỖ TRỢ ĐA TIỀN TỆ ====================

    SUPPORTED_CURRENCIES = {
        'VND': '🇻🇳 Việt Nam Đồng',
        'USD': '🇺🇸 US Dollar',
        'USDT': '💵 Tether (USDT)',
        'LKR': '🇱🇰 Sri Lanka Rupee',
        'KHR': '🇰🇭 Riel Campuchia',
        'HKD': '🇭🇰 Hong Kong Dollar',
        'SGD': '🇸🇬 Singapore Dollar',
        'JPY': '🇯🇵 Japanese Yen',
        'EUR': '🇪🇺 Euro',
        'GBP': '🇬🇧 British Pound',
        'CNY': '🇨🇳 Chinese Yuan',
        'KRW': '🇰🇷 South Korean Won',
        'THB': '🇹🇭 Thai Baht',
        'MYR': '🇾 Malaysian Ringgit',
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
                return f"៛{amount:,.0f}"
            else:
                return f"{amount:,.2f} {currency}"
        except:
            return f"{amount} {currency}"

    def format_currency_simple(amount, currency):
        """Định dạng số tiền đơn giản để hiển thị"""
        try:
            amount = float(amount)
            if currency == 'VND':
                if amount >= 1000000:
                    return f"{amount/1000000:.1f} triệu VND"
                elif amount >= 1000:
                    return f"{amount/1000:.0f} nghìn VND"
                else:
                    return f"{amount:,.0f} VND"
            elif currency == 'USD':
                return f"${amount:,.2f}"
            elif currency == 'KHR':
                if amount >= 1000:
                    return f"{amount/1000:.1f}K Riel"
                else:
                    return f"៛{amount:,.0f}"
            else:
                return f"{amount:,.2f} {currency}"
        except:
            return f"{amount} {currency}"

    # ==================== HÀM TÍNH TOÁN ẨN ====================

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

    # ==================== HÀM THỐNG KÊ PORTFOLIO ====================

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

    # ==================== HÀM XUẤT CSV ====================

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

    # ==================== EXPENSE DATABASE FUNCTIONS ====================

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

    # ==================== EXPENSE DATABASE FUNCTIONS NÂNG CAO ====================

    def get_expenses_by_category_summary(user_id, period='month'):
        """Lấy tổng hợp chi tiêu theo danh mục (có so sánh với budget)"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            now = get_vn_time()
            
            if period == 'month':
                month_filter = now.strftime("%Y-%m")
                query = '''SELECT ec.id, ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ? AND strftime('%Y-%m', e.expense_date) = ?
                          GROUP BY ec.id, ec.name, ec.budget, e.currency
                          ORDER BY SUM(e.amount) DESC'''
                c.execute(query, (user_id, month_filter))
            elif period == 'year':
                year_filter = now.strftime("%Y")
                query = '''SELECT ec.id, ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ? AND strftime('%Y', e.expense_date) = ?
                          GROUP BY ec.id, ec.name, ec.budget, e.currency
                          ORDER BY SUM(e.amount) DESC'''
                c.execute(query, (user_id, year_filter))
            else:  # all time
                query = '''SELECT ec.id, ec.name, SUM(e.amount), COUNT(e.id), ec.budget, e.currency
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ?
                          GROUP BY ec.id, ec.name, ec.budget, e.currency
                          ORDER BY SUM(e.amount) DESC'''
                c.execute(query, (user_id,))
            
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi expenses summary by category: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def export_expenses_to_csv(user_id):
        """Xuất báo cáo chi tiêu ra CSV"""
        try:
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            filename = f"expense_report_{user_id}_{timestamp}.csv"
            filepath = os.path.join(EXPORT_DIR, filename)
            
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                # Sheet 1: Thu nhập
                writer.writerow(['=== THU NHẬP ==='])
                writer.writerow(['ID', 'Ngày', 'Nguồn', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                
                incomes = get_recent_incomes(user_id, 100)
                total_income = 0
                for inc in incomes:
                    inc_id, amount, source, note, date, currency = inc
                    writer.writerow([inc_id, date, source, amount, currency, note])
                    total_income += amount
                
                writer.writerow([])
                writer.writerow(['Tổng thu nhập', '', '', total_income, 'VND', ''])
                writer.writerow([])
                
                # Sheet 2: Chi tiêu
                writer.writerow(['=== CHI TIÊU ==='])
                writer.writerow(['ID', 'Ngày', 'Danh mục', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                
                expenses = get_recent_expenses(user_id, 100)
                total_expense = 0
                for exp in expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    writer.writerow([exp_id, date, cat_name, amount, currency, note])
                    total_expense += amount
                
                writer.writerow([])
                writer.writerow(['Tổng chi tiêu', '', '', total_expense, 'VND', ''])
                writer.writerow([])
                
                # Sheet 3: Tổng kết
                writer.writerow(['=== TỔNG KẾT ==='])
                writer.writerow(['Chỉ tiêu', 'Số tiền (VND)'])
                writer.writerow(['Tổng thu nhập', total_income])
                writer.writerow(['Tổng chi tiêu', total_expense])
                writer.writerow(['Tiết kiệm', total_income - total_expense])
            
            return filepath, None
        except Exception as e:
            logger.error(f"Lỗi export expenses: {e}")
            return None, str(e)

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

    # ==================== KEYBOARD ====================

    def get_main_keyboard():
        """Keyboard chính"""
        keyboard = [
            [KeyboardButton("💰 ĐẦU TƯ COIN"), 
             KeyboardButton("💸 QUẢN LÝ CHI TIÊU")],
            [KeyboardButton("❓ HƯỚNG DẪN")]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def get_invest_menu_keyboard():
        """Keyboard menu đầu tư coin"""
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

    def get_expense_menu_keyboard():
        """Keyboard menu quản lý chi tiêu (dạng bảng)"""
        keyboard = [
            [InlineKeyboardButton("💰 THU NHẬP", callback_data="expense_income_menu"),
             InlineKeyboardButton("💸 CHI TIÊU", callback_data="expense_expense_menu")],
            [InlineKeyboardButton("📋 DANH MỤC", callback_data="expense_categories"),
             InlineKeyboardButton("📊 BÁO CÁO", callback_data="expense_report_menu")],
            [InlineKeyboardButton("📅 HÔM NAY", callback_data="expense_today"),
             InlineKeyboardButton("📅 THÁNG NÀY", callback_data="expense_month")],
            [InlineKeyboardButton("🔄 GẦN ĐÂY", callback_data="expense_recent"),
             InlineKeyboardButton("📥 XUẤT CSV", callback_data="expense_export")],
            [InlineKeyboardButton("❓ HƯỚNG DẪN", callback_data="expense_help"),
             InlineKeyboardButton("🔙 VỀ MENU CHÍNH", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_income_menu_keyboard():
        """Keyboard menu thu nhập"""
        keyboard = [
            [InlineKeyboardButton("➕ THÊM THU NHẬP", callback_data="expense_add_income"),
             InlineKeyboardButton("📋 XEM THU NHẬP", callback_data="expense_view_incomes")],
            [InlineKeyboardButton("📊 THU NHẬP THÁNG", callback_data="expense_income_month"),
             InlineKeyboardButton("📈 TỔNG HỢP", callback_data="expense_income_summary")],
            [InlineKeyboardButton("🔙 VỀ MENU CHI TIÊU", callback_data="back_to_expense")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_expense_menu_keyboard_sub():
        """Keyboard menu chi tiêu"""
        keyboard = [
            [InlineKeyboardButton("➕ THÊM CHI TIÊU", callback_data="expense_add_expense"),
             InlineKeyboardButton("📋 XEM CHI TIÊU", callback_data="expense_view_expenses")],
            [InlineKeyboardButton("📊 CHI TIÊU THÁNG", callback_data="expense_month"),
             InlineKeyboardButton("📈 THEO DANH MỤC", callback_data="expense_by_category")],
            [InlineKeyboardButton("🔙 VỀ MENU CHI TIÊU", callback_data="back_to_expense")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_categories_menu_keyboard(user_id):
        """Keyboard danh sách danh mục"""
        categories = get_expense_categories(user_id)
        keyboard = []
        row = []
        
        for cat in categories:
            cat_id, name, budget, _ = cat
            display_name = name[:10] + "..." if len(name) > 10 else name
            button = InlineKeyboardButton(f"{display_name}", callback_data=f"cat_view_{cat_id}")
            row.append(button)
            if len(row) == 2:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        
        keyboard.extend([
            [InlineKeyboardButton("➕ THÊM DANH MỤC", callback_data="expense_add_category"),
             InlineKeyboardButton("✏️ SỬA BUDGET", callback_data="expense_edit_budget")],
            [InlineKeyboardButton("🗑 XÓA DANH MỤC", callback_data="expense_delete_category"),
             InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]
        ])
        
        return InlineKeyboardMarkup(keyboard)

    def get_report_menu_keyboard():
        """Keyboard báo cáo"""
        keyboard = [
            [InlineKeyboardButton("📊 HÔM NAY", callback_data="expense_today"),
             InlineKeyboardButton("📊 TUẦN NÀY", callback_data="expense_week")],
            [InlineKeyboardButton("📊 THÁNG NÀY", callback_data="expense_month"),
             InlineKeyboardButton("📊 NĂM NAY", callback_data="expense_year")],
            [InlineKeyboardButton("📊 THEO DANH MỤC", callback_data="expense_by_category"),
             InlineKeyboardButton("📈 CHI TIẾT", callback_data="expense_category_detail")],
            [InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ==================== COMMAND HANDLERS ====================

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
            "• `tn 500000` - Thêm thu nhập 500,000 VND\n"
            "• `tn 100 USD Lương` - Thêm 100 USD, nguồn Lương\n"
            "• `tn 5000 KHR Bán hàng` - Thêm 5,000 Riel\n"
            "• `dm Ăn uống 3000000` - Tạo danh mục\n"
            "• `ct 1 50000 VND Ăn trưa` - Chi tiêu danh mục 1\n"
            "• `ds` - Xem giao dịch gần đây\n"
            "• `bc` - Báo cáo tháng này\n"
            "• `/thongke` - Thống kê theo danh mục (tháng này)\n"
            "• `/thongke year` - Thống kê theo năm\n"
            "• `xoa chi 5` - Xóa khoản chi số 5\n"
            "• `xoa thu 3` - Xóa khoản thu số 3\n"
            "• `xoa dm 2` - Xóa danh mục số 2\n"
            "• `sua budget 1 5000000` - Sửa budget danh mục 1\n\n"
            
            "*TÍNH NĂNG ẨN:*\n"
            "• Gõ phép tính: `(5+3)*2`"
        )
        await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

    # ==================== PORTFOLIO COMMANDS ====================

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

    # ==================== ALERT COMMANDS ====================

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

    # ==================== STATS COMMAND ====================

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

    # ==================== EXPORT COMMAND ====================

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

    # ==================== EXPENSE COMMAND HANDLERS ====================

    async def expense_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Menu quản lý chi tiêu - dạng bảng"""
        await update.message.reply_text(
            "💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
            "👇 Chọn chức năng bên dưới:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_expense_menu_keyboard()
        )

    async def expense_add_income_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Hướng dẫn thêm thu nhập"""
        await update.message.reply_text(
            "💰 *THÊM THU NHẬP*\n\n"
            "Gõ theo cú pháp:\n"
            "`tn [số tiền] [mã tiền tệ] [nguồn] [ghi chú]`\n\n"
            "*Ví dụ:*\n"
            "• `tn 500000` - 500,000 VND\n"
            "• `tn 100 USD Lương` - 100 USD, nguồn Lương\n"
            "• `tn 5000 KHR Bán hàng` - 5,000 Riel\n"
            "• `tn 2000000 Lương tháng 3` - 2 triệu VND\n\n"
            "*Mã tiền tệ:* VND, USD, KHR, LKR, HKD, SGD, JPY...",
            parse_mode=ParseMode.MARKDOWN
        )

    async def expense_add_expense_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Hướng dẫn thêm chi tiêu"""
        uid = update.effective_user.id
        categories = get_expense_categories(uid)
        
        if not categories:
            await update.message.reply_text(
                "❌ Bạn chưa có danh mục chi tiêu nào!\n"
                "Tạo danh mục: `dm [tên] [ngân sách]`\n"
                "VD: `dm Ăn uống 3000000`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        msg = "💸 *THÊM CHI TIÊU*\n\n"
        msg += "Gõ: `ct [mã] [số tiền] [mã tiền tệ] [ghi chú]`\n\n"
        msg += "*Danh mục của bạn:*\n"
        for cat in categories:
            cat_id, name, budget, _ = cat
            budget_str = format_currency_amount(budget, 'VND') if budget > 0 else "Không có"
            msg += f"• `{cat_id}`: {name} (Budget: {budget_str})\n"
        
        msg += "\n*Ví dụ:*\n"
        msg += "• `ct 1 50000 VND Ăn trưa`\n"
        msg += "• `ct 2 20 USD Xăng`\n"
        msg += "• `ct 3 1000 KHR`"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_report_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem báo cáo chi tiêu"""
        uid = update.effective_user.id
        
        msg = await update.message.reply_text("🔄 Đang tổng hợp...")
        
        # Báo cáo tháng này
        expenses = get_expenses_by_period(uid, 'month')
        incomes = get_income_by_period(uid, 'month')
        
        report = f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
        
        if incomes:
            total_income = 0
            report += "*💰 THU NHẬP:*\n"
            for inc in incomes:
                source, amount, count, currency = inc
                total_income += amount
                report += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            report += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
        else:
            report += "📭 Chưa có thu nhập trong tháng.\n\n"
        
        if expenses:
            total_expense = 0
            report += "*💸 CHI TIÊU:*\n"
            for exp in expenses:
                cat_name, amount, count, budget, currency = exp
                total_expense += amount
                report += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            report += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
        else:
            report += "📭 Chưa có chi tiêu trong tháng."
        
        await msg.edit_text(report, parse_mode=ParseMode.MARKDOWN)

    async def expense_today_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem chi tiêu hôm nay"""
        uid = update.effective_user.id
        
        expenses = get_expenses_by_period(uid, 'day')
        incomes = get_income_by_period(uid, 'day')
        
        if not expenses and not incomes:
            await update.message.reply_text(f"📭 Hôm nay chưa có giao dịch nào!")
            return
        
        msg = f"📅 *HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
        
        if incomes:
            msg += "*💰 THU NHẬP:*\n"
            for inc in incomes:
                source, amount, count, currency = inc
                msg += f"• {source}: {format_currency_simple(amount, currency)}\n"
            msg += "\n"
        
        if expenses:
            msg += "*💸 CHI TIÊU:*\n"
            for exp in expenses:
                cat_name, amount, count, budget, currency = exp
                msg += f"• {cat_name}: {format_currency_simple(amount, currency)}\n"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_month_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem chi tiêu tháng này"""
        uid = update.effective_user.id
        
        expenses = get_expenses_by_period(uid, 'month')
        incomes = get_income_by_period(uid, 'month')
        
        if not expenses and not incomes:
            await update.message.reply_text(f"📭 Tháng {get_vn_time().strftime('%m/%Y')} chưa có giao dịch!")
            return
        
        msg = f"📅 *THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
        
        if incomes:
            msg += "*💰 THU NHẬP:*\n"
            for inc in incomes:
                source, amount, count, currency = inc
                msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            msg += "\n"
        
        if expenses:
            msg += "*💸 CHI TIÊU:*\n"
            for exp in expenses:
                cat_name, amount, count, budget, currency = exp
                msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_week_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem chi tiêu tuần này"""
        uid = update.effective_user.id
        
        expenses = get_expenses_by_period(uid, 'week')
        incomes = get_income_by_period(uid, 'week')
        
        now = get_vn_time()
        start_of_week = (now - timedelta(days=now.weekday())).strftime('%d/%m')
        end_of_week = (now + timedelta(days=6-now.weekday())).strftime('%d/%m')
        
        if not expenses and not incomes:
            await update.message.reply_text(f"📭 Tuần này ({start_of_week} - {end_of_week}) chưa có giao dịch!")
            return
        
        msg = f"📅 *TUẦN NÀY ({start_of_week} - {end_of_week})*\n━━━━━━━━━━━━━━━━\n\n"
        
        if incomes:
            total_income = 0
            msg += "*💰 THU NHẬP:*\n"
            for inc in incomes:
                source, amount, count, currency = inc
                total_income += amount
                msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            msg += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
        
        if expenses:
            total_expense = 0
            msg += "*💸 CHI TIÊU:*\n"
            for exp in expenses:
                cat_name, amount, count, budget, currency = exp
                total_expense += amount
                msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            msg += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_year_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem chi tiêu năm nay"""
        uid = update.effective_user.id
        
        expenses = get_expenses_by_period(uid, 'year')
        incomes = get_income_by_period(uid, 'year')
        
        now = get_vn_time()
        
        if not expenses and not incomes:
            await update.message.reply_text(f"📭 Năm {now.strftime('%Y')} chưa có giao dịch!")
            return
        
        msg = f"📅 *NĂM {now.strftime('%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
        
        if incomes:
            total_income = 0
            msg += "*💰 THU NHẬP:*\n"
            for inc in incomes:
                source, amount, count, currency = inc
                total_income += amount
                msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            msg += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
        
        if expenses:
            total_expense = 0
            msg += "*💸 CHI TIÊU:*\n"
            for exp in expenses:
                cat_name, amount, count, budget, currency = exp
                total_expense += amount
                msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
            msg += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_recent_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem giao dịch gần đây"""
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
                msg += f"• #{inc_id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
            msg += "\n"
        
        if recent_expenses:
            msg += "*💸 CHI TIÊU:*\n"
            for exp in recent_expenses:
                exp_id, cat_name, amount, note, date, currency = exp
                msg += f"• #{exp_id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
        
        msg += "\n*Xóa:* `xoa chi [id]` hoặc `xoa thu [id]`"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_manage_categories_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Quản lý danh mục chi tiêu"""
        uid = update.effective_user.id
        
        categories = get_expense_categories(uid)
        
        if not categories:
            await update.message.reply_text(
                "📋 *QUẢN LÝ DANH MỤC*\n\n"
                "Chưa có danh mục nào.\n\n"
                "Tạo mới: `dm [tên] [ngân sách]`\n"
                "VD: `dm Ăn uống 3000000`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        msg = "📋 *DANH MỤC CỦA BẠN*\n━━━━━━━━━━━━━━━━\n\n"
        
        for cat in categories:
            cat_id, name, budget, created = cat
            msg += f"*{cat_id}.* {name}\n"
            msg += f"   Budget: {format_currency_simple(budget, 'VND')}\n"
            msg += f"   Tạo: {created.split()[0]}\n\n"
        
        msg += "*Thao tác:*\n"
        msg += "• `dm [tên] [budget]` - Thêm mới\n"
        msg += "• `sua budget [id] [số tiền]` - Sửa ngân sách\n"
        msg += "• `xoa dm [id]` - Xóa danh mục"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def expense_by_category_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Thống kê chi tiêu theo danh mục"""
        uid = update.effective_user.id
        
        # Xác định kỳ báo cáo (mặc định là tháng này)
        period = 'month'
        period_text = f"tháng {get_vn_time().strftime('%m/%Y')}"
        
        if ctx.args:
            if ctx.args[0].lower() in ['year', 'nam', 'năm']:
                period = 'year'
                period_text = f"năm {get_vn_time().strftime('%Y')}"
            elif ctx.args[0].lower() in ['all', 'tatca', 'tất cả']:
                period = 'all'
                period_text = "tất cả các tháng"
        
        msg = await update.message.reply_text("🔄 Đang tổng hợp số liệu...")
        
        # Lấy dữ liệu
        categories_summary = get_expenses_by_category_summary(uid, period)
        
        if not categories_summary:
            await msg.edit_text(f"📭 Không có dữ liệu chi tiêu trong {period_text}!")
            return
        
        # Tạo báo cáo
        report = f"📊 *THỐNG KÊ CHI TIÊU - {period_text.upper()}*\n"
        report += "━━━━━━━━━━━━━━━━\n\n"
        
        total_expense = 0
        category_details = []
        
        for cat in categories_summary:
            cat_id, cat_name, amount, count, budget, currency = cat
            total_expense += amount
            category_details.append({
                'name': cat_name,
                'amount': amount,
                'count': count,
                'budget': budget,
                'currency': currency
            })
        
        # Hiển thị từng danh mục
        for cat in category_details:
            report += f"*{cat['name']}*\n"
            report += f"💰 Đã chi: {format_currency_simple(cat['amount'], cat['currency'])}\n"
            report += f"📌 Số lần: {cat['count']} lần\n"
            
            if cat['budget'] > 0:
                percent = (cat['amount'] / cat['budget']) * 100
                report += f"📊 Budget: {format_currency_simple(cat['budget'], 'VND')}\n"
                
                # Hiển thị thanh tiến trình
                bar_length = 20
                filled = int(bar_length * percent / 100)
                bar = "█" * filled + "░" * (bar_length - filled)
                report += f"`{bar}` {percent:.1f}%\n"
                
                if percent > 100:
                    report += f"⚠️ *Đã vượt budget {percent-100:.1f}%*\n"
            else:
                report += f"📊 Budget: Chưa thiết lập\n"
            
            report += "\n"
        
        report += "━━━━━━━━━━━━━━━━\n"
        report += f"💸 *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
        
        # Thêm gợi ý
        report += f"\n💡 *Gợi ý:*\n"
        report += "• Xem chi tiết: /baocao\n"
        report += "• Thêm budget: `sua budget [id] [số tiền]`\n"
        report += "• Xem theo năm: /thongke year"
        
        keyboard = [[
            InlineKeyboardButton("📅 Tháng này", callback_data="expense_month"),
            InlineKeyboardButton("📊 Menu", callback_data="back_to_expense")
        ]]
        
        await msg.edit_text(
            report,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ==================== EXPENSE SHORTCUT HANDLERS ====================

    async def expense_shortcut_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xử lý các lệnh tắt cho quản lý chi tiêu"""
        text = update.message.text.strip()
        user_id = update.effective_user.id
        
        # ===== THU NHẬP: tn [số tiền] [mã tiền tệ] [nguồn] [ghi chú] =====
        if text.startswith('tn '):
            parts = text.split()
            
            if len(parts) < 2:
                await update.message.reply_text("❌ Thiếu số tiền! VD: `tn 500000`", parse_mode=ParseMode.MARKDOWN)
                return
            
            try:
                # Lấy số tiền
                amount_str = parts[1].replace(',', '')
                amount = float(amount_str)
                
                if amount <= 0:
                    await update.message.reply_text("❌ Số tiền phải lớn hơn 0!")
                    return
                
                # Mặc định
                currency = 'VND'
                source = "Khác"
                note = ""
                
                # Phân tích cú pháp
                if len(parts) >= 3:
                    # Kiểm tra xem parts[2] có phải mã tiền tệ không
                    if parts[2].upper() in SUPPORTED_CURRENCIES:
                        currency = parts[2].upper()
                        if len(parts) >= 4:
                            source = parts[3]
                            note = " ".join(parts[4:]) if len(parts) > 4 else ""
                    else:
                        # Không có mã tiền tệ, mặc định VND
                        source = parts[2]
                        note = " ".join(parts[3:]) if len(parts) > 3 else ""
                        
                        # Thông báo nếu user cố dùng currency không hỗ trợ
                        if parts[2].upper() not in ['VND', 'USD', 'KHR'] and len(parts[2]) == 3:
                            await update.message.reply_text(
                                f"⚠️ Lưu ý: '{parts[2].upper()}' không nằm trong danh sách hỗ trợ.\n"
                                f"Đã hiểu là nguồn thu nhập."
                            )
                
                # Thêm vào database
                if add_income(user_id, amount, source, currency, note):
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: *{format_currency_simple(amount, currency)}*\n"
                        f"📌 Nguồn: *{source}*\n"
                        f"📝 Ghi chú: *{note if note else 'Không có'}*",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm thu nhập!")
                    
            except ValueError:
                await update.message.reply_text("❌ Số tiền không hợp lệ!")
            except Exception as e:
                logger.error(f"Lỗi thu nhập: {e}")
                await update.message.reply_text("❌ Có lỗi xảy ra!")
        
        # ===== DANH MỤC: dm [tên] [ngân sách] =====
        elif text.startswith('dm '):
            parts = text.split()
            if len(parts) < 2:
                await update.message.reply_text("❌ Thiếu tên danh mục! VD: `dm Ăn uống 3000000`")
                return
            
            name = parts[1]
            budget = 0
            
            if len(parts) > 2:
                try:
                    budget = float(parts[2].replace(',', ''))
                except ValueError:
                    await update.message.reply_text("❌ Ngân sách không hợp lệ!")
                    return
            
            if add_expense_category(user_id, name, budget):
                await update.message.reply_text(
                    f"✅ *ĐÃ THÊM DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                    f"📋 Tên: *{name.upper()}*\n"
                    f"💰 Budget: {format_currency_simple(budget, 'VND')}",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Lỗi khi thêm danh mục!")
        
        # ===== CHI TIÊU: ct [mã danh mục] [số tiền] [mã tiền tệ] [ghi chú] =====
        elif text.startswith('ct '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Thiếu thông tin! VD: `ct 1 50000 VND Ăn trưa`")
                return
            
            try:
                category_id = int(parts[1])
                amount_str = parts[2].replace(',', '')
                amount = float(amount_str)
                
                if amount <= 0:
                    await update.message.reply_text("❌ Số tiền phải lớn hơn 0!")
                    return
                
                # Xác định loại tiền
                currency = 'VND'
                start_idx = 3
                
                if len(parts) > 3 and parts[3].upper() in SUPPORTED_CURRENCIES:
                    currency = parts[3].upper()
                    start_idx = 4
                
                note = " ".join(parts[start_idx:]) if len(parts) > start_idx else ""
                
                # Kiểm tra danh mục tồn tại
                categories = get_expense_categories(user_id)
                category_exists = False
                category_name = ""
                for cat in categories:
                    if cat[0] == category_id:
                        category_exists = True
                        category_name = cat[1]
                        break
                
                if not category_exists:
                    await update.message.reply_text(f"❌ Không tìm thấy danh mục #{category_id}!")
                    return
                
                if add_expense(user_id, category_id, amount, currency, note):
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: *{format_currency_simple(amount, currency)}*\n"
                        f"📂 Danh mục: *{category_name}*\n"
                        f"📝 Ghi chú: *{note if note else 'Không có'}*",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm chi tiêu!")
                    
            except ValueError:
                await update.message.reply_text("❌ ID hoặc số tiền không hợp lệ!")
            except Exception as e:
                logger.error(f"Lỗi chi tiêu: {e}")
                await update.message.reply_text("❌ Có lỗi xảy ra!")
        
        # ===== XEM DANH SÁCH GẦN ĐÂY: ds =====
        elif text == 'ds':
            recent_incomes = get_recent_incomes(user_id, 5)
            recent_expenses = get_recent_expenses(user_id, 5)
            
            if not recent_incomes and not recent_expenses:
                await update.message.reply_text("📭 Chưa có giao dịch nào!")
                return
            
            msg = "🔄 *GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
            
            if recent_incomes:
                msg += "*💰 THU NHẬP:*\n"
                for inc in recent_incomes:
                    inc_id, amount, source, note, date, currency = inc
                    msg += f"• #{inc_id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                msg += "\n"
            
            if recent_expenses:
                msg += "*💸 CHI TIÊU:*\n"
                for exp in recent_expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    msg += f"• #{exp_id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        # ===== BÁO CÁO NHANH: bc =====
        elif text == 'bc':
            await expense_report_handler(update, ctx)
        
        # ===== XÓA CHI TIÊU: xoa chi [id] =====
        elif text.startswith('xoa chi '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa chi 5`")
                return
            
            try:
                expense_id = int(parts[2])
                if delete_expense(expense_id, user_id):
                    await update.message.reply_text(f"✅ Đã xóa khoản chi #{expense_id}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản chi #{expense_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        # ===== XÓA THU NHẬP: xoa thu [id] =====
        elif text.startswith('xoa thu '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa thu 3`")
                return
            
            try:
                income_id = int(parts[2])
                if delete_income(income_id, user_id):
                    await update.message.reply_text(f"✅ Đã xóa khoản thu #{income_id}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản thu #{income_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        # ===== XÓA DANH MỤC: xoa dm [id] =====
        elif text.startswith('xoa dm '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa dm 2`")
                return
            
            try:
                category_id = int(parts[2])
                success, message = delete_expense_category(category_id, user_id)
                if success:
                    await update.message.reply_text(f"✅ {message}")
                else:
                    await update.message.reply_text(f"❌ {message}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        # ===== SỬA BUDGET: sua budget [id] [số tiền] =====
        elif text.startswith('sua budget '):
            parts = text.split()
            if len(parts) < 4:
                await update.message.reply_text("❌ Cần có ID và số tiền! VD: `sua budget 1 5000000`")
                return
            
            try:
                category_id = int(parts[2])
                new_budget = float(parts[3].replace(',', ''))
                
                if update_category_budget(category_id, user_id, new_budget):
                    await update.message.reply_text(
                        f"✅ Đã cập nhật budget cho danh mục #{category_id}\n"
                        f"Budget mới: {format_currency_simple(new_budget, 'VND')}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy danh mục #{category_id}")
            except ValueError:
                await update.message.reply_text("❌ ID hoặc số tiền không hợp lệ!")

    # ==================== HANDLE MESSAGE ====================

    async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        user_id = update.effective_user.id
        
        # XỬ LÝ MENU CHÍNH
        if text == "💰 ĐẦU TƯ COIN":
            await update.message.reply_text(
                "💰 *MENU ĐẦU TƯ COIN*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_invest_menu_keyboard()
            )
            return
            
        elif text == "💸 QUẢN LÝ CHI TIÊU":
            await expense_command(update, ctx)
            return
            
        elif text == "❓ HƯỚNG DẪN":
            await help_command(update, ctx)
            return
        
        # XỬ LÝ CÁC LỆNH TẮT CHI TIÊU
        elif text.startswith(('tn ', 'dm ', 'ct ', 'ds', 'bc', 'xoa chi ', 'xoa thu ', 'xoa dm ', 'sua budget ')):
            await expense_shortcut_handler(update, ctx)
            return
        
        # TÍNH NĂNG ẨN
        elif any(op in text for op in ['+', '-', '*', '/', '%']) and not text.startswith('/'):
            result, error = tinh_toan(text)
            if error:
                await update.message.reply_text(error)
            else:
                if isinstance(result, int):
                    await update.message.reply_text(f"{text} = {result:,}")
                else:
                    formatted = f"{result:,.10f}".rstrip('0').rstrip('.')
                    await update.message.reply_text(f"{text} = {formatted}")
        
        # KHÔNG PHẢI LỆNH HỢP LỆ
        else:
            await update.message.reply_text(
                "❌ Không hiểu lệnh. Gõ /help để xem hướng dẫn.",
                reply_markup=get_main_keyboard()
            )

    # ==================== HANDLE CALLBACK ====================

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
            
            # ========== CALLBACK ĐẦU TƯ COIN ==========
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
            
            # ========== CALLBACK QUẢN LÝ CHI TIÊU ==========
            elif data == "back_to_expense":
                await query.edit_message_text(
                    "💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\nChọn chức năng:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_expense_menu_keyboard()
                )

            elif data == "expense_income_menu":
                await query.edit_message_text(
                    "💰 *MENU THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n• Thêm thu nhập mới\n• Xem lịch sử thu nhập\n• Thống kê theo tháng",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_income_menu_keyboard()
                )

            elif data == "expense_expense_menu":
                await query.edit_message_text(
                    "💸 *MENU CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n• Thêm chi tiêu mới\n• Xem lịch sử chi tiêu\n• Thống kê theo danh mục",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_expense_menu_keyboard_sub()
                )

            elif data == "expense_categories":
                uid = query.from_user.id
                categories = get_expense_categories(uid)
                
                if not categories:
                    msg = "📋 *DANH MỤC CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\nBạn chưa có danh mục nào!\n\n👉 Nhấn '➕ THÊM DANH MỤC' để tạo mới."
                    keyboard = [[InlineKeyboardButton("➕ THÊM DANH MỤC", callback_data="expense_add_category")],
                               [InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                else:
                    msg = "📋 *DANH SÁCH DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                    for cat in categories:
                        cat_id, name, budget, created = cat
                        budget_str = format_currency_simple(budget, 'VND') if budget > 0 else "Chưa có"
                        msg += f"• *{cat_id}. {name}*\n  💰 Budget: {budget_str}\n\n"
                    msg += "👇 Chọn danh mục để xem chi tiết hoặc chọn chức năng bên dưới:"
                
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_categories_menu_keyboard(uid)
                )

            elif data == "expense_report_menu":
                await query.edit_message_text(
                    "📊 *BÁO CÁO CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\nChọn kỳ báo cáo:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_report_menu_keyboard()
                )

            elif data == "expense_today":
                uid = query.from_user.id
                
                expenses = get_expenses_by_period(uid, 'day')
                incomes = get_income_by_period(uid, 'day')
                
                if not expenses and not incomes:
                    await query.edit_message_text(f"📭 Hôm nay chưa có giao dịch nào!")
                    return
                
                msg = f"📅 *HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
                
                if incomes:
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in incomes:
                        source, amount, count, currency = inc
                        msg += f"• {source}: {format_currency_simple(amount, currency)}\n"
                    msg += "\n"
                
                if expenses:
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in expenses:
                        cat_name, amount, count, budget, currency = exp
                        msg += f"• {cat_name}: {format_currency_simple(amount, currency)}\n"
                
                keyboard = [[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data == "expense_week":
                uid = query.from_user.id
                
                expenses = get_expenses_by_period(uid, 'week')
                incomes = get_income_by_period(uid, 'week')
                
                now = get_vn_time()
                start_of_week = (now - timedelta(days=now.weekday())).strftime('%d/%m')
                end_of_week = (now + timedelta(days=6-now.weekday())).strftime('%d/%m')
                
                if not expenses and not incomes:
                    await query.edit_message_text(f"📭 Tuần này ({start_of_week} - {end_of_week}) chưa có giao dịch!")
                    return
                
                msg = f"📅 *TUẦN NÀY ({start_of_week} - {end_of_week})*\n━━━━━━━━━━━━━━━━\n\n"
                
                if incomes:
                    total_income = 0
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in incomes:
                        source, amount, count, currency = inc
                        total_income += amount
                        msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
                
                if expenses:
                    total_expense = 0
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in expenses:
                        cat_name, amount, count, budget, currency = exp
                        total_expense += amount
                        msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
                
                keyboard = [[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data == "expense_month":
                uid = query.from_user.id
                
                expenses = get_expenses_by_period(uid, 'month')
                incomes = get_income_by_period(uid, 'month')
                
                now = get_vn_time()
                
                if not expenses and not incomes:
                    await query.edit_message_text(f"📭 Tháng {now.strftime('%m/%Y')} chưa có giao dịch!")
                    return
                
                msg = f"📅 *THÁNG {now.strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
                
                if incomes:
                    total_income = 0
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in incomes:
                        source, amount, count, currency = inc
                        total_income += amount
                        msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
                
                if expenses:
                    total_expense = 0
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in expenses:
                        cat_name, amount, count, budget, currency = exp
                        total_expense += amount
                        msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
                
                keyboard = [[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data == "expense_year":
                uid = query.from_user.id
                
                expenses = get_expenses_by_period(uid, 'year')
                incomes = get_income_by_period(uid, 'year')
                
                now = get_vn_time()
                
                if not expenses and not incomes:
                    await query.edit_message_text(f"📭 Năm {now.strftime('%Y')} chưa có giao dịch!")
                    return
                
                msg = f"📅 *NĂM {now.strftime('%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
                
                if incomes:
                    total_income = 0
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in incomes:
                        source, amount, count, currency = inc
                        total_income += amount
                        msg += f"• {source}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng thu:* {format_currency_simple(total_income, 'VND')}\n\n"
                
                if expenses:
                    total_expense = 0
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in expenses:
                        cat_name, amount, count, budget, currency = exp
                        total_expense += amount
                        msg += f"• {cat_name}: {format_currency_simple(amount, currency)} ({count} lần)\n"
                    msg += f"\n• *Tổng chi:* {format_currency_simple(total_expense, 'VND')}\n"
                
                keyboard = [[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data == "expense_recent":
                uid = query.from_user.id
                
                recent_expenses = get_recent_expenses(uid, 5)
                recent_incomes = get_recent_incomes(uid, 5)
                
                if not recent_expenses and not recent_incomes:
                    await query.edit_message_text("📭 Chưa có giao dịch nào!")
                    return
                
                msg = "🔄 *GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
                
                if recent_incomes:
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in recent_incomes:
                        inc_id, amount, source, note, date, currency = inc
                        msg += f"• #{inc_id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                    msg += "\n"
                
                if recent_expenses:
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in recent_expenses:
                        exp_id, cat_name, amount, note, date, currency = exp
                        msg += f"• #{exp_id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                
                msg += "\n*Xóa:* `xoa chi [id]` hoặc `xoa thu [id]`"
                
                keyboard = [[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]]
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data == "expense_export":
                uid = query.from_user.id
                await query.edit_message_text("🔄 Đang tạo file báo cáo...")
                
                filepath, error = export_expenses_to_csv(uid)
                
                if error:
                    await query.edit_message_text(f"❌ Lỗi: {error}")
                    return
                
                try:
                    with open(filepath, 'rb') as f:
                        await query.message.reply_document(
                            document=f,
                            filename=os.path.basename(filepath),
                            caption="📊 *BÁO CÁO CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    os.remove(filepath)
                    
                    await query.edit_message_text(
                        "💰 *QUẢN LÝ CHI TIÊU*",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=get_expense_menu_keyboard()
                    )
                except Exception as e:
                    logger.error(f"Lỗi gửi file: {e}")
                    await query.edit_message_text("❌ Lỗi gửi file!")

            elif data == "expense_help":
                help_text = (
                    "❓ *HƯỚNG DẪN CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                    "*📝 CÁC LỆNH NHANH:*\n"
                    "• `tn 500000` - Thêm thu nhập 500k VND\n"
                    "• `tn 100 USD Lương` - Thêm 100 USD\n"
                    "• `dm Ăn uống 3000000` - Tạo danh mục\n"
                    "• `ct 1 50000 VND Ăn sáng` - Chi tiêu\n"
                    "• `ds` - Xem gần đây\n"
                    "• `bc` - Báo cáo tháng\n\n"
                    "*💡 MẸO NHỎ:*\n"
                    "• Tạo danh mục trước khi chi tiêu\n"
                    "• Đặt budget để kiểm soát chi phí\n"
                    "• Xem báo cáo cuối tháng để tổng kết\n\n"
                    "*🌍 HỖ TRỢ ĐA TIỀN TỆ:*\n"
                    "VND, USD, KHR, LKR, HKD, SGD, JPY..."
                )
                
                await query.edit_message_text(
                    help_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]])
                )

            elif data == "expense_add_income":
                await query.edit_message_text(
                    "💰 *THÊM THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n"
                    "Gõ: `tn [số tiền] [mã tiền tệ] [nguồn] [ghi chú]`\n\n"
                    "*Ví dụ:*\n"
                    "• `tn 500000`\n"
                    "• `tn 100 USD Lương`\n"
                    "• `tn 5000 KHR Bán hàng`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]])
                )

            elif data == "expense_add_expense":
                uid = query.from_user.id
                categories = get_expense_categories(uid)
                
                if not categories:
                    await query.edit_message_text(
                        "❌ Bạn chưa có danh mục chi tiêu!\nTạo: `dm [tên] [budget]`",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]])
                    )
                    return
                
                msg = "💸 *THÊM CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                msg += "Gõ: `ct [mã] [số tiền] [mã tiền tệ] [ghi chú]`\n\n"
                msg += "*Danh mục:*\n"
                for cat in categories:
                    msg += f"• `{cat[0]}`: {cat[1]}\n"
                
                await query.edit_message_text(
                    msg, parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]])
                )

            elif data == "expense_add_category":
                await query.edit_message_text(
                    "➕ *THÊM DANH MỤC MỚI*\n━━━━━━━━━━━━━━━━\n\n"
                    "Gõ lệnh: `dm [tên] [ngân sách]`\n\n"
                    "*Ví dụ:*\n"
                    "• `dm Ăn uống 3000000`\n"
                    "• `dm Xăng xe 500000`\n"
                    "• `dm Mua sắm 2000000`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ DANH MỤC", callback_data="expense_categories")]])
                )

            elif data == "expense_by_category":
                uid = query.from_user.id
                await query.edit_message_text("🔄 Đang tổng hợp...")
                
                categories_summary = get_expenses_by_category_summary(uid, 'month')
                
                if not categories_summary:
                    await query.edit_message_text(
                        f"📭 Không có dữ liệu chi tiêu trong tháng {get_vn_time().strftime('%m/%Y')}!",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ MENU", callback_data="back_to_expense")]])
                    )
                    return
                
                report = f"📊 *CHI TIÊU THEO DANH MỤC - THÁNG {get_vn_time().strftime('%m/%Y')}*\n"
                report += "━━━━━━━━━━━━━━━━\n\n"
                
                total = 0
                for cat in categories_summary:
                    cat_id, name, amount, count, budget, currency = cat
                    total += amount
                    report += f"*{name}*\n"
                    report += f"💰 {format_currency_simple(amount, currency)} ({count} lần)\n"
                    
                    if budget > 0:
                        percent = (amount / budget) * 100
                        bar_length = 15
                        filled = int(bar_length * percent / 100)
                        bar = "█" * filled + "░" * (bar_length - filled)
                        report += f"`{bar}` {percent:.1f}%\n"
                    
                    report += "\n"
                
                report += f"💸 *Tổng:* {format_currency_simple(total, 'VND')}"
                
                keyboard = [[
                    InlineKeyboardButton("📅 Tháng này", callback_data="expense_month"),
                    InlineKeyboardButton("🔙 Menu", callback_data="back_to_expense")
                ]]
                
                await query.edit_message_text(
                    report,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data.startswith("cat_view_"):
                cat_id = int(data.replace("cat_view_", ""))
                uid = query.from_user.id
                
                categories = get_expense_categories(uid)
                category = next((c for c in categories if c[0] == cat_id), None)
                
                if not category:
                    await query.edit_message_text("❌ Không tìm thấy danh mục!")
                    return
                
                cat_id, name, budget, created = category
                
                now = get_vn_time()
                month_filter = now.strftime("%Y-%m")
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT SUM(amount), COUNT(id), currency FROM expenses 
                             WHERE user_id = ? AND category_id = ? AND strftime('%Y-%m', expense_date) = ?
                             GROUP BY currency''', (uid, cat_id, month_filter))
                expenses = c.fetchall()
                conn.close()
                
                msg = f"📋 *CHI TIẾT DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                msg += f"*{name}*\n"
                msg += f"💰 Budget: {format_currency_simple(budget, 'VND')}\n"
                msg += f"📅 Tạo: {created.split()[0]}\n\n"
                
                if expenses:
                    total_spent = sum(e[0] for e in expenses)
                    msg += f"*💸 Chi tiêu tháng {now.month}:*\n"
                    for exp in expenses:
                        amount, count, currency = exp
                        msg += f"• {format_currency_simple(amount, currency)} ({count} lần)\n"
                    
                    if budget > 0:
                        remaining = budget - total_spent
                        percent = (total_spent / budget) * 100
                        msg += f"\n*📊 Ngân sách:*\n"
                        msg += f"• Đã dùng: {percent:.1f}%\n"
                        msg += f"• Còn lại: {format_currency_simple(remaining, 'VND')}\n"
                        if remaining < 0:
                            msg += f"⚠️ *Đã vượt budget!*"
                else:
                    msg += f"📭 Chưa có chi tiêu trong tháng {now.month}"
                
                keyboard = [
                    [InlineKeyboardButton("✏️ SỬA BUDGET", callback_data=f"cat_edit_budget_{cat_id}"),
                     InlineKeyboardButton("🗑 XÓA", callback_data=f"cat_delete_{cat_id}")],
                    [InlineKeyboardButton("🔙 VỀ DANH MỤC", callback_data="expense_categories")]
                ]
                
                await query.edit_message_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            elif data.startswith("cat_edit_budget_"):
                cat_id = int(data.replace("cat_edit_budget_", ""))
                
                await query.edit_message_text(
                    f"✏️ *SỬA BUDGET CHO DANH MỤC #{cat_id}*\n\n"
                    f"Dùng lệnh: `sua budget {cat_id} [số tiền]`\n\n"
                    f"*Ví dụ:* `sua budget {cat_id} 5000000`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ DANH MỤC", callback_data="expense_categories")]])
                )

            elif data.startswith("cat_delete_"):
                cat_id = int(data.replace("cat_delete_", ""))
                
                await query.edit_message_text(
                    f"⚠️ *Xác nhận xóa danh mục #{cat_id}?*\n\n"
                    f"Dùng lệnh: `xoa dm {cat_id}` để xóa",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 VỀ DANH MỤC", callback_data="expense_categories")]])
                )

        except Exception as e:
            logger.error(f"Lỗi trong handle_callback: {e}", exc_info=True)
            await query.edit_message_text(
                "❌ Có lỗi xảy ra. Vui lòng thử lại sau.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_main")]])
            )

    # ==================== MAIN ====================
    if __name__ == '__main__':
        try:
            logger.info("🚀 BẮT ĐẦU KHỞI ĐỘNG BOT...")
            
            # Khởi tạo database
            if not init_database():
                logger.error("❌ KHÔNG THỂ KHỞI TẠO DATABASE")
                # Không exit, thử lại sau
                time.sleep(5)
            
            # Migrate database
            try:
                migrate_database()
            except Exception as e:
                logger.error(f"❌ Lỗi migrate database: {e}")
            
            # Kiểm tra quyền ghi
            try:
                test_file = os.path.join(DATA_DIR, 'test.txt')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                logger.info("✅ Disk có quyền ghi")
            except Exception as e:
                logger.error(f"❌ Không có quyền ghi disk: {e}")
            
            # Tạo application
            try:
                app = Application.builder().token(TELEGRAM_TOKEN).build()
                logger.info("✅ Đã tạo Telegram Application")
            except Exception as e:
                logger.error(f"❌ Lỗi tạo Application: {e}")
                raise
            
            # ===== ĐĂNG KÝ HANDLERS =====
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
            
            # Alert commands
            app.add_handler(CommandHandler("alert", alert_command))
            app.add_handler(CommandHandler("alerts", alerts_command))
            app.add_handler(CommandHandler("alert_del", alert_del_command))
            
            # Stats command
            app.add_handler(CommandHandler("stats", stats_command))
            
            # Export command
            app.add_handler(CommandHandler("export", export_command))
            
            # Expense commands
            app.add_handler(CommandHandler("thongke", expense_by_category_handler))
            
            # Message handler
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
            
            # Callback handler
            app.add_handler(CallbackQueryHandler(handle_callback))
            
            logger.info("✅ Đã đăng ký tất cả handlers")
            
            # ===== CHẠY CÁC THREAD =====
            # Thread backup
            backup_thread = threading.Thread(target=schedule_backup, daemon=True)
            backup_thread.start()
            logger.info("✅ Đã khởi động thread backup")
            
            # Thread cleanup
            cleanup_thread = threading.Thread(target=schedule_cleanup, daemon=True)
            cleanup_thread.start()
            logger.info("✅ Đã khởi động thread cleanup")
            
            # Thread check alerts
            alerts_thread = threading.Thread(target=check_alerts, daemon=True)
            alerts_thread.start()
            logger.info("✅ Đã khởi động thread check alerts")
            
            # Thread health server
            health_thread = threading.Thread(target=run_health_server, daemon=True)
            health_thread.start()
            logger.info("✅ Đã khởi động thread health server")
            
            logger.info("🎉 BOT ĐÃ SẴN SÀNG! Bắt đầu polling...")
            
            # Chạy bot với error handling
            app.run_polling(
                timeout=30,
                drop_pending_updates=True,
                allowed_updates=['message', 'callback_query']
            )
            
        except TelegramError as e:
            logger.error(f"❌ LỖI TELEGRAM: {e}")
            time.sleep(5)
            # Thử lại sau 5 giây
            logger.info("🔄 Thử khởi động lại...")
            os.execv(sys.executable, ['python'] + sys.argv)
            
        except Exception as e:
            logger.error(f"❌ LỖI KHÔNG XÁC ĐỊNH: {e}", exc_info=True)
            time.sleep(5)
            # Thử lại sau 5 giây
            logger.info("🔄 Thử khởi động lại...")
            os.execv(sys.executable, ['python'] + sys.argv)

except Exception as e:
    logger.critical(f"💥 LỖI NGHIÊM TRỌNG KHI KHỞI ĐỘNG: {e}", exc_info=True)
    # Không exit, đợi và thử lại
    time.sleep(10)
    os.execv(sys.executable, ['python'] + sys.argv)

"""
Crypto & Expense Manager Bot - Optimized for Render
Author: Assistant
Version: 2.0 - Render Optimized
"""

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
import gc
import psutil
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError
from functools import wraps
from flask import Flask, request
import asyncio

# ==================== RENDER CONFIGURATION ====================
class RenderConfig:
    def __init__(self):
        self.is_render = os.environ.get('RENDER', False)
        self.memory_limit = int(os.environ.get('MEMORY_LIMIT', 512))  # MB
        self.cpu_limit = float(os.environ.get('CPU_LIMIT', 1))
        self.render_url = os.environ.get('RENDER_EXTERNAL_URL')
        self.start_time = time.time()
        
    def get_worker_count(self):
        """Auto-adjust workers based on CPU"""
        if self.is_render:
            return max(1, int(self.cpu_limit) * 2)
        return 4
    
    def should_cleanup(self):
        """Check if memory cleanup needed"""
        try:
            memory_percent = psutil.virtual_memory().percent
            return memory_percent > 80
        except:
            return False

render_config = RenderConfig()

# ==================== THIẾT LẬP LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==================== THỜI GIAN VIỆT NAM ====================
def get_vn_time():
    return datetime.utcnow() + timedelta(hours=7)

def format_vn_time():
    return get_vn_time().strftime("%H:%M:%S %d/%m/%Y")

def format_vn_time_short():
    return get_vn_time().strftime("%H:%M %d/%m")

# ==================== ADVANCED CACHE SYSTEM ====================
class AdvancedCache:
    def __init__(self, name, max_size=100, ttl=300):
        self.name = name
        self.cache = {}
        self.max_size = max_size
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
        
    def get(self, key):
        if key in self.cache:
            data, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                self.hits += 1
                return data
            else:
                del self.cache[key]
        self.misses += 1
        return None
    
    def set(self, key, value):
        # Remove oldest if full
        if len(self.cache) >= self.max_size:
            oldest = min(self.cache.keys(), 
                        key=lambda k: self.cache[k][1])
            del self.cache[oldest]
        
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        logger.info(f"🧹 Cache {self.name} cleared")
    
    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': round(hit_rate, 2)
        }

# Initialize caches
price_cache = AdvancedCache('price', max_size=50, ttl=60)  # 1 phút
usdt_cache = AdvancedCache('usdt', max_size=1, ttl=180)    # 3 phút

# ==================== RATE LIMITING ====================
class SecurityManager:
    def __init__(self):
        self.rate_limits = {}
        self.max_requests_per_minute = 30

security = SecurityManager()

def rate_limit(max_calls=30):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            current_time = time.time()
            
            if user_id in security.rate_limits:
                calls, first_call = security.rate_limits[user_id]
                if current_time - first_call < 60:
                    if calls >= max_calls:
                        await update.message.reply_text(f"⚠️ Quá nhiều request. Thử lại sau 1 phút.\n\n🕐 {format_vn_time()}")
                        return
                    security.rate_limits[user_id] = (calls + 1, first_call)
                else:
                    security.rate_limits[user_id] = (1, current_time)
            else:
                security.rate_limits[user_id] = (1, current_time)
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

# ==================== KHỞI TẠO ====================
try:
    load_dotenv()

    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
    CMC_API_KEY = os.getenv('CMC_API_KEY')
    CMC_API_URL = "https://pro-api.coinmarketcap.com/v1"

    if not TELEGRAM_TOKEN:
        logger.error("❌ THIẾU TELEGRAM_TOKEN")
        raise ValueError("TELEGRAM_TOKEN không được để trống")
    
    if not CMC_API_KEY:
        logger.warning("⚠️ THIẾU CMC_API_KEY")

    # ==================== CẤU HÌNH DATABASE ====================
    DATA_DIR = '/data' if os.path.exists('/data') else os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(DATA_DIR, 'crypto_bot.db')
    BACKUP_DIR = os.path.join(DATA_DIR, 'backups')
    EXPORT_DIR = os.path.join(DATA_DIR, 'exports')

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    logger.info(f"📁 Database: {DB_PATH}")
    logger.info(f"🚀 Render mode: {render_config.is_render}")

    app = None
    webhook_app = Flask(__name__)

    # ==================== DATABASE OPTIMIZATION ====================
    def optimize_database():
        """Nén database và xóa dữ liệu cũ"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # VACUUM để nén database
            c.execute("VACUUM")
            
            # Xóa alerts cũ (hơn 30 ngày)
            c.execute('''DELETE FROM alerts 
                         WHERE triggered_at IS NOT NULL 
                         AND date(triggered_at) < date('now', '-30 days')''')
            
            conn.commit()
            conn.close()
            
            # Clean log file
            if os.path.exists('bot.log'):
                with open('bot.log', 'r') as f:
                    lines = f.readlines()
                if len(lines) > 1000:
                    with open('bot.log', 'w') as f:
                        f.writelines(lines[-1000:])
            
            # Tính dung lượng
            size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
            logger.info(f"✅ Database optimized: {size_mb:.2f}MB")
            
        except Exception as e:
            logger.error(f"❌ Lỗi optimize DB: {e}")

    # ==================== MEMORY MONITOR ====================
    def check_memory_usage():
        """Kiểm tra memory và cleanup nếu cần"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            logger.info(f"📊 Memory: {memory_mb:.2f}MB | CPU: {cpu_percent:.1f}% | "
                       f"Cache: P{price_cache.get_stats()['size']}/U{usdt_cache.get_stats()['size']}")
            
            # Nếu dùng quá 70% memory limit
            if memory_mb > render_config.memory_limit * 0.7:
                logger.warning("⚠️ Memory high, cleaning caches...")
                price_cache.clear()
                usdt_cache.clear()
                gc.collect()
                
            # Nếu vẫn cao sau cleanup
            if memory_mb > render_config.memory_limit * 0.9:
                logger.critical("💥 Memory critical, restarting...")
                sys.exit(1)  # Render sẽ tự restart
                
        except Exception as e:
            logger.error(f"❌ Memory check error: {e}")

    def memory_monitor():
        while True:
            check_memory_usage()
            time.sleep(300)  # Check mỗi 5 phút

    # ==================== DATABASE SETUP ====================
    def init_database():
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            c = conn.cursor()
            
            c.execute('''CREATE TABLE IF NOT EXISTS portfolio
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER, symbol TEXT, amount REAL,
                          buy_price REAL, buy_date TEXT, total_cost REAL)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS alerts
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER, symbol TEXT, target_price REAL,
                          condition TEXT, is_active INTEGER DEFAULT 1,
                          created_at TEXT, triggered_at TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS expense_categories
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER, name TEXT, budget REAL, created_at TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS expenses
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER, category_id INTEGER, amount REAL,
                          currency TEXT DEFAULT 'VND', note TEXT,
                          expense_date TEXT, created_at TEXT,
                          FOREIGN KEY (category_id) REFERENCES expense_categories(id))''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS incomes
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER, amount REAL, currency TEXT DEFAULT 'VND',
                          source TEXT, income_date TEXT, note TEXT, created_at TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS users
                         (user_id INTEGER PRIMARY KEY,
                          username TEXT, first_name TEXT, last_name TEXT, last_seen TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS permissions
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          group_id INTEGER, admin_id INTEGER, granted_by INTEGER,
                          can_view_all INTEGER DEFAULT 1, can_edit_all INTEGER DEFAULT 0,
                          can_delete_all INTEGER DEFAULT 0, can_manage_perms INTEGER DEFAULT 0,
                          created_at TEXT)''')
            
            conn.commit()
            logger.info(f"✅ Database initialized")
            
            # Tạo indexes cho performance
            c.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_user ON portfolio(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(is_active)")
            conn.commit()
            
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi database: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def migrate_database():
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            c = conn.cursor()
            
            c.execute("PRAGMA table_info(incomes)")
            columns = [column[1] for column in c.fetchall()]
            if 'currency' not in columns:
                c.execute("ALTER TABLE incomes ADD COLUMN currency TEXT DEFAULT 'VND'")
            
            c.execute("PRAGMA table_info(expenses)")
            columns = [column[1] for column in c.fetchall()]
            if 'currency' not in columns:
                c.execute("ALTER TABLE expenses ADD COLUMN currency TEXT DEFAULT 'VND'")
                
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Lỗi migrate: {e}")
        finally:
            if conn:
                conn.close()
                
    def backup_database():
        try:
            if os.path.exists(DB_PATH):
                # Chỉ backup nếu database > 1MB
                if os.path.getsize(DB_PATH) > 1024 * 1024:
                    timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                    backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')
                    shutil.copy2(DB_PATH, backup_path)
                    
                    # Xóa backup cũ hơn 7 ngày
                    for f in os.listdir(BACKUP_DIR):
                        f_path = os.path.join(BACKUP_DIR, f)
                        if os.path.getctime(f_path) < time.time() - 7 * 86400:
                            os.remove(f_path)
        except Exception as e:
            logger.error(f"❌ Lỗi backup: {e}")

    def schedule_backup():
        while True:
            try:
                backup_database()
                time.sleep(86400)  # 24h
            except:
                time.sleep(3600)

    # ==================== BATCH PRICE FETCHING ====================
    def get_prices_batch(symbols):
        """Lấy giá nhiều coin cùng lúc"""
        try:
            if not CMC_API_KEY or not symbols:
                return {}
            
            # Check cache trước
            results = {}
            uncached = []
            
            for symbol in symbols:
                cached = price_cache.get(symbol)
                if cached:
                    results[symbol] = cached
                else:
                    uncached.append(symbol)
            
            if uncached:
                # Gom nhóm theo từng 10 coin
                for i in range(0, len(uncached), 10):
                    batch = uncached[i:i+10]
                    symbols_str = ','.join(batch)
                    
                    headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
                    params = {'symbol': symbols_str, 'convert': 'USD'}
                    
                    res = requests.get(
                        f"{CMC_API_URL}/cryptocurrency/quotes/latest",
                        headers=headers,
                        params=params,
                        timeout=10
                    )
                    
                    if res.status_code == 200:
                        data = res.json()
                        for symbol in batch:
                            if symbol in data['data']:
                                coin_data = data['data'][symbol]
                                quote = coin_data['quote']['USD']
                                result = {
                                    'p': quote['price'],
                                    'v': quote['volume_24h'],
                                    'c': quote['percent_change_24h'],
                                    'm': quote['market_cap'],
                                    'n': coin_data['name'],
                                    'r': coin_data.get('cmc_rank', 'N/A')
                                }
                                results[symbol] = result
                                price_cache.set(symbol, result)
                    
                    time.sleep(0.5)  # Tránh rate limit
            
            return results
        except Exception as e:
            logger.error(f"❌ Batch price error: {e}")
            return {}

    def get_price(symbol):
        """Lấy giá 1 coin (có cache)"""
        cached = price_cache.get(symbol)
        if cached:
            return cached
            
        try:
            if not CMC_API_KEY:
                return None
                
            clean_symbol = symbol.upper()
            if clean_symbol == 'USDT':
                clean = 'USDT'
            else:
                clean = clean_symbol.replace('USDT', '').replace('USD', '')
            
            headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
            params = {'symbol': clean, 'convert': 'USD'}
            
            res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest", 
                              headers=headers, params=params, timeout=10)
            
            if res.status_code == 200:
                data = res.json()
                if 'data' not in data or clean not in data['data']:
                    return None
                    
                coin_data = data['data'][clean]
                quote_data = coin_data['quote']['USD']
                
                result = {
                    'p': quote_data['price'],
                    'v': quote_data['volume_24h'],
                    'c': quote_data['percent_change_24h'],
                    'm': quote_data['market_cap'],
                    'n': coin_data['name'],
                    'r': coin_data.get('cmc_rank', 'N/A')
                }
                price_cache.set(symbol, result)
                return result
            else:
                return None
        except Exception as e:
            logger.error(f"❌ Lỗi get_price {symbol}: {e}")
            return None

    def get_usdt_vnd_rate():
        """Lấy tỷ giá USDT/VND (có cache)"""
        cached = usdt_cache.get('rate')
        if cached:
            return cached
        
        try:
            # Thử CoinGecko trước
            try:
                url = "https://api.coingecko.com/api/v3/simple/price"
                params = {'ids': 'tether', 'vs_currencies': 'vnd'}
                res = requests.get(url, params=params, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if 'tether' in data:
                        vnd_rate = float(data['tether']['vnd'])
                        result = {
                            'source': 'CoinGecko',
                            'vnd': vnd_rate,
                            'update_time': format_vn_time()
                        }
                        usdt_cache.set('rate', result)
                        return result
            except:
                pass
            
            # Fallback
            result = {
                'source': 'Fallback (25000)',
                'vnd': 25000,
                'update_time': format_vn_time()
            }
            usdt_cache.set('rate', result)
            return result
        except Exception as e:
            logger.error(f"❌ Lỗi get_usdt_vnd_rate: {e}")
            return {'source': 'Error', 'vnd': 25000, 'update_time': format_vn_time()}

    # ==================== PORTFOLIO FUNCTIONS ====================
    def add_transaction(user_id, symbol, amount, buy_price):
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
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi thêm transaction: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_portfolio(user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost 
                         FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
                      (user_id,))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy portfolio: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_transaction_detail(user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost 
                         FROM portfolio WHERE user_id = ? ORDER BY buy_date''',
                      (user_id,))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy transaction: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def delete_transaction(transaction_id, user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''DELETE FROM portfolio WHERE id = ? AND user_id = ?''',
                      (transaction_id, user_id))
            conn.commit()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Lỗi xóa transaction: {e}")
            return False
        finally:
            if conn:
                conn.close()

    # ==================== ALERTS FUNCTIONS ====================
    def add_alert(user_id, symbol, target_price, condition):
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
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi thêm alert: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_user_alerts(user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, symbol, target_price, condition, created_at 
                         FROM alerts WHERE user_id = ? AND is_active = 1 
                         ORDER BY created_at''', (user_id,))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy alerts: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def delete_alert(alert_id, user_id):
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
                        msg = (f"🔔 *CẢNH BÁO GIÁ*\n━━━━━━━━━━━━━━━━\n\n"
                               f"• Coin: *{symbol}*\n"
                               f"• Giá hiện: `{fmt_price(current_price)}`\n"
                               f"• Mốc: `{fmt_price(target_price)}`\n"
                               f"• Điều kiện: {'📈 Lên trên' if condition == 'above' else '📉 Xuống dưới'}\n\n"
                               f"🕐 {format_vn_time()}")
                        
                        try:
                            app.bot.send_message(user_id, msg, parse_mode='Markdown')
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute('''UPDATE alerts SET is_active = 0, triggered_at = ? 
                                         WHERE id = ?''', 
                                      (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), alert_id))
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            logger.error(f"❌ Lỗi gửi alert {alert_id}: {e}")
            except Exception as e:
                logger.error(f"❌ Lỗi check_alerts: {e}")
                time.sleep(10)

    # ==================== PERMISSIONS FUNCTIONS ====================
    def grant_permission(group_id, admin_id, granted_by, permissions):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute("DELETE FROM permissions WHERE group_id = ? AND admin_id = ?", (group_id, admin_id))
            
            c.execute('''INSERT INTO permissions 
                         (group_id, admin_id, granted_by, can_view_all, can_edit_all, 
                          can_delete_all, can_manage_perms, created_at)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, admin_id, granted_by,
                       permissions.get('view', 1),
                       permissions.get('edit', 0),
                       permissions.get('delete', 0),
                       permissions.get('manage', 0),
                       created_at))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi cấp quyền: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def revoke_permission(group_id, admin_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM permissions WHERE group_id = ? AND admin_id = ?", (group_id, admin_id))
            conn.commit()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Lỗi thu hồi quyền: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def check_permission(group_id, user_id, permission_type='view'):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                         FROM permissions WHERE group_id = ? AND admin_id = ?''',
                      (group_id, user_id))
            result = c.fetchone()
            
            if not result:
                return False
            
            can_view, can_edit, can_delete, can_manage = result
            
            if permission_type == 'view':
                return can_view == 1
            elif permission_type == 'edit':
                return can_edit == 1
            elif permission_type == 'delete':
                return can_delete == 1
            elif permission_type == 'manage':
                return can_manage == 1
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra quyền: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_all_admins(group_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT p.admin_id, p.can_view_all, p.can_edit_all, p.can_delete_all, 
                                p.can_manage_perms
                         FROM permissions p
                         WHERE p.group_id = ?
                         ORDER BY p.created_at''', (group_id,))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy danh sách admin: {e}")
            return []
        finally:
            if conn:
                conn.close()

    # ==================== USER FUNCTIONS ====================
    def update_user_info(user):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO users 
                         (user_id, username, first_name, last_name, last_seen)
                         VALUES (?, ?, ?, ?, ?)''',
                      (user.id, user.username, user.first_name, user.last_name,
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi cập nhật user: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_user_id_by_username(username):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE username = ?", (username,))
            result = c.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"❌ Lỗi tìm user: {e}")
            return None
        finally:
            if conn:
                conn.close()

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

    def format_currency_simple(amount, currency):
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
            else:
                return f"{amount:,.2f} {currency}"
        except:
            return f"{amount} {currency}"

    SUPPORTED_CURRENCIES = {
        'VND': '🇻🇳 Việt Nam Đồng',
        'USD': '🇺🇸 US Dollar',
        'USDT': '💵 Tether',
        'KHR': '🇰🇭 Riel Campuchia',
        'LKR': '🇱🇰 Sri Lanka Rupee'
    }

    # ==================== EXPENSE FUNCTIONS ====================
    def add_expense_category(user_id, name, budget=0):
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

    def add_income(user_id, amount, source, currency='VND', note=""):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            income_date = now.strftime("%Y-%m-%d")
            created_at = now.strftime("%Y-%m-%d %H:%M:%S")
            
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
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            expense_date = now.strftime("%Y-%m-%d")
            created_at = now.strftime("%Y-%m-%d %H:%M:%S")
            
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
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, amount, source, note, income_date, currency
                         FROM incomes WHERE user_id = ?
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
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            
            if period == 'day':
                date_filter = now.strftime("%Y-%m-%d")
                query = '''SELECT id, amount, source, note, currency, income_date
                          FROM incomes WHERE user_id = ? AND income_date = ?
                          ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, date_filter))
            elif period == 'month':
                month_filter = now.strftime("%Y-%m")
                query = '''SELECT id, amount, source, note, currency, income_date
                          FROM incomes WHERE user_id = ? AND strftime('%Y-%m', income_date) = ?
                          ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, month_filter))
            else:
                year_filter = now.strftime("%Y")
                query = '''SELECT id, amount, source, note, currency, income_date
                          FROM incomes WHERE user_id = ? AND strftime('%Y', income_date) = ?
                          ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, year_filter))
            
            rows = c.fetchall()
            
            summary = {}
            for row in rows:
                id, amount, source, note, currency, date = row
                if currency not in summary:
                    summary[currency] = 0
                summary[currency] += amount
            
            return {
                'transactions': rows,
                'summary': summary,
                'total_count': len(rows)
            }
        except Exception as e:
            logger.error(f"❌ Lỗi income summary: {e}")
            return {'transactions': [], 'summary': {}, 'total_count': 0}
        finally:
            if conn:
                conn.close()

    def get_expenses_by_period(user_id, period='month'):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            
            if period == 'day':
                date_filter = now.strftime("%Y-%m-%d")
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ? AND e.expense_date = ?
                          ORDER BY e.expense_date DESC, e.created_at DESC'''
                c.execute(query, (user_id, date_filter))
            elif period == 'month':
                month_filter = now.strftime("%Y-%m")
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ? AND strftime('%Y-%m', e.expense_date) = ?
                          ORDER BY e.expense_date DESC, e.created_at DESC'''
                c.execute(query, (user_id, month_filter))
            else:
                year_filter = now.strftime("%Y")
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget
                          FROM expenses e
                          JOIN expense_categories ec ON e.category_id = ec.id
                          WHERE e.user_id = ? AND strftime('%Y', e.expense_date) = ?
                          ORDER BY e.expense_date DESC, e.created_at DESC'''
                c.execute(query, (user_id, year_filter))
            
            rows = c.fetchall()
            
            summary = {}
            category_summary = {}
            
            for row in rows:
                id, cat_name, amount, note, currency, date, budget = row
                if currency not in summary:
                    summary[currency] = 0
                summary[currency] += amount
                
                key = f"{cat_name}_{currency}"
                if key not in category_summary:
                    category_summary[key] = {
                        'category': cat_name,
                        'currency': currency,
                        'total': 0,
                        'count': 0,
                        'budget': budget
                    }
                category_summary[key]['total'] += amount
                category_summary[key]['count'] += 1
            
            return {
                'transactions': rows,
                'summary': summary,
                'category_summary': category_summary,
                'total_count': len(rows)
            }
        except Exception as e:
            logger.error(f"❌ Lỗi expenses summary: {e}")
            return {'transactions': [], 'summary': {}, 'category_summary': {}, 'total_count': 0}
        finally:
            if conn:
                conn.close()

    def delete_expense(expense_id, user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''DELETE FROM expenses WHERE id = ? AND user_id = ?''', (expense_id, user_id))
            conn.commit()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Lỗi xóa expense: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def delete_income(income_id, user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''DELETE FROM incomes WHERE id = ? AND user_id = ?''', (income_id, user_id))
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
        keyboard = [
            [KeyboardButton("💰 ĐẦU TƯ COIN"), KeyboardButton("💸 QUẢN LÝ CHI TIÊU")],
            [KeyboardButton("❓ HƯỚNG DẪN")]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def get_invest_menu_keyboard(user_id=None, group_id=None):
        keyboard = [
            [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
             InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
             InlineKeyboardButton("Ξ SOL", callback_data="price_SOL"),
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
        
        if group_id and user_id:
            try:
                if check_permission(group_id, user_id, 'view'):
                    keyboard.append([InlineKeyboardButton("👑 ADMIN", callback_data="admin_panel")])
            except:
                pass
        
        return InlineKeyboardMarkup(keyboard)

    def get_expense_menu_keyboard():
        keyboard = [
            [InlineKeyboardButton("💰 THU NHẬP", callback_data="expense_income_menu"),
             InlineKeyboardButton("💸 CHI TIÊU", callback_data="expense_expense_menu")],
            [InlineKeyboardButton("📋 DANH MỤC", callback_data="expense_categories"),
             InlineKeyboardButton("📊 BÁO CÁO", callback_data="expense_report_menu")],
            [InlineKeyboardButton("📅 HÔM NAY", callback_data="expense_today"),
             InlineKeyboardButton("📅 THÁNG NÀY", callback_data="expense_month")],
            [InlineKeyboardButton("🔄 GẦN ĐÂY", callback_data="expense_recent"),
             InlineKeyboardButton("📥 XUẤT CSV", callback_data="expense_export")],
            [InlineKeyboardButton("🔙 VỀ MENU CHÍNH", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ==================== COMMAND HANDLERS ====================
    async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.type in ['group', 'supergroup']:
            welcome_msg = (
                "🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*\n\n"
                "🤖 Bot đã sẵn sàng!\n\n"
                "*Các lệnh trong nhóm:*\n"
                "• `/s btc eth` - Xem giá coin\n"
                "• `/usdt` - Tỷ giá USDT/VND\n"
                "• `/buy btc 0.5 40000` - Mua coin\n"
                "• `/sell btc 0.2` - Bán coin\n\n"
                "📱 *Vuốt xuống để hiện menu*\n"
                f"🕐 {format_vn_time()}"
            )
            await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())
        else:
            welcome_msg = (
                "🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*\n\n"
                "🤖 Bot hỗ trợ:\n\n"
                "*💎 ĐẦU TƯ COIN:*\n"
                "• Xem giá coin\n• Top 10 coin\n• Quản lý danh mục\n• Tính lợi nhuận\n• Cảnh báo giá\n\n"
                "*💰 QUẢN LÝ CHI TIÊU:*\n"
                "• Ghi chép thu/chi\n• Đa tiền tệ\n• Quản lý ngân sách\n• Báo cáo ngày/tháng/năm\n\n"
                f"🕐 *Hiện tại:* `{format_vn_time()}`\n\n"
                "👇 *Chọn chức năng bên dưới*"
            )
            await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())

    async def menu_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👇 *Chọn chức năng bên dưới*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_main_keyboard()
        )

    async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        help_msg = (
            "📘 *HƯỚNG DẪN*\n\n"
            "*ĐẦU TƯ COIN:*\n"
            "• `/s btc eth` - Xem giá coin\n"
            "• `/usdt` - Tỷ giá USDT/VND\n"
            "• `/buy btc 0.5 40000` - Mua coin\n"
            "• `/sell btc 0.2` - Bán coin\n"
            "• `/edit` - Xem/sửa giao dịch\n"
            "• `/del [id]` - Xóa giao dịch\n"
            "• `/alert BTC above 50000` - Cảnh báo giá\n"
            "• `/alerts` - Xem cảnh báo\n"
            "• `/stats` - Thống kê\n\n"
            "*QUẢN LÝ CHI TIÊU:*\n"
            "• `tn 500000` - Thêm thu nhập\n"
            "• `dm Ăn uống` - Tạo danh mục\n"
            "• `ct 1 50000 VND Ăn trưa` - Chi tiêu\n"
            "• `ds` - Xem gần đây\n"
            "• `bc` - Báo cáo tháng\n"
            "• `xoa chi 5` - Xóa chi\n"
            "• `xoa thu 3` - Xóa thu\n"
        )
        
        if chat_type in ['group', 'supergroup'] and check_permission(chat_id, user_id, 'view'):
            help_msg += "\n*👑 QUẢN TRỊ:*\n"
            help_msg += "• `/perm list` - Danh sách admin\n"
            help_msg += "• `/perm grant @user view` - Cấp quyền xem\n"
            help_msg += "• `/perm grant @user edit` - Cấp quyền sửa\n"
            help_msg += "• `/perm grant @user delete` - Cấp quyền xóa\n"
            help_msg += "• `/perm grant @user manage` - Cấp quyền QL\n"
            help_msg += "• `/perm revoke @user` - Thu hồi quyền\n"
        
        help_msg += f"\n🕐 {format_vn_time()}"
        await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

    @rate_limit(30)
    async def usdt_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = await update.message.reply_text("🔄 Đang tra cứu...")
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
        
        await msg.delete()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

    @rate_limit(30)
    async def s_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not ctx.args:
            return await update.message.reply_text("❌ /s btc eth doge")
        
        msg = await update.message.reply_text("🔄 Đang tra cứu...")
        
        # Lấy giá batch
        symbols = [arg.upper() for arg in ctx.args]
        prices = get_prices_batch(symbols)
        
        results = []
        for symbol in symbols:
            d = prices.get(symbol)
            if d:
                if symbol == 'USDT':
                    rate_data = get_usdt_vnd_rate()
                    vnd_price = rate_data['vnd']
                    results.append(f"*{d['n']}* #{d['r']}\n💰 USD: `{fmt_price(d['p'])}`\n🇻🇳 VND: `{fmt_vnd(vnd_price)}`\n📈 24h: `{d['c']:.2f}%`")
                else:
                    results.append(f"*{d['n']}* #{d['r']}\n💰 Giá: `{fmt_price(d['p'])}`\n📈 24h: `{d['c']:.2f}%`")
            else:
                results.append(f"❌ *{symbol}*: Không có dữ liệu")
        
        await msg.delete()
        await update.message.reply_text("\n━━━━━━━━━━━━\n".join(results) + f"\n\n🕐 {format_vn_time_short()}", parse_mode='Markdown')

    @rate_limit(30)
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
                f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
                f"🕐 {format_vn_time()}"
            )
            await update.message.reply_text(msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"❌ Lỗi khi thêm giao dịch *{symbol}*", parse_mode='Markdown')

    @rate_limit(30)
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
            portfolio.append({'symbol': row[0], 'amount': row[1], 'buy_price': row[2], 'buy_date': row[3], 'total_cost': row[4]})
        
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
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM portfolio WHERE user_id = ?", (uid,))
        for tx in new_portfolio:
            c.execute('''INSERT INTO portfolio (user_id, symbol, amount, buy_price, buy_date, total_cost)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (uid, tx['symbol'], tx['amount'], tx['buy_price'], tx['buy_date'], tx['total_cost']))
        conn.commit()
        conn.close()
        
        profit = sold_value - sold_cost
        profit_percent = (profit / sold_cost) * 100 if sold_cost > 0 else 0
        
        msg = (
            f"✅ *ĐÃ BÁN {sell_amount:.4f} {symbol}*\n━━━━━━━━━━━━━━━━\n\n"
            f"💰 Giá bán: `{fmt_price(current_price)}`\n"
            f"💵 Giá trị: `{fmt_price(sold_value)}`\n"
            f"📊 Vốn: `{fmt_price(sold_cost)}`\n"
            f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
            f"🕐 {format_vn_time()}"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    @rate_limit(30)
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
    
            msg += f"\n🕐 {format_vn_time_short()}"
    
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
                    f"*Sửa:* `/edit {tx_id} [sl] [giá]`\n*Xóa:* `/del {tx_id}`\n\n"
                    f"🕐 {format_vn_time()}"
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
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                new_total = new_amount * new_price
                c.execute('''UPDATE portfolio SET amount = ?, buy_price = ?, total_cost = ?
                             WHERE id = ? AND user_id = ?''',
                          (new_amount, new_price, new_total, tx_id, uid))
                conn.commit()
                affected = c.rowcount
                conn.close()
                
                if affected > 0:
                    await update.message.reply_text(
                        f"✅ Đã cập nhật giao dịch #{tx_id}\n"
                        f"📊 SL mới: `{new_amount:.4f}`\n"
                        f"💰 Giá mới: `{fmt_price(new_price)}`\n\n"
                        f"🕐 {format_vn_time()}",
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
            except ValueError:
                await update.message.reply_text("❌ /edit [id] [sl] [giá]")
        else:
            await update.message.reply_text("❌ /edit - Xem DS\n/edit [id] - Xem chi tiết\n/edit [id] [sl] [giá] - Sửa")

    @rate_limit(30)
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
                f"⚠️ *Xác nhận xóa giao dịch #{tx_id}?*\n\n🕐 {format_vn_time_short()}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ")

    @rate_limit(30)
    async def alert_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if len(ctx.args) < 3:
            await update.message.reply_text("❌ /alert BTC above 50000", parse_mode='Markdown')
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
                f"🕐 {format_vn_time()}"
            )
            await update.message.reply_text(msg, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Lỗi khi tạo cảnh báo!")

    @rate_limit(30)
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
        
        msg += f"🕐 {format_vn_time_short()}"
        await update.message.reply_text(msg, parse_mode='Markdown')

    @rate_limit(30)
    async def stats_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        msg = await update.message.reply_text("🔄 Đang tính toán thống kê...")
        
        stats = get_portfolio_stats(uid)
        
        if not stats:
            await msg.edit_text("📭 Danh mục trống!")
            return
        
        stats_msg = (
            f"📊 *THỐNG KÊ DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
            f"*TỔNG QUAN*\n"
            f"• Vốn: `{fmt_price(stats['total_invest'])}`\n"
            f"• Giá trị: `{fmt_price(stats['total_value'])}`\n"
            f"• Lợi nhuận: `{fmt_price(stats['total_profit'])}`\n"
            f"• Tỷ suất: `{stats['total_profit_percent']:+.2f}%`\n\n"
            f"*📈 TOP COIN LỜI NHẤT*\n"
        )
        
        count = 0
        for symbol, profit, profit_pct, value, cost in stats['coin_profits']:
            if profit > 0:
                count += 1
                stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
            if count >= 3:
                break
        
        if count == 0:
            stats_msg += "Không có coin lời\n"
        
        stats_msg += f"\n*📉 TOP COIN LỖ NHẤT*\n"
        count = 0
        for symbol, profit, profit_pct, value, cost in reversed(stats['coin_profits']):
            if profit < 0:
                count += 1
                stats_msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
            if count >= 3:
                break
        
        if count == 0:
            stats_msg += "Không có coin lỗ\n"
        
        stats_msg += f"\n🕐 {format_vn_time()}"
        
        await msg.edit_text(stats_msg, parse_mode=ParseMode.MARKDOWN)

    # ==================== PERMISSION COMMAND ====================
    async def perm_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền quản lý phân quyền!")
            return
        
        if not ctx.args:
            msg = (
                "🔐 *QUẢN LÝ PHÂN QUYỀN*\n━━━━━━━━━━━━━━━━\n\n"
                "*Các lệnh:*\n"
                "• `/perm list` - Xem danh sách admin\n"
                "• `/perm grant @user view` - Cấp quyền xem\n"
                "• `/perm grant @user edit` - Cấp quyền sửa\n"
                "• `/perm grant @user delete` - Cấp quyền xóa\n"
                "• `/perm grant @user manage` - Cấp quyền quản lý\n"
                "• `/perm grant @user full` - Cấp toàn quyền\n"
                "• `/perm revoke @user` - Thu hồi quyền\n\n"
                f"🕐 {format_vn_time_short()}"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        if ctx.args[0] == "list":
            admins = get_all_admins(chat_id)
            if not admins:
                await update.message.reply_text("📭 Chưa có admin nào được cấp quyền!")
                return
            
            msg = "👑 *DANH SÁCH ADMIN*\n━━━━━━━━━━━━━━━━\n\n"
            for admin in admins:
                admin_id, view, edit, delete, manage = admin
                permissions = []
                if view: permissions.append("👁 Xem")
                if edit: permissions.append("✏️ Sửa")
                if delete: permissions.append("🗑 Xóa")
                if manage: permissions.append("🔐 Quản lý")
                
                msg += f"• `{admin_id}`: {', '.join(permissions)}\n"
            
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        elif ctx.args[0] == "grant" and len(ctx.args) >= 3:
            target = ctx.args[1]
            perm_type = ctx.args[2].lower()
            
            if target.startswith('@'):
                username = target[1:]
                target_id = get_user_id_by_username(username)
                if not target_id:
                    await update.message.reply_text(f"❌ Không tìm thấy user {target}")
                    return
            else:
                try:
                    target_id = int(target)
                except:
                    await update.message.reply_text("❌ ID không hợp lệ!")
                    return
            
            permissions = {'view': 0, 'edit': 0, 'delete': 0, 'manage': 0}
            
            if perm_type == 'view':
                permissions['view'] = 1
            elif perm_type == 'edit':
                permissions['view'] = 1
                permissions['edit'] = 1
            elif perm_type == 'delete':
                permissions['view'] = 1
                permissions['delete'] = 1
            elif perm_type == 'manage':
                permissions['manage'] = 1
            elif perm_type == 'full':
                permissions['view'] = 1
                permissions['edit'] = 1
                permissions['delete'] = 1
                permissions['manage'] = 1
            else:
                await update.message.reply_text("❌ Loại quyền không hợp lệ!")
                return
            
            if grant_permission(chat_id, target_id, user_id, permissions):
                await update.message.reply_text(f"✅ Đã cấp quyền {perm_type} cho {target}")
            else:
                await update.message.reply_text("❌ Lỗi khi cấp quyền!")
        
        elif ctx.args[0] == "revoke" and len(ctx.args) >= 2:
            target = ctx.args[1]
            
            if target.startswith('@'):
                username = target[1:]
                target_id = get_user_id_by_username(username)
                if not target_id:
                    await update.message.reply_text(f"❌ Không tìm thấy user {target}")
                    return
            else:
                try:
                    target_id = int(target)
                except:
                    await update.message.reply_text("❌ ID không hợp lệ!")
                    return
            
            if revoke_permission(chat_id, target_id):
                await update.message.reply_text(f"✅ Đã thu hồi quyền của {target}")
            else:
                await update.message.reply_text("❌ Không tìm thấy quyền!")

    # ==================== EXPENSE SHORTCUT HANDLERS ====================
    async def expense_shortcut_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        user_id = update.effective_user.id
        
        if text.startswith('tn '):
            parts = text.split()
            if len(parts) < 2:
                await update.message.reply_text("❌ Thiếu số tiền! VD: `tn 500000`", parse_mode=ParseMode.MARKDOWN)
                return
            
            try:
                amount = float(parts[1].replace(',', ''))
                if amount <= 0:
                    await update.message.reply_text("❌ Số tiền phải lớn hơn 0!")
                    return
                
                currency = 'VND'
                source = "Khác"
                note = ""
                
                if len(parts) >= 3:
                    if parts[2].upper() in SUPPORTED_CURRENCIES:
                        currency = parts[2].upper()
                        if len(parts) >= 4:
                            source = parts[3]
                            note = " ".join(parts[4:]) if len(parts) > 4 else ""
                    else:
                        source = parts[2]
                        note = " ".join(parts[3:]) if len(parts) > 3 else ""
                
                if add_income(user_id, amount, source, currency, note):
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: *{format_currency_simple(amount, currency)}*\n"
                        f"📌 Nguồn: *{source}*\n"
                        f"📝 Ghi chú: *{note if note else 'Không có'}*\n\n"
                        f"🕐 {format_vn_time()}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm thu nhập!")
            except ValueError:
                await update.message.reply_text("❌ Số tiền không hợp lệ!")
        
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
                    f"💰 Budget: {format_currency_simple(budget, 'VND')}\n\n"
                    f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Lỗi khi thêm danh mục!")
        
        elif text.startswith('ct '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Thiếu thông tin! VD: `ct 1 50000 VND Ăn trưa`")
                return
            
            try:
                category_id = int(parts[1])
                amount = float(parts[2].replace(',', ''))
                
                if amount <= 0:
                    await update.message.reply_text("❌ Số tiền phải lớn hơn 0!")
                    return
                
                currency = 'VND'
                start_idx = 3
                
                if len(parts) > 3 and parts[3].upper() in SUPPORTED_CURRENCIES:
                    currency = parts[3].upper()
                    start_idx = 4
                
                note = " ".join(parts[start_idx:]) if len(parts) > start_idx else ""
                
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
                        f"📝 Ghi chú: *{note if note else 'Không có'}*\n\n"
                        f"🕐 {format_vn_time()}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm chi tiêu!")
            except ValueError:
                await update.message.reply_text("❌ ID hoặc số tiền không hợp lệ!")
        
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
            
            msg += f"\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        elif text == 'bc':
            incomes_data = get_income_by_period(user_id, 'month')
            expenses_data = get_expenses_by_period(user_id, 'month')
            
            msg = f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
            
            if incomes_data['transactions']:
                msg += "*💰 THU NHẬP:*\n"
                for inc in incomes_data['transactions'][:5]:
                    id, amount, source, note, currency, date = inc
                    msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                    if note:
                        msg += f"  📝 {note}\n"
                
                msg += f"\n📊 *Tổng thu theo loại tiền:*\n"
                for currency, total in incomes_data['summary'].items():
                    msg += f"  {format_currency_simple(total, currency)}\n"
                msg += f"  *Tổng số:* {incomes_data['total_count']} giao dịch\n\n"
            else:
                msg += "📭 Chưa có thu nhập trong tháng này.\n\n"
            
            if expenses_data['transactions']:
                msg += "*💸 CHI TIÊU:*\n"
                for exp in expenses_data['transactions'][:5]:
                    id, cat_name, amount, note, currency, date, budget = exp
                    msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                    if note:
                        msg += f"  📝 {note}\n"
                
                msg += f"\n📊 *Tổng chi theo loại tiền:*\n"
                for currency, total in expenses_data['summary'].items():
                    msg += f"  {format_currency_simple(total, currency)}\n"
                
                msg += f"\n📋 *Chi tiêu theo danh mục:*\n"
                for key, data in expenses_data['category_summary'].items():
                    budget_status = ""
                    if data['budget'] > 0:
                        percent = (data['total'] / data['budget']) * 100
                        if percent > 100:
                            budget_status = " ⚠️ Vượt budget!"
                        elif percent > 80:
                            budget_status = " ⚠️ Gần hết budget"
                        msg += f"  • {data['category']} ({data['currency']}): {format_currency_simple(data['total'], data['currency'])} ({data['count']} lần) - Budget: {format_currency_simple(data['budget'], 'VND')}{budget_status}\n"
                    else:
                        msg += f"  • {data['category']} ({data['currency']}): {format_currency_simple(data['total'], data['currency'])} ({data['count']} lần)\n"
                
                msg += f"\n  *Tổng số:* {expenses_data['total_count']} giao dịch\n"
            else:
                msg += "📭 Không có chi tiêu trong tháng này."
            
            msg += f"\n\n*⚖️ CÂN ĐỐI THEO LOẠI TIỀN:*\n"
            all_currencies = set(list(incomes_data['summary'].keys()) + list(expenses_data['summary'].keys()))
            
            for currency in all_currencies:
                income = incomes_data['summary'].get(currency, 0)
                expense = expenses_data['summary'].get(currency, 0)
                balance = income - expense
                if balance > 0:
                    emoji = "✅"
                elif balance < 0:
                    emoji = "❌"
                else:
                    emoji = "➖"
                
                msg += f"  {emoji} {currency}: Thu {format_currency_simple(income, currency)} - Chi {format_currency_simple(expense, currency)} = {format_currency_simple(balance, currency)}\n"
            
            msg += f"\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        elif text.startswith('xoa chi '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa chi 5`")
                return
            
            try:
                expense_id = int(parts[2])
                if delete_expense(expense_id, user_id):
                    await update.message.reply_text(f"✅ Đã xóa khoản chi #{expense_id}\n\n🕐 {format_vn_time_short()}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản chi #{expense_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        elif text.startswith('xoa thu '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa thu 3`")
                return
            
            try:
                income_id = int(parts[2])
                if delete_income(income_id, user_id):
                    await update.message.reply_text(f"✅ Đã xóa khoản thu #{income_id}\n\n🕐 {format_vn_time_short()}")
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản thu #{income_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")

    # ==================== HANDLE MESSAGE ====================
    async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user:
            update_user_info(update.effective_user)
        
        logger.info(f"Nhận tin nhắn từ user {update.effective_user.id} trong chat {update.effective_chat.type}: {update.message.text}")
        
        text = update.message.text.strip()
        chat_type = update.effective_chat.type
        
        # Tính toán đơn giản
        if re.search(r'[\+\-\*\/]', text) and re.match(r'^[\d\s\+\-\*\/\.\(\)]+$', text):
            try:
                result = eval(text, {"__builtins__": {}}, {})
                if isinstance(result, float):
                    if result.is_integer():
                        result = int(result)
                    else:
                        result = round(result, 6)
                await update.message.reply_text(f"`{result}`", parse_mode=ParseMode.MARKDOWN)
                return
            except:
                return
        
        if chat_type == 'private' and text.startswith(('tn ', 'dm ', 'ct ', 'ds', 'bc', 'xoa chi ', 'xoa thu ')):
            await expense_shortcut_handler(update, ctx)
            return
        
        if text == "💰 ĐẦU TƯ COIN":
            await update.message.reply_text(
                f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_invest_menu_keyboard(update.effective_user.id, update.effective_chat.id)
            )
        elif text == "💸 QUẢN LÝ CHI TIÊU":
            await update.message.reply_text(
                f"💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_expense_menu_keyboard()
            )
        elif text == "❓ HƯỚNG DẪN":
            await help_command(update, ctx)

    # ==================== CALLBACK HANDLER ====================
    async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        logger.info(f"Callback: {query.data}")
        
        data = query.data
        
        try:
            if data == "back_to_main":
                await query.edit_message_text(
                    f"💰 *MENU CHÍNH*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=None
                )
                await query.message.reply_text("👇 Chọn chức năng:", reply_markup=get_main_keyboard())
            
            elif data == "back_to_invest":
                uid = query.from_user.id
                gid = query.message.chat.id
                await query.edit_message_text(
                    f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_invest_menu_keyboard(uid, gid)
                )
            
            elif data == "refresh_usdt":
                rate_data = get_usdt_vnd_rate()
                text = (
                    "💱 *TỶ GIÁ USDT/VND*\n━━━━━━━━━━━━━━━━\n\n"
                    f"🇺🇸 *1 USDT* = `{fmt_vnd(rate_data['vnd'])}`\n"
                    f"🇻🇳 *1,000,000 VND* = `{1000000/rate_data['vnd']:.4f} USDT`\n\n"
                    f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
                    f"📊 *Nguồn:* `{rate_data['source']}`\n\n"
                    f"🕐 {format_vn_time()}"
                )
                keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                            [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data.startswith("price_"):
                symbol = data.replace("price_", "")
                d = get_price(symbol)
                
                if d:
                    if symbol == 'USDT':
                        rate_data = get_usdt_vnd_rate()
                        msg = f"*{d['n']}* #{d['r']}\n💰 USD: `{fmt_price(d['p'])}`\n🇻🇳 VND: `{fmt_vnd(rate_data['vnd'])}`\n📦 Volume: `{fmt_vol(d['v'])}`\n💎 Market Cap: `{fmt_vol(d['m'])}`\n📈 24h: {fmt_percent(d['c'])}"
                    else:
                        msg = f"*{d['n']}* #{d['r']}\n💰 Giá: `{fmt_price(d['p'])}`\n📦 Volume: `{fmt_vol(d['v'])}`\n💎 Market Cap: `{fmt_vol(d['m'])}`\n📈 24h: {fmt_percent(d['c'])}"
                    msg += f"\n\n🕐 {format_vn_time_short()}"
                else:
                    msg = f"❌ *{symbol}*: Không có dữ liệu\n\n🕐 {format_vn_time_short()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "show_portfolio":
                uid = query.from_user.id
                portfolio_data = get_portfolio(uid)
                
                if not portfolio_data:
                    await query.edit_message_text(f"📭 Danh mục trống!\n\n🕐 {format_vn_time()}")
                    return
                
                # Lấy giá batch cho tất cả symbol
                symbols = list(set([row[0] for row in portfolio_data]))
                prices = get_prices_batch(symbols)
                
                summary = {}
                total_invest = 0
                total_value = 0
                
                for row in portfolio_data:
                    symbol, amount, price, date, cost = row
                    if symbol not in summary:
                        summary[symbol] = {'amount': 0, 'cost': 0}
                    summary[symbol]['amount'] += amount
                    summary[symbol]['cost'] += cost
                    total_invest += cost
                
                msg = "📊 *DANH MỤC*\n━━━━━━━━━━━━\n\n"
                for symbol, data in summary.items():
                    price_data = prices.get(symbol)
                    if price_data:
                        current = data['amount'] * price_data['p']
                        profit = current - data['cost']
                        profit_percent = (profit / data['cost']) * 100 if data['cost'] > 0 else 0
                        total_value += current
                        
                        msg += f"*{symbol}*\n"
                        msg += f"📊 SL: `{data['amount']:.4f}`\n"
                        msg += f"💰 TB: `{fmt_price(data['cost']/data['amount'])}`\n"
                        msg += f"💎 TT: `{fmt_price(current)}`\n"
                        msg += f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n"
                
                total_profit = total_value - total_invest
                total_profit_percent = (total_profit / total_invest) * 100 if total_invest > 0 else 0
                
                msg += "━━━━━━━━━━━━\n"
                msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
                msg += f"💰 GT: `{fmt_price(total_value)}`\n"
                msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)\n\n"
                msg += f"🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "show_profit":
                uid = query.from_user.id
                transactions = get_transaction_detail(uid)
                
                if not transactions:
                    await query.edit_message_text(f"📭 Danh mục trống!\n\n🕐 {format_vn_time()}")
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
                msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)\n\n"
                msg += f"🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "show_stats":
                uid = query.from_user.id
                await query.edit_message_text("🔄 Đang tính toán thống kê...")
                
                stats = get_portfolio_stats(uid)
                
                if not stats:
                    await query.edit_message_text("📭 Danh mục trống!")
                    return
                
                msg = (
                    f"📊 *THỐNG KÊ DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                    f"*TỔNG QUAN*\n"
                    f"• Vốn: `{fmt_price(stats['total_invest'])}`\n"
                    f"• Giá trị: `{fmt_price(stats['total_value'])}`\n"
                    f"• Lợi nhuận: `{fmt_price(stats['total_profit'])}`\n"
                    f"• Tỷ suất: `{stats['total_profit_percent']:+.2f}%`\n\n"
                    f"*📈 TOP COIN LỜI NHẤT*\n"
                )
                
                count = 0
                for symbol, profit, profit_pct, value, cost in stats['coin_profits']:
                    if profit > 0:
                        count += 1
                        msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
                    if count >= 3:
                        break
                
                if count == 0:
                    msg += "Không có coin lời\n"
                
                msg += f"\n*📉 TOP COIN LỖ NHẤT*\n"
                count = 0
                for symbol, profit, profit_pct, value, cost in reversed(stats['coin_profits']):
                    if profit < 0:
                        count += 1
                        msg += f"{count}. *{symbol}*: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n"
                    if count >= 3:
                        break
                
                if count == 0:
                    msg += "Không có coin lỗ\n"
                
                msg += f"\n🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "show_alerts":
                uid = query.from_user.id
                alerts = get_user_alerts(uid)
                
                if not alerts:
                    await query.edit_message_text(f"📭 Bạn chưa có cảnh báo nào!\n\n🕐 {format_vn_time()}")
                    return
                
                msg = "🔔 *CẢNH BÁO GIÁ*\n━━━━━━━━━━━━━━━━\n\n"
                for alert in alerts:
                    alert_id, symbol, target, condition, created = alert
                    created_date = created.split()[0]
                    price_data = get_price(symbol)
                    current_price = price_data['p'] if price_data else 0
                    status = "🟢" if (condition == 'above' and current_price < target) or (condition == 'below' and current_price > target) else "🔴"
                    msg += f"{status} *#{alert_id}*: {symbol} {condition} `{fmt_price(target)}`\n"
                    msg += f"   Giá hiện: `{fmt_price(current_price)}` (tạo {created_date})\n\n"
                
                msg += f"🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

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
                    f"*Nhập lệnh:*\n`/edit {tx_id} [sl] [giá]`\n\n"
                    f"🕐 {format_vn_time()}"
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
                
                msg = f"⚠️ *Xác nhận xóa giao dịch #{tx_id}?*\n\n🕐 {format_vn_time_short()}"
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
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''DELETE FROM portfolio WHERE id = ? AND user_id = ?''', (tx_id, uid))
                conn.commit()
                affected = c.rowcount
                conn.close()
                
                if affected > 0:
                    msg = f"✅ Đã xóa giao dịch #{tx_id}\n\n🕐 {format_vn_time()}"
                else:
                    msg = f"❌ Không thể xóa giao dịch #{tx_id}\n\n🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về danh mục", callback_data="show_portfolio")]]
                
                await query.edit_message_text(
                    msg, parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif data == "edit_transactions":
                uid = query.from_user.id
                transactions = get_transaction_detail(uid)
                
                if not transactions:
                    await query.edit_message_text(
                        f"📭 Không có giao dịch!\n\n🕐 {format_vn_time()}",
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
                
                msg += f"\n🕐 {format_vn_time_short()}"
                
                await query.edit_message_text(
                    msg, parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            elif data == "show_top10":
                await query.edit_message_text("🔄 Đang tải...")
                
                try:
                    headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
                    res = requests.get(f"{CMC_API_URL}/cryptocurrency/listings/latest", headers=headers, params={'limit': 10, 'convert': 'USD'}, timeout=10)
                    
                    if res.status_code == 200:
                        data = res.json()['data']
                        msg = "📊 *TOP 10 COIN*\n━━━━━━━━━━━━\n\n"
                        
                        for i, coin in enumerate(data, 1):
                            quote = coin['quote']['USD']
                            change = quote['percent_change_24h']
                            emoji = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                            
                            msg += f"{i}. *{coin['symbol']}* - {coin['name']}\n"
                            msg += f"   💰 `{fmt_price(quote['price'])}` {emoji} `{change:+.2f}%`\n"
                        
                        msg += f"\n🕐 {format_vn_time_short()}"
                    else:
                        msg = "❌ Không thể lấy dữ liệu"
                except Exception as e:
                    msg = "❌ Lỗi kết nối"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "show_buy":
                await query.edit_message_text(
                    "➕ *MUA COIN*\n\n"
                    "Dùng lệnh: `/buy [coin] [sl] [giá]`\n\n"
                    "*Ví dụ:*\n"
                    "• `/buy btc 0.5 40000`\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                )
            
            elif data == "show_sell":
                await query.edit_message_text(
                    "➖ *BÁN COIN*\n\n"
                    "Dùng lệnh: `/sell [coin] [sl]`\n\n"
                    "*Ví dụ:*\n"
                    "• `/sell btc 0.2`\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                )
            
            elif data == "export_csv":
                uid = query.from_user.id
                await query.edit_message_text("🔄 Đang tạo file CSV...")
                
                transactions = get_transaction_detail(uid)
                if not transactions:
                    await query.edit_message_text("📭 Không có dữ liệu để xuất!")
                    return
                
                timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                filename = f"portfolio_{uid}_{timestamp}.csv"
                filepath = os.path.join(EXPORT_DIR, filename)
                
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 'Ngày mua', 'Tổng vốn (USD)'])
                    for tx in transactions:
                        writer.writerow([tx[0], tx[1], tx[2], tx[3], tx[4], tx[5]])
                
                try:
                    with open(filepath, 'rb') as f:
                        await query.message.reply_document(
                            document=f,
                            filename=filename,
                            caption=f"📊 *BÁO CÁO DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!\n🕐 {format_vn_time()}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    os.remove(filepath)
                    
                    await query.edit_message_text(
                        f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=get_invest_menu_keyboard(uid, query.message.chat.id)
                    )
                except Exception as e:
                    logger.error(f"Lỗi export: {e}")
                    await query.edit_message_text("❌ Lỗi khi gửi file!")
            
            elif data == "admin_panel":
                uid = query.from_user.id
                group_id = query.message.chat.id
                
                msg = (
                    "👑 *ADMIN PANEL*\n━━━━━━━━━━━━━━━━\n\n"
                    "• `/perm list` - Danh sách admin\n"
                    "• `/perm grant @user view` - Cấp quyền xem\n"
                    "• `/perm grant @user edit` - Cấp quyền sửa\n"
                    "• `/perm grant @user delete` - Cấp quyền xóa\n"
                    "• `/perm grant @user manage` - Cấp quyền QL\n"
                    "• `/perm revoke @user` - Thu hồi quyền\n\n"
                    f"🕐 {format_vn_time()}"
                )
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
            elif data == "back_to_expense":
                await query.edit_message_text(
                    f"💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_expense_menu_keyboard()
                )
            
            elif data == "expense_income_menu":
                await query.edit_message_text(
                    "💰 *MENU THU NHẬP*\n\n"
                    "• `tn [số tiền]` - Thêm thu nhập\n"
                    "• `tn 100 USD Lương` - Thêm 100 USD\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                )
            
            elif data == "expense_expense_menu":
                await query.edit_message_text(
                    "💸 *MENU CHI TIÊU*\n\n"
                    "• `ct [mã] [số tiền]` - Thêm chi tiêu\n"
                    "• `ct 1 50000 VND Ăn trưa` - Ví dụ\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                )
            
            elif data == "expense_categories":
                uid = query.from_user.id
                categories = get_expense_categories(uid)
                
                if not categories:
                    await query.edit_message_text(
                        f"📋 Chưa có danh mục nào!\nTạo: `dm [tên] [budget]`\n\n🕐 {format_vn_time_short()}",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
                    return
                
                msg = "📋 *DANH MỤC CỦA BẠN*\n━━━━━━━━━━━━━━━━\n\n"
                for cat in categories:
                    cat_id, name, budget, created = cat
                    msg += f"• *{cat_id}.* {name} - {format_currency_simple(budget, 'VND')}\n"
                msg += f"\n🕐 {format_vn_time_short()}"
                
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]))
            
            elif data == "expense_report_menu":
                uid = query.from_user.id
                expenses = get_expenses_by_period(uid, 'month')
                incomes = get_income_by_period(uid, 'month')
                
                msg = f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
                
                if incomes['transactions']:
                    total_income = 0
                    msg += "*💰 THU NHẬP:*\n"
                    for inc in incomes['transactions'][:5]:
                        id, amount, source, note, currency, date = inc
                        msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                    msg += f"\n"
                else:
                    msg += "📭 Chưa có thu nhập.\n\n"
                
                if expenses['transactions']:
                    total_expense = 0
                    msg += "*💸 CHI TIÊU:*\n"
                    for exp in expenses['transactions'][:5]:
                        id, cat_name, amount, note, currency, date, budget = exp
                        msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                    msg += f"\n"
                else:
                    msg += "📭 Chưa có chi tiêu."
                
                msg += f"\n🕐 {format_vn_time()}"
                
                await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]))
            
            elif data == "expense_today":
                uid = query.from_user.id
                try:
                    incomes_data = get_income_by_period(uid, 'day')
                    expenses_data = get_expenses_by_period(uid, 'day')
                    
                    msg = f"📅 *HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    if incomes_data['transactions']:
                        msg += "*💰 THU NHẬP:*\n"
                        for inc in incomes_data['transactions']:
                            id, amount, source, note, currency, date = inc
                            msg += f"• #{id}: {format_currency_simple(amount, currency)} - {source}\n"
                            if note:
                                msg += f"  📝 {note}\n"
                        
                        msg += f"\n📊 *Tổng thu:*\n"
                        for currency, total in incomes_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                        msg += "\n"
                    else:
                        msg += "📭 Không có thu nhập hôm nay.\n\n"
                    
                    if expenses_data['transactions']:
                        msg += "*💸 CHI TIÊU:*\n"
                        for exp in expenses_data['transactions']:
                            id, cat_name, amount, note, currency, date, budget = exp
                            msg += f"• #{id}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                            if note:
                                msg += f"  📝 {note}\n"
                        
                        msg += f"\n📊 *Tổng chi:*\n"
                        for currency, total in expenses_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                    else:
                        msg += "📭 Không có chi tiêu hôm nay."
                    
                    msg += f"\n\n🕐 {format_vn_time()}"
                    
                    await query.edit_message_text(
                        msg, 
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
                except Exception as e:
                    logger.error(f"Lỗi expense_today: {e}")
                    await query.edit_message_text(
                        "❌ Có lỗi xảy ra khi xem hôm nay!",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
            
            elif data == "expense_month":
                uid = query.from_user.id
                try:
                    incomes_data = get_income_by_period(uid, 'month')
                    expenses_data = get_expenses_by_period(uid, 'month')
                    
                    msg = f"📅 *THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    if incomes_data['transactions']:
                        msg += "*💰 THU NHẬP:*\n"
                        for inc in incomes_data['transactions'][:10]:
                            id, amount, source, note, currency, date = inc
                            msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                            if note:
                                msg += f"  📝 {note}\n"
                        
                        msg += f"\n📊 *Tổng thu theo loại tiền:*\n"
                        for currency, total in incomes_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                        msg += f"  *Tổng số:* {incomes_data['total_count']} giao dịch\n\n"
                    else:
                        msg += "📭 Không có thu nhập trong tháng này.\n\n"
                    
                    if expenses_data['transactions']:
                        msg += "*💸 CHI TIÊU:*\n"
                        for exp in expenses_data['transactions'][:10]:
                            id, cat_name, amount, note, currency, date, budget = exp
                            msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                            if note:
                                msg += f"  📝 {note}\n"
                        
                        msg += f"\n📊 *Tổng chi theo loại tiền:*\n"
                        for currency, total in expenses_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                        
                        msg += f"\n📋 *Chi tiêu theo danh mục:*\n"
                        for key, data in expenses_data['category_summary'].items():
                            budget_status = ""
                            if data['budget'] > 0:
                                percent = (data['total'] / data['budget']) * 100
                                if percent > 100:
                                    budget_status = " ⚠️ Vượt budget!"
                                elif percent > 80:
                                    budget_status = " ⚠️ Gần hết budget"
                                msg += f"  • {data['category']} ({data['currency']}): {format_currency_simple(data['total'], data['currency'])} ({data['count']} lần) - Budget: {format_currency_simple(data['budget'], 'VND')}{budget_status}\n"
                            else:
                                msg += f"  • {data['category']} ({data['currency']}): {format_currency_simple(data['total'], data['currency'])} ({data['count']} lần)\n"
                        
                        msg += f"\n  *Tổng số:* {expenses_data['total_count']} giao dịch\n"
                    else:
                        msg += "📭 Không có chi tiêu trong tháng này."
                    
                    msg += f"\n\n*⚖️ CÂN ĐỐI THU CHI:*\n"
                    all_currencies = set(list(incomes_data['summary'].keys()) + list(expenses_data['summary'].keys()))
                    
                    for currency in all_currencies:
                        income = incomes_data['summary'].get(currency, 0)
                        expense = expenses_data['summary'].get(currency, 0)
                        balance = income - expense
                        if balance > 0:
                            emoji = "✅"
                        elif balance < 0:
                            emoji = "❌"
                        else:
                            emoji = "➖"
                        
                        msg += f"  {emoji} {currency}: {format_currency_simple(balance, currency)}\n"
                    
                    msg += f"\n🕐 {format_vn_time()}"
                    
                    await query.edit_message_text(
                        msg, 
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
                except Exception as e:
                    logger.error(f"Lỗi expense_month: {e}")
                    await query.edit_message_text(
                        "❌ Có lỗi xảy ra khi xem tháng này!",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
            
            elif data == "expense_recent":
                uid = query.from_user.id
                try:
                    recent_incomes = get_recent_incomes(uid, 10)
                    recent_expenses = get_recent_expenses(uid, 10)
                    
                    if not recent_incomes and not recent_expenses:
                        await query.edit_message_text(
                            f"📭 Không có giao dịch nào!\n\n🕐 {format_vn_time_short()}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                        )
                        return
                    
                    msg = "🔄 *20 GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    all_transactions = []
                    
                    for inc in recent_incomes:
                        id, amount, source, note, date, currency = inc
                        all_transactions.append(('💰', id, date, f"{format_currency_simple(amount, currency)} - {source}", note))
                    
                    for exp in recent_expenses:
                        id, cat_name, amount, note, date, currency = exp
                        all_transactions.append(('💸', id, date, f"{format_currency_simple(amount, currency)} - {cat_name}", note))
                    
                    all_transactions.sort(key=lambda x: x[2], reverse=True)
                    
                    for emoji, id, date, desc, note in all_transactions[:20]:
                        msg += f"{emoji} #{id} {date}: {desc}\n"
                        if note:
                            msg += f"   📝 {note}\n"
                    
                    msg += f"\n🕐 {format_vn_time_short()}"
                    
                    await query.edit_message_text(
                        msg, 
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
                except Exception as e:
                    logger.error(f"Lỗi expense_recent: {e}")
                    await query.edit_message_text(
                        "❌ Có lỗi xảy ra!",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
            
            elif data == "expense_export":
                uid = query.from_user.id
                await query.edit_message_text("🔄 Đang tạo file báo cáo...")
                
                expenses = get_recent_expenses(uid, 100)
                incomes = get_recent_incomes(uid, 100)
                
                if not expenses and not incomes:
                    await query.edit_message_text("📭 Không có dữ liệu để xuất!")
                    return
                
                timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                filename = f"expense_report_{uid}_{timestamp}.csv"
                filepath = os.path.join(EXPORT_DIR, filename)
                
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    writer.writerow(['=== THU NHẬP ==='])
                    writer.writerow(['ID', 'Ngày', 'Nguồn', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                    for inc in incomes:
                        writer.writerow([inc[0], inc[4], inc[2], inc[1], inc[5], inc[3]])
                    
                    writer.writerow([])
                    writer.writerow(['=== CHI TIÊU ==='])
                    writer.writerow(['ID', 'Ngày', 'Danh mục', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                    for exp in expenses:
                        writer.writerow([exp[0], exp[4], exp[1], exp[2], exp[5], exp[3]])
                
                try:
                    with open(filepath, 'rb') as f:
                        await query.message.reply_document(
                            document=f,
                            filename=filename,
                            caption=f"📊 *BÁO CÁO CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!\n🕐 {format_vn_time()}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    os.remove(filepath)
                    await query.edit_message_text(
                        "💰 *QUẢN LÝ CHI TIÊU*",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=get_expense_menu_keyboard()
                    )
                except Exception as e:
                    await query.edit_message_text("❌ Lỗi khi gửi file!")
            
            else:
                await query.edit_message_text("❌ Không hiểu lệnh!")
        
        except Exception as e:
            logger.error(f"Lỗi callback: {e}")
            await query.edit_message_text("❌ Có lỗi xảy ra!")

    # ==================== PORTFOLIO STATS HELPER ====================
    def get_portfolio_stats(user_id):
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

    # ==================== WEBHOOK SETUP ====================
    async def setup_webhook():
        """Cấu hình webhook cho Render"""
        try:
            if not render_config.render_url:
                logger.warning("⚠️ Không có RENDER_EXTERNAL_URL, dùng polling")
                return False
            
            webhook_url = f"{render_config.render_url}/webhook"
            
            # Xóa webhook cũ
            await app.bot.delete_webhook(drop_pending_updates=True)
            
            # Set webhook mới
            await app.bot.set_webhook(
                url=webhook_url,
                allowed_updates=['message', 'callback_query'],
                drop_pending_updates=True,
                max_connections=render_config.get_worker_count()
            )
            
            webhook_info = await app.bot.get_webhook_info()
            logger.info(f"✅ Webhook set: {webhook_url}")
            logger.info(f"📊 Pending updates: {webhook_info.pending_update_count}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi setup webhook: {e}")
            return False

    # ==================== WEBHOOK HANDLER ====================
    @webhook_app.route('/webhook', methods=['POST'])
    def webhook():
        """Nhận updates từ Telegram"""
        try:
            update = Update.de_json(request.get_json(force=True), app.bot)
            asyncio.run_coroutine_threadsafe(
                app.process_update(update),
                app.loop
            )
            return 'OK', 200
        except Exception as e:
            logger.error(f"❌ Webhook error: {e}")
            return 'Error', 500

    @webhook_app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # Kiểm tra database
            db_exists = os.path.exists(DB_PATH)
            db_size = os.path.getsize(DB_PATH) / 1024 if db_exists else 0
            
            status = {
                'status': 'healthy',
                'time': format_vn_time(),
                'uptime': time.time() - render_config.start_time,
                'memory_mb': round(memory_mb, 2),
                'db_size_kb': round(db_size, 2),
                'cache_stats': {
                    'price': price_cache.get_stats(),
                    'usdt': usdt_cache.get_stats()
                }
            }
            return json.dumps(status), 200, {'Content-Type': 'application/json'}
        except Exception as e:
            return json.dumps({'status': 'error', 'message': str(e)}), 500

    @webhook_app.route('/', methods=['GET'])
    def home():
        """Home page"""
        return f"""
        <html>
            <head><title>Crypto Bot</title></head>
            <body>
                <h1>🚀 Crypto & Expense Manager Bot</h1>
                <p>Status: <span style="color: green;">Running</span></p>
                <p>Time: {format_vn_time()}</p>
                <p>Uptime: {time.time() - render_config.start_time:.0f} seconds</p>
                <p><a href="/health">Health Check</a></p>
            </body>
        </html>
        """

    def run_webhook_server():
        """Chạy Flask server cho webhook"""
        port = int(os.environ.get('PORT', 10000))
        logger.info(f"🌐 Starting webhook server on port {port}")
        webhook_app.run(host='0.0.0.0', port=port, threaded=True)

    # ==================== ENHANCED HEALTH CHECK (HTTP Server) ====================
    class EnhancedHealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                try:
                    import psutil
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    cpu_percent = process.cpu_percent()
                    
                    db_size = os.path.getsize(DB_PATH) / 1024 if os.path.exists(DB_PATH) else 0
                    
                    status = {
                        'status': 'healthy',
                        'time': format_vn_time(),
                        'memory_mb': round(memory_mb, 2),
                        'cpu_percent': cpu_percent,
                        'db_size_kb': round(db_size, 2),
                        'cache_stats': {
                            'price': price_cache.get_stats(),
                            'usdt': usdt_cache.get_stats()
                        },
                        'uptime': time.time() - render_config.start_time
                    }
                    
                    self.wfile.write(json.dumps(status, indent=2).encode())
                except:
                    self.wfile.write(b'{"status": "healthy"}')
            
            elif self.path == '/metrics':
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                
                try:
                    import psutil
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    cpu_percent = process.cpu_percent()
                    db_size = os.path.getsize(DB_PATH) / 1024 if os.path.exists(DB_PATH) else 0
                    
                    metrics = f"""# HELP bot_memory Memory usage in MB
# TYPE bot_memory gauge
bot_memory {memory_mb}

# HELP bot_cpu CPU usage percent
# TYPE bot_cpu gauge
bot_cpu {cpu_percent}

# HELP bot_db_size Database size in KB
# TYPE bot_db_size gauge
bot_db_size {db_size}

# HELP bot_uptime Uptime in seconds
# TYPE bot_uptime counter
bot_uptime {time.time() - render_config.start_time}

# HELP bot_cache_hits Cache hit rate
# TYPE bot_cache_hits gauge
bot_cache_hits_price {price_cache.get_stats()['hit_rate']}
bot_cache_hits_usdt {usdt_cache.get_stats()['hit_rate']}
"""
                    self.wfile.write(metrics.encode())
                except:
                    self.wfile.write(b'# No metrics available')
            
            else:
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                html = f"""
                <html>
                    <head><title>Crypto Bot</title></head>
                    <body>
                        <h1>🚀 Crypto & Expense Manager Bot</h1>
                        <p>Status: <span style="color: green;">Running</span></p>
                        <p>Time: {format_vn_time()}</p>
                        <p>Uptime: {time.time() - render_config.start_time:.0f} seconds</p>
                        <p>
                            <a href="/health">Health Check (JSON)</a> | 
                            <a href="/metrics">Metrics (Prometheus)</a>
                        </p>
                    </body>
                </html>
                """
                self.wfile.write(html.encode())
        
        def log_message(self, format, *args):
            return

    def run_health_server():
        """Chạy HTTP server cho health check (fallback)"""
        try:
            port = int(os.environ.get('PORT', 10000))
            server = HTTPServer(('0.0.0.0', port), EnhancedHealthHandler)
            logger.info(f"✅ Health server on port {port}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"❌ Health server error: {e}")
            time.sleep(10)

    # ==================== SMART STARTUP ====================
    def smart_startup():
        """Khởi động thông minh tùy theo môi trường"""
        logger.info("🚀 SMART STARTUP")
        logger.info(f"📊 Render mode: {render_config.is_render}")
        logger.info(f"💾 Memory limit: {render_config.memory_limit}MB")
        logger.info(f"⚙️ CPU limit: {render_config.cpu_limit}")
        logger.info(f"🌐 Render URL: {render_config.render_url}")
        
        # Khởi tạo database
        if not init_database():
            logger.error("❌ KHÔNG THỂ KHỞI TẠO DATABASE")
            time.sleep(5)
        
        # Migrate database
        try:
            migrate_database()
        except Exception as e:
            logger.error(f"❌ Lỗi migrate: {e}")
        
        # Optimize database lúc khởi động
        optimize_database()
        
        # Chọn chế độ chạy
        if render_config.is_render and render_config.render_url:
            logger.info("🌐 Using webhook mode")
            # Setup webhook
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(setup_webhook())
            
            # Chạy Flask webhook server
            threading.Thread(target=run_webhook_server, daemon=True).start()
        else:
            logger.info("🔄 Using polling mode")
            # Chạy health check server
            threading.Thread(target=run_health_server, daemon=True).start()
        
        # Background threads
        threading.Thread(target=memory_monitor, daemon=True).start()
        threading.Thread(target=schedule_backup, daemon=True).start()
        threading.Thread(target=check_alerts, daemon=True).start()
        
        logger.info(f"🎉 BOT ĐÃ SẴN SÀNG! {format_vn_time()}")

    # ==================== MAIN ====================
    if __name__ == '__main__':
        try:
            logger.info("🚀 KHỞI ĐỘNG CRYPTO BOT - RENDER OPTIMIZED")
            logger.info(f"🕐 Thời gian: {format_vn_time()}")
            
            # Tạo application
            app = Application.builder().token(TELEGRAM_TOKEN).build()
            app.bot_data = {}
            logger.info("✅ Đã tạo Telegram Application")
            
            # Đăng ký handlers
            app.add_handler(CommandHandler("start", start))
            app.add_handler(CommandHandler("help", help_command))
            app.add_handler(CommandHandler("menu", menu_command))
            app.add_handler(CommandHandler("usdt", usdt_command))
            app.add_handler(CommandHandler("s", s_command))
            app.add_handler(CommandHandler("buy", buy_command))
            app.add_handler(CommandHandler("sell", sell_command))
            app.add_handler(CommandHandler("edit", edit_command))
            app.add_handler(CommandHandler("del", delete_tx_command))
            app.add_handler(CommandHandler("delete", delete_tx_command))
            app.add_handler(CommandHandler("xoa", delete_tx_command))
            app.add_handler(CommandHandler("alert", alert_command))
            app.add_handler(CommandHandler("alerts", alerts_command))
            app.add_handler(CommandHandler("stats", stats_command))
            app.add_handler(CommandHandler("perm", perm_command))
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
            app.add_handler(CallbackQueryHandler(handle_callback))
            
            logger.info("✅ Đã đăng ký handlers")
            
            # Khởi động thông minh
            smart_startup()
            
            # Chạy bot
            if render_config.is_render and render_config.render_url:
                # Webhook mode: Flask đã chạy, cần giữ main thread alive
                logger.info("⏳ Bot running in webhook mode...")
                while True:
                    time.sleep(60)
                    check_memory_usage()
            else:
                # Polling mode
                logger.info("⏳ Bot running in polling mode...")
                app.run_polling(timeout=30, drop_pending_updates=True)
            
        except Exception as e:
            logger.error(f"❌ LỖI: {e}", exc_info=True)
            time.sleep(5)
            os.execv(sys.executable, ['python'] + sys.argv)

except Exception as e:
    logger.critical(f"💥 LỖI NGHIÊM TRỌNG: {e}", exc_info=True)
    time.sleep(10)
    os.execv(sys.executable, ['python'] + sys.argv)

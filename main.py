"""
Crypto & Expense Manager Bot - Optimized for Render
Author: Assistant
Version: 2.0 - Render Optimized
"""
# ==================== PHẦN 1: IMPORTS ====================
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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.constants import ParseMode
from telegram.error import TelegramError
from functools import wraps
from flask import Flask, request
import asyncio

# ==================== HÀM ESCAPE MARKDOWN ====================
def escape_markdown(text):
    if not text:
        return ""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

# ==================== THÊM HÀM NÀY VÀO ĐÂY ====================
async def safe_edit_message(query, text, reply_markup=None, parse_mode=ParseMode.MARKDOWN):
    """Sửa message an toàn, tự động escape nếu có lỗi Markdown"""
    try:
        # Thử gửi với Markdown
        await query.edit_message_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return
    except Exception as e:
        # Log lỗi để debug
        logger.warning(f"⚠️ Lỗi Markdown trong callback: {e}")
        logger.warning(f"📝 Text gây lỗi (độ dài {len(text)}): {text[:100]}...")
        
        # KIỂM TRA NẾU TEXT CHỨA #cat_ THÌ THAY THẾ
        if "#cat_" in text:
            logger.warning("🚫 Phát hiện #cat_ trong text, thay bằng message mặc định")
            text = "❌ Có lỗi xảy ra, vui lòng thử lại sau."
        
        # Cách 1: Thử gửi không Markdown
        try:
            await query.edit_message_text(text, parse_mode=None, reply_markup=reply_markup)
            logger.info("✅ Đã sửa bằng cách gửi không Markdown")
            return
        except Exception as e2:
            logger.warning(f"⚠️ Vẫn lỗi khi gửi không Markdown: {e2}")
        
        # Cách 2: Gửi message mặc định
        try:
            await query.edit_message_text("❌ Có lỗi hiển thị, vui lòng thử lại sau.", parse_mode=None)
        except:
            pass
            
# ==================== HÀM HELPER CHO PHÂN QUYỀN ====================
async def update_perm_message(query, ctx, target_id, view, edit, delete, manage):
    """Cập nhật message phân quyền"""
    try:
        # Thử lấy thông tin user từ Telegram
        chat = await ctx.bot.get_chat(target_id)
        name = f"@{chat.username}" if chat.username else chat.first_name
    except Exception as e:
        logger.warning(f"Không thể lấy thông tin user {target_id}: {e}")
        name = f"User {target_id}"
    
    # Tạo message hiển thị quyền
    msg = (f"🔐 *PHÂN QUYỀN CHO {name}*\n"
           f"━━━━━━━━━━━━━━━━\n\n"
           f"User ID: `{target_id}`\n\n"
           f"*Quyền hiện tại:*\n"
           f"{'✅' if view else '⬜'} 👁 XEM\n"
           f"{'✅' if edit else '⬜'} ✏️ SỬA\n"
           f"{'✅' if delete else '⬜'} 🗑 XÓA\n"
           f"{'✅' if manage else '⬜'} 🔐 QUẢN LÝ\n\n"
           f"*Chọn để thay đổi:*")
    
    # Tạo keyboard với các nút
    keyboard = [
        [
            InlineKeyboardButton(f"{'✅' if view else '⬜'} 👁 Xem", callback_data=f"perm_toggle_{target_id}_view"),
            InlineKeyboardButton(f"{'✅' if edit else '⬜'} ✏️ Sửa", callback_data=f"perm_toggle_{target_id}_edit")
        ],
        [
            InlineKeyboardButton(f"{'✅' if delete else '⬜'} 🗑 Xóa", callback_data=f"perm_toggle_{target_id}_delete"),
            InlineKeyboardButton(f"{'✅' if manage else '⬜'} 🔐 Q.Lý", callback_data=f"perm_toggle_{target_id}_manage")
        ],
        [
            InlineKeyboardButton("👑 FULL QUYỀN", callback_data=f"perm_set_{target_id}_full"),
            InlineKeyboardButton("❌ XÓA HẾT", callback_data=f"perm_set_{target_id}_none")
        ],
        [InlineKeyboardButton("💾 LƯU THAY ĐỔI", callback_data=f"perm_save_{target_id}")],
        [InlineKeyboardButton("🔙 Quay lại danh sách", callback_data="settings_members")]
    ]
    
    # Gửi message đã được escape
    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
    
# ==================== AUTO DELETE MESSAGE ====================
async def auto_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, seconds: int = 30):
    """Tự động xóa tin nhắn sau seconds giây"""
    try:
        await asyncio.sleep(seconds)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Đã tự động xóa tin nhắn {message_id} sau {seconds}s")
    except Exception as e:
        logger.error(f"❌ Lỗi xóa tin nhắn tự động: {e}")
            
# ==================== OWNER CONFIGURATION ====================
OWNER_ID = 6737175223
OWNER_USERNAME = "adm"

def is_owner(user_id):
    return user_id == OWNER_ID

# ==================== GROUP OWNER MANAGEMENT ====================
GROUP_OWNERS = {}

def load_group_owners():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT group_id, owner_id FROM group_owners")
        rows = c.fetchall()
        for group_id, owner_id in rows:
            GROUP_OWNERS[group_id] = owner_id
        conn.close()
        logger.info(f"✅ Loaded {len(GROUP_OWNERS)} group owners from DB")
    except Exception as e:
        logger.error(f"❌ Lỗi load group owners: {e}")
        
def set_group_owner(group_id, owner_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
        c.execute('''INSERT OR REPLACE INTO group_owners (group_id, owner_id, created_at) VALUES (?, ?, ?)''', (group_id, owner_id, created_at))
        conn.commit()
        conn.close()
        GROUP_OWNERS[group_id] = owner_id
        logger.info(f"✅ Set owner {owner_id} for group {group_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi set group owner: {e}")
        return False

def get_group_owner(group_id):
    """Lấy owner_id của group, ưu tiên từ RAM, nếu không có thì đọc từ DB"""
    # Kiểm tra trong RAM trước
    owner_id = GROUP_OWNERS.get(group_id)
    if owner_id:
        return owner_id
    
    # Nếu không có trong RAM, đọc từ database
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT owner_id FROM group_owners WHERE group_id = ?", (group_id,))
        result = c.fetchone()
        conn.close()
        
        if result:
            owner_id = result[0]
            # Lưu lại vào RAM cho lần sau
            GROUP_OWNERS[group_id] = owner_id
            logger.info(f"✅ Loaded owner {owner_id} for group {group_id} from DB")
            return owner_id
    except Exception as e:
        logger.error(f"❌ Lỗi đọc group owner từ DB: {e}")
    
    # Fallback về OWNER_ID
    return OWNER_ID
    
def load_group_owner(group_id):
    """Load một group cụ thể vào RAM"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT owner_id FROM group_owners WHERE group_id = ?", (group_id,))
        result = c.fetchone()
        conn.close()
        
        if result:
            GROUP_OWNERS[group_id] = result[0]
            return result[0]
    except Exception as e:
        logger.error(f"❌ Lỗi load group owner {group_id}: {e}")
    
    return None
    
def is_group_owner(group_id, user_id):
    return user_id == get_group_owner(group_id)

def get_effective_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    user_id = update.effective_user.id
    
    if chat_type in ['group', 'supergroup']:
        group_id = update.effective_chat.id
        owner_id = get_group_owner(group_id)
        
        if not owner_id:
            logger.warning(f"⚠️ Group {group_id} chưa có owner")
            return None, user_id
        
        logger.info(f"🏢 Group {group_id}: user {user_id} đang thao tác trên data của owner {owner_id}")
        return owner_id, user_id
    
    logger.info(f"💬 Private: user {user_id} tự quản lý data riêng")
    return user_id, user_id
    
# ==================== ĐA NGÔN NGỮ ====================
LANGUAGE = {}  # Sẽ lưu ngôn ngữ của từng user

# Tiếng Việt
VI = {
    'welcome': "🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*",
    'price': "💰 Giá",
    'buy': "Mua",
    'sell': "Bán",
    'profit': "Lợi nhuận",
    'stats': "Thống kê",
    'settings': "Cài đặt",
    'help': "Hướng dẫn",
    'back': "Quay lại",
    'confirm': "Xác nhận",
    'cancel': "Hủy",
    'save': "Lưu",
    'delete': "Xóa",
    'edit': "Sửa",
    'view': "Xem",
    'manage': "Quản lý",
    'members': "Thành viên",
    'permissions': "Phân quyền",
    'sync': "Đồng bộ",
    'list': "Danh sách",
    'time': "Thời gian",
    'error': "Lỗi",
    'success': "Thành công",
    'warning': "Cảnh báo",
}

# Tiếng Trung (Giản thể)
ZH = {
    'welcome': "🚀 *加密货币与支出管理机器人*",
    'price': "💰 价格",
    'buy': "购买",
    'sell': "出售", 
    'profit': "利润",
    'stats': "统计",
    'settings': "设置",
    'help': "帮助",
    'back': "返回",
    'confirm': "确认",
    'cancel': "取消",
    'save': "保存",
    'delete': "删除",
    'edit': "编辑",
    'view': "查看",
    'manage': "管理",
    'members': "成员",
    'permissions': "权限",
    'sync': "同步",
    'list': "列表",
    'time': "时间",
    'error': "错误",
    'success': "成功",
    'warning': "警告",
}

# Hàm lấy ngôn ngữ của user
def get_lang(user_id):
    """Lấy ngôn ngữ của user (mặc định là VI)"""
    return LANGUAGE.get(user_id, 'VI')

# Hàm dịch
def _(text, user_id):
    """Dịch text sang ngôn ngữ của user"""
    lang = get_lang(user_id)
    if lang == 'ZH':
        return ZH.get(text, VI.get(text, text))
    return VI.get(text, text)
# ==================== USERNAME CACHE ====================
class UsernameCache:
    def __init__(self):
        self.cache = {}
        self.last_update = {}
        self.ttl = 3600
    
    def get(self, username):
        clean = username.lower().replace('@', '')
        if clean in self.cache:
            if time.time() - self.last_update.get(clean, 0) < self.ttl:
                return self.cache[clean]
        return None
    
    def set(self, username, user_id):
        if username:
            clean = username.lower().replace('@', '')
            self.cache[clean] = user_id
            self.last_update[clean] = time.time()
    
    def clear(self):
        self.cache.clear()
        self.last_update.clear()

username_cache = UsernameCache()

# ==================== THÊM HÀM NÀY VÀO ĐÂY ====================

def get_user_id_by_username(username):
    """Lấy user_id từ username, ưu tiên cache trước, sau đó database"""
    try:
        clean_username = username.lower().replace('@', '').strip()
        
        # Kiểm tra cache trước
        cached_id = username_cache.get(clean_username)
        if cached_id:
            logger.info(f"✅ Cache hit for @{clean_username}: {cached_id}")
            return cached_id
        
        # Nếu không có trong cache, tìm trong database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Tìm chính xác username
        c.execute("SELECT user_id FROM users WHERE username = ?", (clean_username,))
        result = c.fetchone()
        
        if result:
            user_id = result[0]
            username_cache.set(clean_username, user_id)
            conn.close()
            return user_id
        
        # Nếu không tìm thấy chính xác, thử tìm gần đúng (cho trường hợp username lưu thiếu @)
        c.execute("SELECT user_id, username FROM users WHERE username LIKE ?", (f"%{clean_username}%",))
        results = c.fetchall()
        
        if results:
            # Lấy kết quả đầu tiên
            user_id = results[0][0]
            username_cache.set(clean_username, user_id)
            logger.info(f"✅ Found {len(results)} users matching '{username}', using first: {user_id}")
            conn.close()
            return user_id
        
        conn.close()
        return None
        
    except Exception as e:
        logger.error(f"❌ Lỗi get_user_id_by_username({username}): {e}")
        return None

# ==================== RENDER CONFIGURATION ====================
class RenderConfig:
    def __init__(self):
        self.is_render = os.environ.get('RENDER', False)
        self.memory_limit = int(os.environ.get('MEMORY_LIMIT', 512))
        self.cpu_limit = float(os.environ.get('CPU_LIMIT', 1))
        self.render_url = os.environ.get('RENDER_EXTERNAL_URL')
        self.start_time = time.time()
        
    def get_worker_count(self):
        if self.is_render:
            return max(1, int(self.cpu_limit) * 2)
        return 4
    
    def should_cleanup(self):
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

# ==================== KIỂM TRA THƯ VIỆN ====================
# Kiểm tra pyzipper cho tính năng xuất file có mật khẩu
try:
    import pyzipper
    HAS_PYZIPPER = True
    logger.info("✅ pyzipper installed - Support AES-256 encryption")
    logger.info(f"   • pyzipper version: {pyzipper.__version__}")
except ImportError:
    HAS_PYZIPPER = False
    logger.warning("⚠️ pyzipper NOT installed - Secure export feature disabled")
    logger.warning("   • To enable: add 'pyzipper==0.3.6' to requirements.txt")

# Kiểm tra cryptography (dự phòng nếu cần)
try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
    logger.info("✅ cryptography installed")
except ImportError:
    HAS_CRYPTO = False
    pass
    
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
        if len(self.cache) >= self.max_size:
            oldest = min(self.cache.keys(), key=lambda k: self.cache[k][1])
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

price_cache = AdvancedCache('price', max_size=50, ttl=60)
usdt_cache = AdvancedCache('usdt', max_size=1, ttl=180)

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

# ==================== PERMISSION DECORATORS ====================
def require_permission(permission_type):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            chat_type = update.effective_chat.type
            
            # Owner bot luôn có quyền
            if is_owner(user_id):
                return await func(update, context, *args, **kwargs)
            
            # PRIVATE CHAT: Ai cũng dùng được
            if chat_type == 'private':
                return await func(update, context, *args, **kwargs)
            
            # TRONG GROUP: Kiểm tra quyền
            if chat_type in ['group', 'supergroup']:
                # ===== CHỈ LỆNH /s ĐƯỢC PUBLIC =====
                func_name = func.__name__
                
                # LỆNH /s (s_command) - PUBLIC, KHÔNG CẦN QUYỀN
                if func_name == 's_command':
                    return await func(update, context, *args, **kwargs)
                # ===== KẾT THÚC =====
                
                # Các lệnh khác - kiểm tra quyền như cũ
                if not check_permission(chat_id, user_id, permission_type):
                    # Thông báo chi tiết hơn
                    command_map = {
                        'buy_command': 'mua coin',
                        'sell_command': 'bán coin',
                        'edit_command': 'sửa giao dịch',
                        'delete_tx_command': 'xóa giao dịch',
                        'alert_command': 'tạo cảnh báo',
                        'alerts_command': 'xem cảnh báo',
                        'stats_command': 'xem thống kê',
                        'usdt_command': 'xem tỷ giá USDT',
                        'export_master_command': 'xuất dữ liệu'
                    }
                    
                    cmd_name = command_map.get(func_name, 'này')
                    
                    await update.message.reply_text(
                        f"❌ *KHÔNG CÓ QUYỀN*\n━━━━━━━━━━━━━━━━\n\n"
                        f"Bạn không có quyền sử dụng lệnh {cmd_name} trong nhóm.\n\n"
                        f"💡 *Lệnh public duy nhất:* `/s btc` (xem giá coin)\n\n"
                        f"👑 Liên hệ chủ nhóm để được cấp quyền.\n\n"
                        f"🕐 {format_vn_time()}", 
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                return await func(update, context, *args, **kwargs)
            
            return await func(update, context, *args, **kwargs)
            
        return wrapper
    return decorator

def require_group_permission(permission_type):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            chat_type = update.effective_chat.type
            
            # Owner bot luôn có quyền
            if is_owner(user_id):
                return await func(update, context, *args, **kwargs)
            
            # Lệnh này chỉ dùng trong group
            if chat_type not in ['group', 'supergroup']:
                await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
                return
            
            # Trong group, kiểm tra quyền
            if not check_permission(chat_id, user_id, permission_type):
                await update.message.reply_text(
                    "❌ *KHÔNG CÓ QUYỀN THỰC HIỆN LỆNH NÀY*\n\n"
                    "Bạn không có quyền sử dụng lệnh này trong nhóm.\n\n"
                    f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
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
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("VACUUM")
            c.execute('''DELETE FROM alerts WHERE triggered_at IS NOT NULL AND date(triggered_at) < date('now', '-30 days')''')
            conn.commit()
            conn.close()
            
            if os.path.exists('bot.log'):
                with open('bot.log', 'r') as f:
                    lines = f.readlines()
                if len(lines) > 1000:
                    with open('bot.log', 'w') as f:
                        f.writelines(lines[-1000:])
            
            size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
            logger.info(f"✅ Database optimized: {size_mb:.2f}MB")
        except Exception as e:
            logger.error(f"❌ Lỗi optimize DB: {e}")

    # ==================== BACKGROUND TASKS ====================
    def check_memory_usage():
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            logger.info(f"📊 Memory: {memory_mb:.2f}MB | CPU: {cpu_percent:.1f}% | Cache: P{price_cache.get_stats()['size']}/U{usdt_cache.get_stats()['size']}")
            
            if memory_mb > render_config.memory_limit * 0.7:
                logger.warning("⚠️ Memory high, cleaning caches...")
                price_cache.clear()
                usdt_cache.clear()
                gc.collect()
                
            if memory_mb > render_config.memory_limit * 0.9:
                logger.critical("💥 Memory critical, restarting...")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Memory check error: {e}")

    def memory_monitor():
        while True:
            check_memory_usage()
            time.sleep(300)

    # ==================== DATABASE SETUP ====================
    def init_database():
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            c = conn.cursor()
            
            c.execute('''CREATE TABLE IF NOT EXISTS portfolio (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, symbol TEXT, amount REAL, buy_price REAL, buy_date TEXT, total_cost REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, symbol TEXT, target_price REAL, condition TEXT, is_active INTEGER DEFAULT 1, created_at TEXT, triggered_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS expense_categories (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT, budget REAL, created_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS expenses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, category_id INTEGER, amount REAL, currency TEXT DEFAULT 'VND', note TEXT, expense_date TEXT, created_at TEXT, FOREIGN KEY (category_id) REFERENCES expense_categories(id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS incomes (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, currency TEXT DEFAULT 'VND', source TEXT, income_date TEXT, note TEXT, created_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, last_seen TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS group_admins (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, admin_id INTEGER, granted_by INTEGER, can_view INTEGER DEFAULT 0, can_edit INTEGER DEFAULT 0, can_delete INTEGER DEFAULT 0, can_manage INTEGER DEFAULT 0, created_at TEXT, UNIQUE(group_id, admin_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS permission_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, action_by INTEGER, target_user INTEGER, action TEXT, old_role TEXT, new_role TEXT, created_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS group_owners (group_id INTEGER PRIMARY KEY, owner_id INTEGER, created_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, granted_by INTEGER, is_approved INTEGER DEFAULT 1, role TEXT DEFAULT 'user', can_view_all INTEGER DEFAULT 0, can_edit_all INTEGER DEFAULT 0, can_delete_all INTEGER DEFAULT 0, can_manage_perms INTEGER DEFAULT 0, created_at TEXT, approved_at TEXT, UNIQUE(group_id, user_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS sell_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                symbol TEXT,
                amount REAL,
                sell_price REAL,
                buy_price REAL,
                total_sold REAL,
                total_cost REAL,
                profit REAL,
                profit_percent REAL,
                sell_date TEXT,
                created_at TEXT
            )''')
            conn.commit()
            mod_init_tables()
            logger.info(f"✅ Database initialized")
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

            c.execute('''CREATE TABLE IF NOT EXISTS sell_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                symbol TEXT,
                amount REAL,
                sell_price REAL,
                buy_price REAL,
                total_sold REAL,
                total_cost REAL,
                profit REAL,
                profit_percent REAL,
                sell_date TEXT,
                created_at TEXT
            )''')

            # === MULTI-GROUP SYSTEM TABLES ===
            c.execute('''CREATE TABLE IF NOT EXISTS master_groups (
                group_id INTEGER PRIMARY KEY,
                group_name TEXT,
                set_by INTEGER,
                created_at TEXT
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS group_hierarchy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_group_id INTEGER,
                child_group_id INTEGER,
                child_group_name TEXT,
                autonomy_level INTEGER DEFAULT 0,
                added_by INTEGER,
                created_at TEXT,
                UNIQUE(master_group_id, child_group_id)
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS group_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER,
                feature_key TEXT,
                is_enabled INTEGER DEFAULT 0,
                set_by INTEGER,
                updated_at TEXT,
                UNIQUE(group_id, feature_key)
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS cross_bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_group_id INTEGER,
                banned_user_id INTEGER,
                banned_by INTEGER,
                reason TEXT,
                banned_at TEXT,
                is_active INTEGER DEFAULT 1,
                UNIQUE(master_group_id, banned_user_id)
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_group_id INTEGER,
                message TEXT,
                sent_by INTEGER,
                target_groups TEXT,
                sent_at TEXT,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0
            )''')

            conn.commit()
            logger.info(f"✅ Database initialized with sell_history + multi-group tables")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi database: {e}")
            return False
        finally:
            if conn:
                conn.close()
                
    def backup_database():
        try:
            if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 1024 * 1024:
                timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')
                shutil.copy2(DB_PATH, backup_path)
                
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
                time.sleep(86400)
            except:
                time.sleep(3600)

    # ==================== BATCH PRICE FETCHING ====================
    def get_prices_batch(symbols):
        try:
            if not CMC_API_KEY or not symbols:
                return {}
            
            results = {}
            uncached = []
            
            for symbol in symbols:
                cached = price_cache.get(symbol)
                if cached:
                    results[symbol] = cached
                else:
                    uncached.append(symbol)
            
            if uncached:
                for i in range(0, len(uncached), 10):
                    batch = uncached[i:i+10]
                    symbols_str = ','.join(batch)
                    
                    headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
                    params = {'symbol': symbols_str, 'convert': 'USD'}
                    
                    res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest", headers=headers, params=params, timeout=10)
                    
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
                    
                    time.sleep(0.5)
            
            return results
        except Exception as e:
            logger.error(f"❌ Batch price error: {e}")
            return {}

    def get_price(symbol):
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
            
            res = requests.get(f"{CMC_API_URL}/cryptocurrency/quotes/latest", headers=headers, params=params, timeout=10)
            
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
        cached = usdt_cache.get('rate')
        if cached:
            return cached
        
        try:
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
            
            c.execute('''INSERT INTO portfolio (user_id, symbol, amount, buy_price, buy_date, total_cost) VALUES (?, ?, ?, ?, ?, ?)''',
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
            c.execute('''SELECT symbol, amount, buy_price, buy_date, total_cost FROM portfolio WHERE user_id = ? ORDER BY buy_date''', (user_id,))
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
                        FROM portfolio WHERE user_id = ? ORDER BY buy_date''', (user_id,))
            transactions = c.fetchall()
            
            # THÊM LOG ĐỂ DEBUG
            logger.info(f"🔍 get_transaction_detail: user_id={user_id}, found={len(transactions)} transactions")
            for tx in transactions:
                logger.info(f"   • #{tx[0]}: {tx[1]} {tx[2]} @ {tx[3]}")
                
            return transactions
        except sqlite3.Error as e:  # <-- THÊM DÒNG NÀY
            logger.error(f"❌ Lỗi SQL lấy transaction: {e}")
            return []
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
            c.execute('''DELETE FROM portfolio WHERE id = ? AND user_id = ?''', (transaction_id, user_id))
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
            
            c.execute('''INSERT INTO alerts (user_id, symbol, target_price, condition, created_at) VALUES (?, ?, ?, ?, ?)''',
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
            c.execute('''SELECT id, symbol, target_price, condition, created_at FROM alerts WHERE user_id = ? AND is_active = 1 ORDER BY created_at''', (user_id,))
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
                c.execute('''SELECT id, user_id, symbol, target_price, condition FROM alerts WHERE is_active = 1''')
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
                            c.execute('''UPDATE alerts SET is_active = 0, triggered_at = ? WHERE id = ?''', 
                                      (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), alert_id))
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            logger.error(f"❌ Lỗi gửi alert {alert_id}: {e}")
            except Exception as e:
                logger.error(f"❌ Lỗi check_alerts: {e}")
                time.sleep(10)

    # ==================== PERMISSIONS FUNCTIONS ====================
    def grant_permission(group_id, user_id, granted_by, permissions):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute("DELETE FROM permissions WHERE group_id = ? AND user_id = ?", (group_id, user_id))
            
            c.execute('''INSERT INTO permissions (group_id, user_id, granted_by, is_approved, role, can_view_all, can_edit_all, can_delete_all, can_manage_perms, created_at, approved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, user_id, granted_by, 1, 'staff',
                       permissions.get('view', 0), permissions.get('edit', 0),
                       permissions.get('delete', 0), permissions.get('manage', 0),
                       created_at, created_at))
            
            conn.commit()
            logger.info(f"✅ Granted permissions to user {user_id} in group {group_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi cấp quyền: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def revoke_permission(group_id, user_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM permissions WHERE group_id = ? AND user_id = ?", (group_id, user_id))
            conn.commit()
            affected = c.rowcount
            conn.close()
            
            if affected > 0:
                logger.info(f"✅ Đã thu hồi quyền của user {user_id} trong group {group_id}")
                return True
            else:
                logger.info(f"ℹ️ Không tìm thấy quyền của user {user_id} trong group {group_id}")
                return False
        except Exception as e:
            logger.error(f"❌ Lỗi thu hồi quyền: {e}")
            return False

    def check_permission(group_id, user_id, permission_type='view'):
        """Kiểm tra quyền của user trong group"""
        conn = None
        try:
            # Owner bot luôn có quyền
            if is_owner(user_id):
                return True
            
            # Chủ sở hữu group luôn có quyền
            owner_id = get_group_owner(group_id)
            if user_id == owner_id:
                return True
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                        FROM permissions WHERE group_id = ? AND user_id = ?''', 
                        (group_id, user_id))
            result = c.fetchone()
            
            if not result:
                return False
            
            can_view, can_edit, can_delete, can_manage = result
            
            # Admin có quyền cao hơn user thường
            if permission_type == 'view':
                return can_view == 1 or can_edit == 1 or can_delete == 1 or can_manage == 1
            elif permission_type == 'edit':
                return can_edit == 1 or can_manage == 1  # Manage cũng có quyền edit
            elif permission_type == 'delete':
                return can_delete == 1 or can_manage == 1  # Manage cũng có quyền delete
            elif permission_type == 'manage':
                return can_manage == 1
            
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi check_permission: {e}")
            return False
        finally:
            if conn:
                conn.close()


    def check_user_access(group_id, user_id, required_role='user'):
        try:
            if is_owner(user_id):
                return True
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute('''SELECT role, is_approved, can_view_all, can_edit_all, can_delete_all, can_manage_perms FROM permissions WHERE group_id = ? AND user_id = ?''', (group_id, user_id))
            result = c.fetchone()
            conn.close()
            
            if not result:
                return False
            
            role, is_approved, can_view, can_edit, can_delete, can_manage = result
            
            if is_approved == 0:
                return False
            
            if required_role == 'owner':
                return role == 'owner'
            elif required_role == 'staff':
                return role in ['owner', 'staff']
            elif required_role == 'user':
                return role in ['owner', 'staff', 'user']
            
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi check_user_access: {e}")
            return False

    def grant_user_access(group_id, target_user_id, granted_by, role='user'):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute("DELETE FROM permissions WHERE group_id = ? AND user_id = ?", (group_id, target_user_id))
            
            if role == 'staff':
                permissions = {'is_approved': 1, 'role': 'staff', 'view': 1, 'edit': 1, 'delete': 1, 'manage': 0}
            else:
                permissions = {'is_approved': 1, 'role': 'user', 'view': 1, 'edit': 0, 'delete': 0, 'manage': 0}
            
            c.execute('''INSERT INTO permissions (group_id, user_id, granted_by, is_approved, role, can_view_all, can_edit_all, can_delete_all, can_manage_perms, created_at, approved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, target_user_id, granted_by,
                       permissions['is_approved'], permissions['role'],
                       permissions['view'], permissions['edit'],
                       permissions['delete'], permissions['manage'],
                       created_at, created_at))
            
            c.execute('''INSERT INTO permission_logs (group_id, action_by, target_user, action, new_role, created_at) VALUES (?, ?, ?, ?, ?, ?)''',
                      (group_id, granted_by, target_user_id, 'GRANT', role, created_at))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Granted {role} access to user {target_user_id} in group {group_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi grant_user_access: {e}")
            return False

    def migrate_admin_data():
        """Di chuyển dữ liệu admin từ bảng cũ sang bảng mới"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Kiểm tra xem bảng group_admins có dữ liệu không
            c.execute("SELECT COUNT(*) FROM group_admins")
            old_admin_count = c.fetchone()[0]
            
            if old_admin_count > 0:
                logger.info(f"🔄 Migrating {old_admin_count} old admin records...")
                
                # Lấy tất cả admin cũ
                c.execute('''
                    SELECT group_id, admin_id, granted_by, can_view, can_edit, can_delete, can_manage, created_at 
                    FROM group_admins
                ''')
                old_admins = c.fetchall()
                
                migrated = 0
                for admin in old_admins:
                    group_id, admin_id, granted_by, can_view, can_edit, can_delete, can_manage, created_at = admin
                    
                    # Kiểm tra xem đã có trong bảng permissions chưa
                    c.execute('''SELECT id FROM permissions WHERE group_id = ? AND user_id = ?''', (group_id, admin_id))
                    if not c.fetchone():
                        # Thêm vào bảng permissions
                        c.execute('''
                            INSERT INTO permissions 
                            (group_id, user_id, granted_by, is_approved, role, 
                             can_view_all, can_edit_all, can_delete_all, can_manage_perms, created_at, approved_at) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            group_id, admin_id, granted_by, 1, 'staff',
                            can_view, can_edit, can_delete, can_manage,
                            created_at, created_at
                        ))
                        migrated += 1
                
                conn.commit()
                logger.info(f"✅ Migrated {migrated} admin records to permissions table")
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Lỗi migrate admin data: {e}")

    # ==================== USER FUNCTIONS WITH AUTO-UPDATE ====================
    async def update_user_info_async(user):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            current_time = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
            exists = c.fetchone()
            
            if exists:
                c.execute('''UPDATE users SET username = ?, first_name = ?, last_name = ?, last_seen = ? WHERE user_id = ?''',
                          (user.username, user.first_name, user.last_name, current_time, user.id))
            else:
                c.execute('''INSERT INTO users (user_id, username, first_name, last_name, last_seen) VALUES (?, ?, ?, ?, ?)''',
                          (user.id, user.username, user.first_name, user.last_name, current_time))
            
            conn.commit()
            conn.close()
            
            if user.username:
                username_cache.set(user.username, user.id)
            
            logger.info(f"✅ Updated user {user.id} (@{user.username})")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi cập nhật user {user.id}: {e}")
            return False

    def get_user_id_by_username(username):
        conn = None
        try:
            clean_username = username.lower().replace('@', '').strip()
            
            cached_id = username_cache.get(clean_username)
            if cached_id:
                logger.info(f"Cache hit for @{clean_username}: {cached_id}")
                return cached_id
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute("SELECT user_id FROM users WHERE username = ?", (clean_username,))
            result = c.fetchone()
            
            if result:
                user_id = result[0]
                username_cache.set(clean_username, user_id)
                return user_id
            
            c.execute("SELECT user_id, username FROM users WHERE username LIKE ?", (f"%{clean_username}%",))
            results = c.fetchall()
            
            if results:
                user_id = results[0][0]
                username_cache.set(clean_username, user_id)
                logger.info(f"Found {len(results)} users matching '{username}', using first: {user_id}")
                return user_id
            
            return None
        except Exception as e:
            logger.error(f"❌ Lỗi tìm user {username}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def auto_update_user(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            if update.effective_user:
                await update_user_info_async(update.effective_user)
            
            chat_type = update.effective_chat.type
            current_user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            
            # ===== QUAN TRỌNG: XÓA SẠCH DỮ LIỆU CŨ =====
            # Xóa tất cả biến liên quan đến quyền trong context
            keys_to_remove = ['effective_user_id', 'is_admin', 'is_owner', 'group_owner_id']
            for key in keys_to_remove:
                if key in context.bot_data:
                    del context.bot_data[key]
            
            # PRIVATE CHAT: LUÔN TỰ QUẢN LÝ, KHÔNG BAO GIỜ LÀ ADMIN
            if chat_type == 'private':
                context.bot_data['effective_user_id'] = current_user_id
                context.bot_data['current_user_id'] = current_user_id
                context.bot_data['chat_type'] = chat_type
                context.bot_data['is_admin'] = False  # LUÔN FALSE
                context.bot_data['is_owner'] = False
                logger.info(f"💬 PRIVATE CHAT: user {current_user_id} tự quản lý (KHÔNG phải admin)")
                return await func(update, context, *args, **kwargs)
            
            # TRONG GROUP
            elif chat_type in ['group', 'supergroup']:
                owner_id = get_group_owner(chat_id)
                
                if not owner_id:
                    await update.message.reply_text(
                        f"❌ *GROUP CHƯA ĐƯỢC CẤU HÌNH*\n\n"
                        f"Vui lòng liên hệ @{OWNER_USERNAME} để thiết lập.\n\n"
                        f"🕐 {format_vn_time()}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
                
                # Lưu owner_id để dùng sau
                context.bot_data['group_owner_id'] = owner_id
                
                # Kiểm tra quyền trong group
                has_permission = check_permission(chat_id, current_user_id, 'view')
                
                if not has_permission:
                    # User không có quyền: vẫn cho phép nhưng tự quản lý
                    context.bot_data['effective_user_id'] = current_user_id
                    context.bot_data['current_user_id'] = current_user_id
                    context.bot_data['chat_type'] = chat_type
                    context.bot_data['is_admin'] = False
                    context.bot_data['is_owner'] = (current_user_id == owner_id)
                    logger.info(f"👤 GROUP: user {current_user_id} chưa có quyền, tự quản lý")
                    return await func(update, context, *args, **kwargs)
                
                # Kiểm tra quyền admin
                is_admin = check_permission(chat_id, current_user_id, 'edit') or \
                          check_permission(chat_id, current_user_id, 'delete') or \
                          check_permission(chat_id, current_user_id, 'manage')
                
                if is_admin or current_user_id == owner_id:
                    # Admin hoặc owner: thao tác trên dữ liệu của owner
                    context.bot_data['effective_user_id'] = owner_id
                    context.bot_data['is_admin'] = True
                    context.bot_data['is_owner'] = (current_user_id == owner_id)
                    logger.info(f"👑 GROUP: admin {current_user_id} thao tác trên dữ liệu owner {owner_id}")
                else:
                    # User thường có quyền view: tự quản lý
                    context.bot_data['effective_user_id'] = current_user_id
                    context.bot_data['is_admin'] = False
                    context.bot_data['is_owner'] = False
                    logger.info(f"👤 GROUP: user {current_user_id} có quyền view, tự quản lý")
                
                context.bot_data['current_user_id'] = current_user_id
                context.bot_data['chat_type'] = chat_type
                
                return await func(update, context, *args, **kwargs)
            
            # Các loại chat khác
            else:
                context.bot_data['effective_user_id'] = current_user_id
                context.bot_data['current_user_id'] = current_user_id
                context.bot_data['chat_type'] = chat_type
                context.bot_data['is_admin'] = False
                context.bot_data['is_owner'] = False
                return await func(update, context, *args, **kwargs)
                
        return wrapper
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
            
            c.execute('''INSERT INTO expense_categories (user_id, name, budget, created_at) VALUES (?, ?, ?, ?)''',
                      (user_id, name.upper(), budget, created_at))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi thêm category: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_expense_categories(owner_id):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, name, budget, created_at FROM expense_categories WHERE user_id = ? ORDER BY name''', (owner_id,))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy categories: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def add_income(owner_id, amount, source, currency='VND', note=""):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            income_date = now.strftime("%Y-%m-%d")
            created_at = now.strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute('''INSERT INTO incomes (user_id, amount, source, income_date, note, created_at, currency) VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (owner_id, amount, source, income_date, note, created_at, currency))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi thêm income: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def add_expense(owner_id, category_id, amount, currency='VND', note=""):
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = get_vn_time()
            expense_date = now.strftime("%Y-%m-%d")
            created_at = now.strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute('''INSERT INTO expenses (user_id, category_id, amount, note, expense_date, created_at, currency) VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (owner_id, category_id, amount, note, expense_date, created_at, currency))
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
            c.execute('''SELECT id, amount, source, note, income_date, currency FROM incomes WHERE user_id = ? ORDER BY income_date DESC, created_at DESC LIMIT ?''', (user_id, limit))
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
            c.execute('''SELECT e.id, ec.name, e.amount, e.note, e.expense_date, e.currency FROM expenses e JOIN expense_categories ec ON e.category_id = ec.id WHERE e.user_id = ? ORDER BY e.expense_date DESC, e.created_at DESC LIMIT ?''', (user_id, limit))
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
                query = '''SELECT id, amount, source, note, currency, income_date FROM incomes WHERE user_id = ? AND income_date = ? ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, date_filter))
            elif period == 'month':
                month_filter = now.strftime("%Y-%m")
                query = '''SELECT id, amount, source, note, currency, income_date FROM incomes WHERE user_id = ? AND strftime('%Y-%m', income_date) = ? ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, month_filter))
            else:
                year_filter = now.strftime("%Y")
                query = '''SELECT id, amount, source, note, currency, income_date FROM incomes WHERE user_id = ? AND strftime('%Y', income_date) = ? ORDER BY income_date DESC, created_at DESC'''
                c.execute(query, (user_id, year_filter))
            
            rows = c.fetchall()
            
            summary = {}
            for row in rows:
                id, amount, source, note, currency, date = row
                if currency not in summary:
                    summary[currency] = 0
                summary[currency] += amount
            
            return {'transactions': rows, 'summary': summary, 'total_count': len(rows)}
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
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget FROM expenses e JOIN expense_categories ec ON e.category_id = ec.id WHERE e.user_id = ? AND e.expense_date = ? ORDER BY e.expense_date DESC, e.created_at DESC'''
                c.execute(query, (user_id, date_filter))
            elif period == 'month':
                month_filter = now.strftime("%Y-%m")
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget FROM expenses e JOIN expense_categories ec ON e.category_id = ec.id WHERE e.user_id = ? AND strftime('%Y-%m', e.expense_date) = ? ORDER BY e.expense_date DESC, e.created_at DESC'''
                c.execute(query, (user_id, month_filter))
            else:
                year_filter = now.strftime("%Y")
                query = '''SELECT e.id, ec.name, e.amount, e.note, e.currency, e.expense_date, ec.budget FROM expenses e JOIN expense_categories ec ON e.category_id = ec.id WHERE e.user_id = ? AND strftime('%Y', e.expense_date) = ? ORDER BY e.expense_date DESC, e.created_at DESC'''
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
            
            return {'transactions': rows, 'summary': summary, 'category_summary': category_summary, 'total_count': len(rows)}
        except Exception as e:
            logger.error(f"❌ Lỗi expenses summary: {e}")
            return {'transactions': [], 'summary': {}, 'category_summary': {}, 'total_count': 0}
        finally:
            if conn:
                conn.close()

    def get_balance_summary(user_id, period='month'):
        try:
            if period == 'day':
                incomes = get_income_by_period(user_id, 'day')
                expenses = get_expenses_by_period(user_id, 'day')
                title = f"HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})"
            elif period == 'month':
                incomes = get_income_by_period(user_id, 'month')
                expenses = get_expenses_by_period(user_id, 'month')
                title = f"THÁNG {get_vn_time().strftime('%m/%Y')}"
            elif period == 'year':
                incomes = get_income_by_period(user_id, 'year')
                expenses = get_expenses_by_period(user_id, 'year')
                title = f"NĂM {get_vn_time().strftime('%Y')}"
            else:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                c.execute('''SELECT currency, SUM(amount) FROM incomes WHERE user_id = ? GROUP BY currency''', (user_id,))
                income_rows = c.fetchall()
                
                c.execute('''SELECT currency, SUM(amount) FROM expenses WHERE user_id = ? GROUP BY currency''', (user_id,))
                expense_rows = c.fetchall()
                
                conn.close()
                
                incomes = {'summary': {}}
                expenses = {'summary': {}}
                
                for currency, total in income_rows:
                    incomes['summary'][currency] = total
                
                for currency, total in expense_rows:
                    expenses['summary'][currency] = total
                
                title = "TỔNG KẾT TẤT CẢ"
            
            # Lấy tất cả các loại tiền tệ
            all_currencies = set(list(incomes['summary'].keys()) + list(expenses['summary'].keys()))
            
            balance_data = []
            
            for currency in all_currencies:
                income = incomes['summary'].get(currency, 0)
                expense = expenses['summary'].get(currency, 0)
                balance = income - expense
                
                balance_data.append({
                    'currency': currency,
                    'income': income,
                    'expense': expense,
                    'balance': balance,
                    'status': 'positive' if balance > 0 else 'negative' if balance < 0 else 'zero'
                })
            
            return {
                'title': title,
                'period': period,
                'balances': balance_data,
                'income_count': incomes.get('total_count', 0),
                'expense_count': expenses.get('total_count', 0)
            }
        except Exception as e:
            logger.error(f"❌ Lỗi get_balance_summary: {e}")
            return None

    def format_balance_message(balance_data, user_name=""):
        if not balance_data:
            return "❌ Không có dữ liệu để hiển thị!"
        
        # Icon và tên cho các loại tiền tệ
        currency_icons = {
            'VND': '🇻🇳',
            'USD': '🇺🇸',
            'USDT': '💵',
            'KHR': '🇰🇭',
            'LKR': '🇱🇰'
        }
        
        msg = f"⚖️ *CÂN ĐỐI THU CHI - {balance_data['title']}*"
        if user_name:
            msg += f" - {user_name}"
        msg += "\n━━━━━━━━━━━━━━━━\n\n"
        
        # Hiển thị theo từng loại tiền tệ
        for b in balance_data['balances']:
            currency = b['currency']
            income = b['income']
            expense = b['expense']
            balance = b['balance']
            
            # Icon cho loại tiền
            icon = currency_icons.get(currency, '💱')
            
            # Header cho loại tiền
            msg += f"{icon} *{currency}*\n"
            msg += "```\n"
            
            # Thu nhập
            if income > 0:
                msg += f"💰 Thu:    {format_currency_simple(income, currency):>15}\n"
            else:
                msg += f"💰 Thu:    {'0':>15}\n"
            
            # Chi tiêu
            if expense > 0:
                msg += f"💸 Chi:    {format_currency_simple(expense, currency):>15}\n"
            else:
                msg += f"💸 Chi:    {'0':>15}\n"
            
            # Đường kẻ
            msg += f"{'─'*25}\n"
            
            # Cân đối
            if balance > 0:
                msg += f"✅ Dư:     {format_currency_simple(balance, currency):>15}\n"
            elif balance < 0:
                msg += f"❌ Thiếu:  {format_currency_simple(abs(balance), currency):>15}\n"
            else:
                msg += f"➖ Cân bằng: {'0':>15}\n"
            
            msg += "```\n"
        
        # Thống kê số giao dịch
        msg += f"\n📊 *THỐNG KÊ:*\n"
        msg += f"• {balance_data['income_count']} khoản thu\n"
        msg += f"• {balance_data['expense_count']} khoản chi\n"
        
        # Tổng số dư theo từng loại tiền (không quy đổi)
        msg += f"\n💎 *SỐ DƯ HIỆN TẠI:*\n"
        for b in balance_data['balances']:
            if b['balance'] != 0:
                icon = currency_icons.get(b['currency'], '💱')
                msg += f"• {icon} {format_currency_simple(b['balance'], b['currency'])}\n"
        
        msg += f"\n🕐 {format_vn_time()}"
        
        return msg

    # ==================== HÀM XÓA ====================
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

    def delete_category(category_id, owner_id):
        """Xóa danh mục và tất cả chi tiêu liên quan"""
        logger.info(f"🔍 delete_category được gọi với category_id={category_id}, owner_id={owner_id}")
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # BẬT KHÓA NGOẠI
            c.execute("PRAGMA foreign_keys = ON")
            logger.info(f"🔧 PRAGMA foreign_keys = ON cho user {owner_id}")
            
            # Kiểm tra danh mục có tồn tại không
            logger.info(f"🔍 Đang tìm danh mục ID {category_id} cho user {owner_id}")
            c.execute('''SELECT id, name FROM expense_categories WHERE id = ? AND user_id = ?''', (category_id, owner_id))
            category = c.fetchone()
            
            if not category:
                logger.warning(f"❌ Không tìm thấy danh mục ID {category_id} cho user {owner_id}")
                return False, "❌ Không tìm thấy danh mục!", 0
            
            category_name = category[1]
            logger.info(f"📝 Tìm thấy danh mục: '{category_name}' (ID: {category_id})")
            
            # Đếm số khoản chi
            logger.info(f"🔍 Đếm số khoản chi trong danh mục {category_id}")
            c.execute('''SELECT COUNT(*) FROM expenses WHERE category_id = ? AND user_id = ?''', (category_id, owner_id))
            expenses_count = c.fetchone()[0]
            logger.info(f"📊 Có {expenses_count} khoản chi trong danh mục '{category_name}'")
            
            # Bắt đầu transaction
            logger.info("🔄 Bắt đầu transaction...")
            c.execute("BEGIN TRANSACTION")
            
            # Xóa chi tiêu trước
            logger.info(f"🗑 Đang xóa chi tiêu trong danh mục {category_id}...")
            c.execute('''DELETE FROM expenses WHERE category_id = ? AND user_id = ?''', (category_id, owner_id))
            deleted_expenses = c.rowcount
            logger.info(f"✅ Đã xóa {deleted_expenses} khoản chi (dự kiến: {expenses_count})")
            
            # Xóa danh mục
            logger.info(f"🗑 Đang xóa danh mục {category_id}...")
            c.execute('''DELETE FROM expense_categories WHERE id = ? AND user_id = ?''', (category_id, owner_id))
            
            if c.rowcount == 0:
                logger.error(f"❌ Không thể xóa danh mục {category_id} - rowcount = 0")
                conn.rollback()
                logger.info("↩️ Đã rollback transaction")
                return False, "❌ Không thể xóa danh mục!", 0
            
            # Commit transaction
            conn.commit()
            logger.info("💾 Đã commit transaction")
            
            logger.info(f"✅ ĐÃ XÓA THÀNH CÔNG danh mục '{category_name}' (ID: {category_id}), kèm {deleted_expenses} khoản chi")
            
            return True, category_name, deleted_expenses
            
        except sqlite3.IntegrityError as e:
            # Lỗi ràng buộc khóa ngoại
            if conn:
                conn.rollback()
                logger.info("↩️ Đã rollback transaction do lỗi IntegrityError")
            
            logger.error(f"❌ LỖI INTEGRITY: {e}", exc_info=True)
            logger.error(f"   • category_id: {category_id}")
            logger.error(f"   • owner_id: {owner_id}")
            
            # Thử cách khác: xóa từng bước
            try:
                logger.info("🔄 Thử xóa bằng cách 2 (không dùng transaction)...")
                conn2 = sqlite3.connect(DB_PATH)
                c2 = conn2.cursor()
                
                # Xóa chi tiêu trước
                c2.execute('''DELETE FROM expenses WHERE category_id = ? AND user_id = ?''', (category_id, owner_id))
                deleted = c2.rowcount
                logger.info(f"✅ Cách 2: Đã xóa {deleted} khoản chi")
                
                # Xóa danh mục sau
                c2.execute('''DELETE FROM expense_categories WHERE id = ? AND user_id = ?''', (category_id, owner_id))
                logger.info(f"✅ Cách 2: Đã xóa danh mục")
                
                conn2.commit()
                conn2.close()
                
                logger.info(f"✅ Cách 2 THÀNH CÔNG: đã xóa danh mục '{category_name}', kèm {deleted} khoản chi")
                return True, category_name, deleted
                
            except Exception as e2:
                logger.error(f"❌ Cách 2 cũng thất bại: {e2}", exc_info=True)
                return False, f"❌ Lỗi ràng buộc dữ liệu: {str(e)}", 0
                
        except Exception as e:
            if conn:
                conn.rollback()
                logger.info("↩️ Đã rollback transaction do lỗi Exception")
            
            logger.error(f"❌ LỖI NGOẠI LỆ: {e}", exc_info=True)
            logger.error(f"   • category_id: {category_id}")
            logger.error(f"   • owner_id: {owner_id}")
            logger.error(f"   • Kiểu lỗi: {type(e).__name__}")
            
            return False, str(e), 0
            
        finally:
            if conn:
                conn.close()
                logger.info("🔚 Đã đóng kết nối database")

    def get_sell_history(user_id, limit=50):
        """Lấy lịch sử bán của user"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, symbol, amount, sell_price, buy_price, profit, profit_percent, sell_date, created_at 
                        FROM sell_history 
                        WHERE user_id = ? 
                        ORDER BY sell_date DESC, created_at DESC 
                        LIMIT ?''', (user_id, limit))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy sell history: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_sell_detail(sell_id, user_id):
        """Lấy chi tiết một lệnh bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT * FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            return c.fetchone()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy sell detail: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def delete_sell_history(sell_id, user_id):
        """Xóa lịch sử bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''DELETE FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            conn.commit()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Lỗi xóa sell history: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_sell_history(sell_id, user_id, amount=None, sell_price=None):
        """Cập nhật lịch sử bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Lấy thông tin cũ
            c.execute('''SELECT amount, sell_price, total_sold, total_cost FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            old = c.fetchone()
            if not old:
                return False, "Không tìm thấy lệnh bán"
            
            old_amount, old_price, old_total_sold, old_total_cost = old
            
            # Tính toán mới
            new_amount = amount if amount is not None else old_amount
            new_price = sell_price if sell_price is not None else old_price
            new_total_sold = new_amount * new_price
            new_profit = new_total_sold - old_total_cost
            new_profit_percent = (new_profit / old_total_cost * 100) if old_total_cost > 0 else 0
            
            c.execute('''UPDATE sell_history 
                        SET amount = ?, sell_price = ?, total_sold = ?, profit = ?, profit_percent = ?
                        WHERE id = ? AND user_id = ?''',
                      (new_amount, new_price, new_total_sold, new_profit, new_profit_percent, sell_id, user_id))
            conn.commit()
            return True, "Đã cập nhật thành công"
        except Exception as e:
            logger.error(f"❌ Lỗi update sell history: {e}")
            return False, str(e)
        finally:
            if conn:
                conn.close()
    
    def add_sell_history_manual(user_id, symbol, amount, sell_price, buy_price, sell_date):
        """Thêm lịch sử bán thủ công (cho dữ liệu cũ)"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            total_sold = amount * sell_price
            total_cost = amount * buy_price
            profit = total_sold - total_cost
            profit_percent = (profit / total_cost * 100) if total_cost > 0 else 0
            created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            c.execute('''INSERT INTO sell_history 
                        (user_id, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_percent, sell_date, created_at) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (user_id, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_percent, sell_date, created_at))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi thêm sell history: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_sell_history(user_id, limit=50):
        """Lấy lịch sử bán của user"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id, symbol, amount, sell_price, buy_price, profit, profit_percent, sell_date, created_at 
                        FROM sell_history 
                        WHERE user_id = ? 
                        ORDER BY sell_date DESC, created_at DESC 
                        LIMIT ?''', (user_id, limit))
            return c.fetchall()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy sell history: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_sell_detail(sell_id, user_id):
        """Lấy chi tiết một lệnh bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT * FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            return c.fetchone()
        except Exception as e:
            logger.error(f"❌ Lỗi lấy sell detail: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def delete_sell_history(sell_id, user_id):
        """Xóa lịch sử bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''DELETE FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            conn.commit()
            return c.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Lỗi xóa sell history: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_sell_history(sell_id, user_id, amount=None, sell_price=None):
        """Cập nhật lịch sử bán"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Lấy thông tin cũ
            c.execute('''SELECT amount, sell_price, total_sold, total_cost FROM sell_history WHERE id = ? AND user_id = ?''', (sell_id, user_id))
            old = c.fetchone()
            if not old:
                return False, "Không tìm thấy lệnh bán"
            
            old_amount, old_price, old_total_sold, old_total_cost = old
            
            # Tính toán mới
            new_amount = amount if amount is not None else old_amount
            new_price = sell_price if sell_price is not None else old_price
            new_total_sold = new_amount * new_price
            new_profit = new_total_sold - old_total_cost
            new_profit_percent = (new_profit / old_total_cost * 100) if old_total_cost > 0 else 0
            
            c.execute('''UPDATE sell_history 
                        SET amount = ?, sell_price = ?, total_sold = ?, profit = ?, profit_percent = ?
                        WHERE id = ? AND user_id = ?''',
                      (new_amount, new_price, new_total_sold, new_profit, new_profit_percent, sell_id, user_id))
            conn.commit()
            return True, "Đã cập nhật thành công"
        except Exception as e:
            logger.error(f"❌ Lỗi update sell history: {e}")
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def edit_income(income_id, user_id, amount=None, source=None, note=None, currency=None):
        """Sửa thông tin khoản thu
        
        Args:
            income_id: ID khoản thu
            user_id: ID người dùng
            amount: Số tiền mới (None nếu không sửa)
            source: Nguồn thu mới (None nếu không sửa)
            note: Ghi chú mới (None nếu không sửa)
            currency: Loại tiền mới (None nếu không sửa)
        
        Returns:
            (success, message)
        """
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Kiểm tra khoản thu có tồn tại không
            c.execute('''SELECT id FROM incomes WHERE id = ? AND user_id = ?''', (income_id, user_id))
            if not c.fetchone():
                return False, "❌ Không tìm thấy khoản thu!"
            
            # Xây dựng câu lệnh UPDATE dựa trên các trường được cung cấp
            updates = []
            params = []
            
            if amount is not None:
                updates.append("amount = ?")
                params.append(amount)
            
            if source is not None:
                updates.append("source = ?")
                params.append(source)
            
            if note is not None:
                updates.append("note = ?")
                params.append(note)
            
            if currency is not None:
                updates.append("currency = ?")
                params.append(currency)
            
            if not updates:
                return False, "❌ Không có thông tin nào để cập nhật!"
            
            # Thêm điều kiện WHERE
            query = f"UPDATE incomes SET {', '.join(updates)} WHERE id = ? AND user_id = ?"
            params.extend([income_id, user_id])
            
            c.execute(query, params)
            conn.commit()
            
            if c.rowcount > 0:
                logger.info(f"✅ Edited income {income_id} for user {user_id}")
                return True, "✅ Đã cập nhật khoản thu thành công!"
            else:
                return False, "❌ Không thể cập nhật khoản thu!"
                
        except Exception as e:
            logger.error(f"❌ Lỗi edit income: {e}")
            return False, f"❌ Lỗi: {str(e)}"
        finally:
            if conn:
                conn.close()

    def edit_expense(expense_id, user_id, amount=None, category_id=None, note=None, currency=None):
        """Sửa thông tin khoản chi
        
        Args:
            expense_id: ID khoản chi
            user_id: ID người dùng
            amount: Số tiền mới (None nếu không sửa)
            category_id: ID danh mục mới (None nếu không sửa)
            note: Ghi chú mới (None nếu không sửa)
            currency: Loại tiền mới (None nếu không sửa)
        
        Returns:
            (success, message)
        """
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Kiểm tra khoản chi có tồn tại không
            c.execute('''SELECT id FROM expenses WHERE id = ? AND user_id = ?''', (expense_id, user_id))
            if not c.fetchone():
                return False, "❌ Không tìm thấy khoản chi!"
            
            # Nếu có category_id mới, kiểm tra category có tồn tại không
            if category_id is not None:
                c.execute('''SELECT id FROM expense_categories WHERE id = ? AND user_id = ?''', (category_id, user_id))
                if not c.fetchone():
                    return False, "❌ Không tìm thấy danh mục mới!"
            
            # Xây dựng câu lệnh UPDATE
            updates = []
            params = []
            
            if amount is not None:
                updates.append("amount = ?")
                params.append(amount)
            
            if category_id is not None:
                updates.append("category_id = ?")
                params.append(category_id)
            
            if note is not None:
                updates.append("note = ?")
                params.append(note)
            
            if currency is not None:
                updates.append("currency = ?")
                params.append(currency)
            
            if not updates:
                return False, "❌ Không có thông tin nào để cập nhật!"
            
            # Thêm điều kiện WHERE
            query = f"UPDATE expenses SET {', '.join(updates)} WHERE id = ? AND user_id = ?"
            params.extend([expense_id, user_id])
            
            c.execute(query, params)
            conn.commit()
            
            if c.rowcount > 0:
                logger.info(f"✅ Edited expense {expense_id} for user {user_id}")
                return True, "✅ Đã cập nhật khoản chi thành công!"
            else:
                return False, "❌ Không thể cập nhật khoản chi!"
                
        except Exception as e:
            logger.error(f"❌ Lỗi edit expense: {e}")
            return False, f"❌ Lỗi: {str(e)}"
        finally:
            if conn:
                conn.close()

    # ==================== KEYBOARD ====================
    def get_main_keyboard(user_id=None):
        lang = get_lang(user_id) if user_id else 'VI'
        
        if lang == 'ZH':
            keyboard = [
                [KeyboardButton("💰 加密货币"), KeyboardButton("💵 支出管理")],
                [KeyboardButton("⚙️ 设置"), KeyboardButton("🤔 帮助")],
            ]
        else:
            keyboard = [
                [KeyboardButton("💰 ĐẦU TƯ COIN"), KeyboardButton("💵 QUẢN LÝ CHI TIÊU")],
                [KeyboardButton("⚙️ CÀI ĐẶT"), KeyboardButton("🤔 HƯỚNG DẪN")],
            ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def get_invest_menu_keyboard(user_id=None, group_id=None, chat_type=None):
        lang = get_lang(user_id) if user_id else 'VI'
        
        if lang == 'ZH':
            keyboard = [
                [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
                 InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
                 InlineKeyboardButton("Ξ SOL", callback_data="price_SOL"),
                 InlineKeyboardButton("💵 USDT", callback_data="price_USDT")],
                [InlineKeyboardButton("📊 前10", callback_data="show_top10"),
                 InlineKeyboardButton("📈 利润", callback_data="show_profit")],
                [InlineKeyboardButton("✏️ 编辑/删除", callback_data="edit_transactions"),
                 InlineKeyboardButton("📊 统计", callback_data="show_stats")],
                [InlineKeyboardButton("🔔 价格提醒", callback_data="show_alerts"),
                 InlineKeyboardButton("🔐 导出数据", callback_data="export_master")],
                [InlineKeyboardButton("➕ 购买", callback_data="show_buy"),
                 InlineKeyboardButton("➖ 出售", callback_data="show_sell")]
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("₿ BTC", callback_data="price_BTC"),
                 InlineKeyboardButton("Ξ ETH", callback_data="price_ETH"),
                 InlineKeyboardButton("Ξ SOL", callback_data="price_SOL"),
                 InlineKeyboardButton("💵 USDT", callback_data="price_USDT")],
                [InlineKeyboardButton("📊 Top 10", callback_data="show_top10"),
                 InlineKeyboardButton("📈 Lợi nhuận", callback_data="show_profit")],
                [InlineKeyboardButton("✏️ Sửa/Xóa", callback_data="edit_transactions"),
                 InlineKeyboardButton("📊 Thống kê", callback_data="show_stats")],
                [InlineKeyboardButton("🔔 Cảnh báo giá", callback_data="show_alerts"),
                 InlineKeyboardButton("🔐 Xuất dữ liệu", callback_data="export_master")],
                [InlineKeyboardButton("➕ Mua coin", callback_data="show_buy"),
                 InlineKeyboardButton("➖ Bán coin", callback_data="show_sell")]
            ]
        
        # Thêm nút ADMIN nếu cần
        if group_id and user_id:
            if chat_type in ['group', 'supergroup'] and check_permission(group_id, user_id, 'view'):
                admin_text = "👑 ADMIN" if lang == 'VI' else "👑 管理员"
                keyboard.append([InlineKeyboardButton(admin_text, callback_data="admin_panel")])
        
        return InlineKeyboardMarkup(keyboard)

    def get_expense_menu_keyboard():
        keyboard = [
            [InlineKeyboardButton("💰 THU NHẬP", callback_data="expense_income_menu"),
             InlineKeyboardButton("💸 CHI TIÊU", callback_data="expense_expense_menu")],
            [InlineKeyboardButton("📋 DANH MỤC", callback_data="expense_categories"),
             InlineKeyboardButton("⚖️ CÂN ĐỐI", callback_data="balance_month")],
            [InlineKeyboardButton("📅 HÔM NAY", callback_data="expense_today"),
             InlineKeyboardButton("📅 THÁNG NÀY", callback_data="expense_month")],
            [InlineKeyboardButton("🔄 GẦN ĐÂY", callback_data="expense_recent"),
             InlineKeyboardButton("🔐 Xuất báo cáo", callback_data="export_expense_menu")],  # <-- NÚT DUY NHẤT
            [InlineKeyboardButton("🔙 VỀ MENU CHÍNH", callback_data="back_to_main")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # ==================== COMMAND HANDLERS ====================
    @auto_update_user
    @require_permission('edit')
    async def edit_income_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Sửa khoản thu: /editthu [id] [số tiền] [nguồn] [ghi chú]"""
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        if len(ctx.args) < 2:
            # Hiển thị danh sách thu gần đây để chọn
            recent = get_recent_incomes(owner_id, 10)
            if not recent:
                await update.message.reply_text("📭 Không có khoản thu nào để sửa!")
                return
            
            msg = "✏️ *CHỌN KHOẢN THU CẦN SỬA*\n━━━━━━━━━━━━━━━━\n\n"
            for inc in recent:
                inc_id, amount, source, note, date, currency = inc
                msg += f"• #{inc_id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                if note:
                    msg += f"  📝 {note}\n"
            
            msg += f"\n🕐 {format_vn_time_short()}\n\n"
            msg += "👉 Dùng: `/editthu [id] [số tiền] [nguồn] [ghi chú]`\n"
            msg += "VD: `/editthu 5 200000 Lương tháng 3`"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        try:
            income_id = int(ctx.args[0])
            
            # Parse các tham số
            amount = None
            source = None
            note = None
            currency = None
            
            if len(ctx.args) >= 2:
                # Thử parse số tiền
                try:
                    amount = float(ctx.args[1].replace(',', ''))
                except:
                    pass
            
            if len(ctx.args) >= 3:
                # Kiểm tra nếu là currency
                if ctx.args[2].upper() in SUPPORTED_CURRENCIES:
                    currency = ctx.args[2].upper()
                    if len(ctx.args) >= 4:
                        source = ctx.args[3]
                        note = " ".join(ctx.args[4:]) if len(ctx.args) > 4 else ""
                else:
                    source = ctx.args[2]
                    note = " ".join(ctx.args[3:]) if len(ctx.args) > 3 else ""
            
            success, message = edit_income(income_id, owner_id, amount, source, note, currency)
            
            if success:
                # Lấy thông tin mới để hiển thị
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT amount, source, note, currency FROM incomes WHERE id = ?''', (income_id,))
                updated = c.fetchone()
                conn.close()
                
                if updated:
                    new_amount, new_source, new_note, new_currency = updated
                    msg = (f"✅ *ĐÃ SỬA KHOẢN THU #{income_id}*\n━━━━━━━━━━━━━━━━\n\n"
                           f"💰 Số tiền: {format_currency_simple(new_amount, new_currency)}\n"
                           f"📌 Nguồn: {new_source}\n"
                           f"📝 Ghi chú: {new_note if new_note else 'Không có'}\n\n"
                           f"🕐 {format_vn_time()}")
                    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(message)
                
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi: {str(e)}")

    @auto_update_user
    @require_permission('edit')
    async def edit_expense_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Sửa khoản chi: /editchi [id] [số tiền] [mã DM] [ghi chú]"""
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        if len(ctx.args) < 2:
            # Hiển thị danh sách chi gần đây để chọn
            recent = get_recent_expenses(owner_id, 10)
            if not recent:
                await update.message.reply_text("📭 Không có khoản chi nào để sửa!")
                return
            
            msg = "✏️ *CHỌN KHOẢN CHI CẦN SỬA*\n━━━━━━━━━━━━━━━━\n\n"
            for exp in recent:
                exp_id, cat_name, amount, note, date, currency = exp
                # Escape các ký tự đặc biệt
                safe_cat = escape_markdown(cat_name)
                safe_note = escape_markdown(note) if note else ""
                msg += f"• #{exp_id} {date}: {format_currency_simple(amount, currency)} - {safe_cat}\n"
                if safe_note:
                    msg += f"  📝 {safe_note}\n"
            
            msg += f"\n🕐 {format_vn_time_short()}\n\n"
            msg += "👉 Dùng: `/editchi [id] [số tiền] [mã DM] [ghi chú]`\n"
            msg += "VD: `/editchi 3 75000 1 Ăn trưa`"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        try:
            expense_id = int(ctx.args[0])
            
            # Parse các tham số
            amount = None
            category_id = None
            note = None
            currency = None
            
            if len(ctx.args) >= 2:
                try:
                    amount = float(ctx.args[1].replace(',', ''))
                except:
                    pass
            
            if len(ctx.args) >= 3:
                # Thử parse category_id
                try:
                    category_id = int(ctx.args[2])
                    if len(ctx.args) >= 4:
                        # Kiểm tra currency
                        if ctx.args[3].upper() in SUPPORTED_CURRENCIES:
                            currency = ctx.args[3].upper()
                            note = " ".join(ctx.args[4:]) if len(ctx.args) > 4 else ""
                        else:
                            note = " ".join(ctx.args[3:]) if len(ctx.args) > 3 else ""
                except:
                    # Nếu không phải số, có thể là note
                    note = " ".join(ctx.args[2:])
            
            success, message = edit_expense(expense_id, owner_id, amount, category_id, note, currency)
            
            if success:
                # Lấy thông tin mới để hiển thị
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT e.amount, ec.name, e.note, e.currency 
                           FROM expenses e 
                           JOIN expense_categories ec ON e.category_id = ec.id 
                           WHERE e.id = ?''', (expense_id,))
                updated = c.fetchone()
                conn.close()
                
                if updated:
                    new_amount, new_cat, new_note, new_currency = updated
                    # Escape các ký tự đặc biệt
                    safe_cat = escape_markdown(new_cat)
                    safe_note = escape_markdown(new_note) if new_note else ""
                    
                    msg = (f"✅ *ĐÃ SỬA KHOẢN CHI #{expense_id}*\n━━━━━━━━━━━━━━━━\n\n"
                           f"💰 Số tiền: {format_currency_simple(new_amount, new_currency)}\n"
                           f"📂 Danh mục: {safe_cat}\n"
                           f"📝 Ghi chú: {safe_note if safe_note else 'Không có'}\n\n"
                           f"🕐 {format_vn_time()}")
                    
                    # Escape toàn bộ msg trước khi gửi
                    safe_msg = escape_markdown(msg)
                    await update.message.reply_text(safe_msg, parse_mode=ParseMode.MARKDOWN)
            else:
                # Message từ edit_expense đã có thể có ký tự đặc biệt
                safe_message = escape_markdown(message)
                await update.message.reply_text(safe_message)
                
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ!")
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi: {str(e)}")

    @auto_update_user
    async def grant_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Cấp quyền sử dụng bot cho user: /grant @user [quyền]"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        # Kiểm tra có phải trong group không
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        # Chỉ chủ sở hữu nhóm mới được cấp quyền
        owner_id = get_group_owner(chat_id)
        if user_id != owner_id and not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ chủ sở hữu nhóm mới có thể cấp quyền!")
            return
        
        # Nếu không có tham số, hiển thị hướng dẫn
        if not ctx.args:
            # Lấy danh sách user đã có quyền
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''
                SELECT p.user_id, p.can_view_all, p.can_edit_all, p.can_delete_all, p.can_manage_perms, 
                       u.username, u.first_name 
                FROM permissions p 
                LEFT JOIN users u ON p.user_id = u.user_id 
                WHERE p.group_id = ?
                ORDER BY p.created_at
            ''', (chat_id,))
            granted_users = c.fetchall()
            conn.close()
            
            msg = "📝 *HƯỚNG DẪN CẤP QUYỀN*\n━━━━━━━━━━━━━━━━\n\n"
            msg += "*Các mức quyền:*\n"
            msg += "• `view` - Xem giá, portfolio, lợi nhuận\n"
            msg += "• `edit` - Được thêm/sửa giao dịch\n"
            msg += "• `delete` - Được xóa giao dịch\n"
            msg += "• `manage` - Quản lý phân quyền\n"
            msg += "• `full` - Tất cả quyền trên\n\n"
            
            msg += "*Cú pháp:*\n"
            msg += "`/grant @username view`\n"
            msg += "`/grant @username edit`\n"
            msg += "`/grant @username full`\n\n"
            
            if granted_users:
                msg += "*Danh sách đã cấp quyền:*\n"
                for u in granted_users:
                    uid, view, edit, delete, manage, username, first_name = u
                    display = f"@{username}" if username else first_name or f"User {uid}"
                    perms = []
                    if view: perms.append("👁")
                    if edit: perms.append("✏️")
                    if delete: perms.append("🗑")
                    if manage: perms.append("🔐")
                    msg += f"• {display}: {' '.join(perms)}\n"
            else:
                msg += "*Chưa có ai được cấp quyền*"
            
            msg += f"\n\n🕐 {format_vn_time_short()}"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        # Xử lý cấp quyền
        target = ctx.args[0]
        perm_type = ctx.args[1].lower() if len(ctx.args) > 1 else 'view'
        
        # Lấy user_id từ username
        if target.startswith('@'):
            username = target[1:]
            target_id = get_user_id_by_username(username)
            if not target_id:
                await update.message.reply_text(f"❌ Không tìm thấy user {target}\n\nHãy yêu cầu họ nhắn tin riêng cho bot trước!")
                return
        else:
            try:
                target_id = int(target)
            except:
                await update.message.reply_text("❌ ID không hợp lệ!")
                return
        
        # Không cho tự cấp quyền cho chính mình
        if target_id == user_id:
            await update.message.reply_text("❌ Bạn không thể tự cấp quyền cho chính mình!")
            return
        
        # Xác định quyền
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
            await update.message.reply_text("❌ Loại quyền không hợp lệ! Chỉ chấp nhận: view, edit, delete, manage, full")
            return
        
        # Cấp quyền
        if grant_permission(chat_id, target_id, user_id, permissions):
            # Lấy tên hiển thị
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (target_id,))
            user_info = c.fetchone()
            conn.close()
            
            display_name = f"@{user_info[0]}" if user_info and user_info[0] else (user_info[1] if user_info else f"User {target_id}")
            
            # Tạo message thông báo
            perm_emoji = {
                'view': '👁',
                'edit': '✏️',
                'delete': '🗑',
                'manage': '🔐',
                'full': '👑'
            }
            
            msg = (f"✅ *CẤP QUYỀN THÀNH CÔNG*\n━━━━━━━━━━━━━━━━\n\n"
                   f"• Người dùng: {display_name}\n"
                   f"• Quyền: {perm_emoji.get(perm_type, '📌')} {perm_type.upper()}\n\n")
            
            if perm_type == 'view':
                msg += "Họ có thể:\n• Xem giá coin\n• Xem portfolio\n• Xem lợi nhuận\n• Xem thống kê"
            elif perm_type == 'edit':
                msg += "Họ có thể:\n• Xem dữ liệu\n• Thêm giao dịch mới\n• Sửa giao dịch"
            elif perm_type == 'delete':
                msg += "Họ có thể:\n• Xem dữ liệu\n• Xóa giao dịch"
            elif perm_type == 'manage':
                msg += "Họ có thể:\n• Quản lý phân quyền\n• Cấp/thu hồi quyền cho người khác"
            elif perm_type == 'full':
                msg += "Họ có TOÀN QUYỀN trong nhóm này!"
            
            msg += f"\n\n🕐 {format_vn_time()}"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            
            # Thông báo cho người được cấp quyền
            try:
                await ctx.bot.send_message(
                    target_id,
                    f"✅ *BẠN ĐÃ ĐƯỢC CẤP QUYỀN*\n━━━━━━━━━━━━━━━━\n\n"
                    f"• Nhóm: {update.effective_chat.title}\n"
                    f"• Quyền: {perm_emoji.get(perm_type, '📌')} {perm_type.upper()}\n\n"
                    f"Bạn có thể sử dụng bot trong nhóm này!\n\n"
                    f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass
        else:
            await update.message.reply_text("❌ Lỗi khi cấp quyền! Vui lòng thử lại sau.")

    @auto_update_user
    async def myperm_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Kiểm tra quyền của bản thân trong nhóm: /myperm"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        # Trong private chat
        if chat_type == 'private':
            msg = (
                f"👤 *THÔNG TIN CÁ NHÂN*\n━━━━━━━━━━━━━━━━\n\n"
                f"• ID: `{user_id}`\n"
                f"• Username: @{update.effective_user.username or 'None'}\n\n"
                f"📌 Trong private chat, bạn có toàn quyền với dữ liệu của mình.\n\n"
                f"🕐 {format_vn_time()}"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        # Trong group
        if chat_type in ['group', 'supergroup']:
            # Lấy tên nhóm từ Telegram
            try:
                chat = await ctx.bot.get_chat(chat_id)
                group_name = chat.title or "Nhóm không tên"
            except:
                group_name = "Nhóm này"
            
            # Kiểm tra có phải chủ sở hữu không
            owner_id = get_group_owner(chat_id)
            is_group_owner = (user_id == owner_id)
            
            # Lấy thông tin quyền từ database
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                        FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, user_id))
            result = c.fetchone()
            conn.close()
            
            # Tạo message
            msg = f"🔐 *QUYỀN CỦA BẠN TRONG NHÓM*\n━━━━━━━━━━━━━━━━\n\n"
            msg += f"📌 Nhóm: {group_name}\n"
            msg += f"👤 Bạn: @{update.effective_user.username or 'None'} (`{user_id}`)\n\n"
            
            if is_group_owner:
                msg += "👑 *BẠN LÀ CHỦ SỞ HỮU NHÓM*\n• Có TOÀN QUYỀN quản lý dữ liệu\n• Có thể cấp quyền cho người khác\n\n"
            elif is_owner(user_id):
                msg += "👑 *BẠN LÀ OWNER BOT*\n• Có TOÀN QUYỀN ở mọi nhóm\n\n"
            elif result:
                can_view, can_edit, can_delete, can_manage = result
                msg += "*CHI TIẾT QUYỀN:*\n"
                msg += f"• 👁 Xem dữ liệu: {'✅' if can_view else '❌'}\n"
                msg += f"• ✏️ Thêm/sửa giao dịch: {'✅' if can_edit else '❌'}\n"
                msg += f"• 🗑 Xóa giao dịch: {'✅' if can_delete else '❌'}\n"
                msg += f"• 🔐 Quản lý phân quyền: {'✅' if can_manage else '❌'}\n"
                
                if can_edit or can_delete or can_manage:
                    msg += "\n📌 Bạn có thể quản lý dữ liệu của chủ sở hữu nhóm.\n"
            else:
                msg += "❌ *BẠN CHƯA CÓ QUYỀN*\n\n"
                msg += "Bạn chưa được cấp quyền sử dụng bot trong nhóm này.\n"
                msg += "Vui lòng liên hệ chủ sở hữu nhóm để được cấp quyền.\n"
            
            msg += f"\n🕐 {format_vn_time()}"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
            
    @auto_update_user
    async def whoami_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT user_id, username, first_name, last_name, last_seen FROM users WHERE user_id = ?''', (user.id,))
        db_user = c.fetchone()
        conn.close()
        
        msg = f"👤 *THÔNG TIN CỦA BẠN*\n━━━━━━━━━━━━━━━━\n\n"
        msg += f"• ID: `{user.id}`\n"
        msg += f"• Username: @{user.username if user.username else 'None'}\n"
        msg += f"• First Name: {user.first_name}\n"
        msg += f"• Last Name: {user.last_name}\n\n"
        
        if db_user:
            msg += f"*📦 DATABASE:*\n"
            msg += f"• Username: @{db_user[1] if db_user[1] else 'None'}\n"
            msg += f"• Last Seen: {db_user[4]}\n"
            msg += f"• Status: ✅ Đã được lưu"
        else:
            msg += f"• Status: ❌ Chưa được lưu trong database"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    async def quick_grant_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền quản lý phân quyền!")
            return
        
        if not update.message.reply_to_message:
            await update.message.reply_text("❌ Hãy reply tin nhắn của người cần grant!")
            return
        
        if not ctx.args:
            await update.message.reply_text("❌ Thiếu loại quyền! VD: `/permgrant view`", parse_mode=ParseMode.MARKDOWN)
            return
        
        target_user = update.message.reply_to_message.from_user
        perm_type = ctx.args[0].lower()
        
        await update_user_info_async(target_user)
        
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
        
        if grant_permission(chat_id, target_user.id, user_id, permissions):
            await update.message.reply_text(f"✅ Đã cấp quyền {perm_type} cho @{target_user.username or target_user.id}")
        else:
            await update.message.reply_text("❌ Lỗi khi cấp quyền!")

    @auto_update_user
    async def getid_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        msg = f"🔑 *THÔNG TIN ID*\n━━━━━━━━━━━━━━━━\n\n"
        msg += f"👤 *Bạn:*\n"
        msg += f"• ID: `{user.id}`\n"
        msg += f"• Username: @{user.username if user.username else 'None'}\n\n"
        
        if update.message.reply_to_message and update.message.reply_to_message.from_user:
            replied = update.message.reply_to_message.from_user
            msg += f"👥 *Người được reply:*\n"
            msg += f"• ID: `{replied.id}`\n"
            msg += f"• Username: @{replied.username if replied.username else 'None'}\n"
        
        msg += f"\n💡 Dùng ID để grant: `/perm grant {user.id} view`"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    async def sync_users_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền!")
            return
        
        msg = await update.message.reply_text("🔄 Đang đồng bộ danh sách thành viên...")
        
        try:
            admins = await ctx.bot.get_chat_administrators(chat_id)
            count = 0
            
            for admin in admins:
                if admin.user:
                    await update_user_info_async(admin.user)
                    count += 1
            
            await msg.edit_text(f"✅ *ĐỒNG BỘ THÀNH CÔNG*\n━━━━━━━━━━━━━━━━\n\n📊 Đã cập nhật: {count} admin\n👥 Tổng số: {len(admins)} thành viên\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await msg.edit_text(f"❌ Lỗi: {e}")

    @auto_update_user
    async def owner_panel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ Owner mới có quyền sử dụng lệnh này!")
            return
        
        if not ctx.args:
            msg = ("👑 *OWNER PANEL*\n━━━━━━━━━━━━━━━━\n\n"
                   "*QUẢN LÝ NHÂN VIÊN:*\n"
                   "• `/owner addstaff @user` - Thêm nhân viên\n"
                   "• `/owner removestaff @user` - Xóa nhân viên\n"
                   "• `/owner liststaff` - Danh sách nhân viên\n\n"
                   "*QUẢN LÝ NGƯỜI DÙNG:*\n"
                   "• `/owner approve @user` - Duyệt user\n"
                   "• `/owner revoke @user` - Thu hồi quyền\n"
                   "• `/owner listpending` - DS chờ duyệt\n"
                   "• `/owner listusers` - DS người dùng\n\n"
                   "*THỐNG KÊ:*\n"
                   "• `/owner stats` - Thống kê hệ thống\n\n"
                   f"🕐 {format_vn_time()}")
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        action = ctx.args[0].lower()
        
        if action == "addstaff" and len(ctx.args) >= 2:
            target = ctx.args[1]
            target_id = await resolve_user_id(target, ctx)
            
            if not target_id:
                await update.message.reply_text("❌ Không tìm thấy user!")
                return
            
            chat_id = update.effective_chat.id
            
            if grant_user_access(chat_id, target_id, user_id, role='staff'):
                await update.message.reply_text(f"✅ Đã thêm @{target} làm nhân viên!\nHọ có thể quản lý dữ liệu trong group này.")
            else:
                await update.message.reply_text("❌ Lỗi khi thêm nhân viên!")

        elif action == "removestaff" and len(ctx.args) >= 2:
            target = ctx.args[1]
            target_id = await resolve_user_id(target, ctx)
            
            if not target_id:
                await update.message.reply_text("❌ Không tìm thấy user!")
                return
            
            chat_id = update.effective_chat.id
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT role FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, target_id))
            result = c.fetchone()
            
            if not result or result[0] != 'staff':
                conn.close()
                await update.message.reply_text(f"❌ {target} không phải là nhân viên!")
                return
            conn.close()
            
            if revoke_permission(chat_id, target_id):
                await update.message.reply_text(f"✅ Đã xóa @{target} khỏi danh sách nhân viên!")
            else:
                await update.message.reply_text("❌ Lỗi khi xóa nhân viên!")

        elif action == "liststaff":
            chat_id = update.effective_chat.id
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT p.user_id, p.can_view_all, p.can_edit_all, p.can_delete_all, p.can_manage_perms, u.username, u.first_name FROM permissions p LEFT JOIN users u ON p.user_id = u.user_id WHERE p.group_id = ? AND p.role = 'staff' ORDER BY p.created_at''', (chat_id,))
            staff_list = c.fetchall()
            conn.close()
            
            if not staff_list:
                await update.message.reply_text("📭 Chưa có nhân viên nào!")
                return
            
            msg = "👥 *DANH SÁCH NHÂN VIÊN*\n━━━━━━━━━━━━━━━━\n\n"
            for staff in staff_list:
                user_id, view, edit, delete, manage, username, first_name = staff
                
                if username:
                    display = f"`{user_id}` @{username}"
                elif first_name:
                    display = f"`{user_id}` {first_name}"
                else:
                    display = f"`{user_id}`"
                
                permissions = []
                if view: permissions.append("👁 Xem")
                if edit: permissions.append("✏️ Sửa")
                if delete: permissions.append("🗑 Xóa")
                if manage: permissions.append("🔐 Quản lý")
                
                msg += f"• {display}: {', '.join(permissions)}\n"
            
            msg += f"\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        elif action == "revoke" and len(ctx.args) >= 2:
            target = ctx.args[1]
            target_id = await resolve_user_id(target, ctx)
            
            if not target_id:
                await update.message.reply_text("❌ Không tìm thấy user!")
                return
            
            chat_id = update.effective_chat.id
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT role FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, target_id))
            result = c.fetchone()
            conn.close()
            
            if not result:
                await update.message.reply_text(f"❌ {target} chưa được cấp quyền!")
                return
            
            if target_id == user_id:
                await update.message.reply_text("❌ Không thể tự thu hồi quyền của chính mình!")
                return
            
            if revoke_permission(chat_id, target_id):
                await update.message.reply_text(f"✅ Đã thu hồi toàn bộ quyền của {target}!")
            else:
                await update.message.reply_text("❌ Lỗi khi thu hồi quyền!")
        
        elif action == "approve" and len(ctx.args) >= 2:
            target = ctx.args[1]
            target_id = await resolve_user_id(target, ctx)
            
            if not target_id:
                await update.message.reply_text("❌ Không tìm thấy user!")
                return
            
            chat_id = update.effective_chat.id
            
            if grant_user_access(chat_id, target_id, user_id, role='user'):
                await update.message.reply_text(f"✅ Đã duyệt @{target} sử dụng bot!\nHọ có thể xem dữ liệu trong group này.")
            else:
                await update.message.reply_text("❌ Lỗi khi duyệt user!")
        
        elif action == "listpending":
            chat_id = update.effective_chat.id
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT user_id, username, first_name, created_at FROM permissions WHERE group_id = ? AND is_approved = 0 AND role = 'user' ORDER BY created_at''', (chat_id,))
            pending = c.fetchall()
            conn.close()
            
            if not pending:
                await update.message.reply_text("📭 Không có user nào chờ duyệt!")
                return
            
            msg = "⏳ *DANH SÁCH CHỜ DUYỆT*\n━━━━━━━━━━━━━━━━\n\n"
            for user in pending:
                user_id, username, first_name, created = user
                display = f"@{username}" if username else first_name or f"User {user_id}"
                msg += f"• {display} (`{user_id}`) - {created[:10]}\n"
            
            msg += f"\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        elif action == "stats":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM users")
            total_users = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM permissions WHERE role = 'staff'")
            total_staff = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM permissions WHERE role = 'user' AND is_approved = 1")
            total_approved = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM permissions WHERE role = 'user' AND is_approved = 0")
            total_pending = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM portfolio")
            total_transactions = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM portfolio")
            users_with_portfolio = c.fetchone()[0]
            
            conn.close()
            
            msg = ("📊 *THỐNG KÊ HỆ THỐNG*\n━━━━━━━━━━━━━━━━\n\n"
                   f"👥 *Tổng user:* {total_users}\n"
                   f"👑 *Nhân viên:* {total_staff}\n"
                   f"✅ *Đã duyệt:* {total_approved}\n"
                   f"⏳ *Chờ duyệt:* {total_pending}\n\n"
                   f"💼 *Giao dịch:* {total_transactions}\n"
                   f"👤 *User có portfolio:* {users_with_portfolio}\n\n"
                   f"🕐 {format_vn_time()}")
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def resolve_user_id(target, ctx):
        """Giải quyết user ID từ username hoặc ID, với fallback từ Telegram API"""
        if target.startswith('@'):
            username = target[1:]
            user_id = get_user_id_by_username(username)
            
            if not user_id:
                # Thử lấy từ Telegram API
                try:
                    # Thử lấy chat từ username
                    chat = await ctx.bot.get_chat(username)
                    if chat:
                        user_id = chat.id
                        # Lưu vào database
                        await update_user_info_async(chat)
                        logger.info(f"✅ Lấy user {user_id} từ Telegram API cho @{username}")
                except Exception as e:
                    logger.error(f"❌ Lỗi get_chat từ Telegram: {e}")
                    
                    # Thử lấy bằng cách khác: gửi tin nhắn tạm? (không khả thi)
                    # Thông báo lỗi
                    pass
            
            return user_id
        else:
            try:
                # Thử parse thành số
                return int(target)
            except ValueError:
                # Nếu không phải số, thử lấy từ reply
                if ctx.message and ctx.message.reply_to_message:
                    return ctx.message.reply_to_message.from_user.id
        return None
        
    @auto_update_user
    async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        lang = get_lang(user_id)
        
        if update.effective_chat.type in ['group', 'supergroup']:
            if lang == 'ZH':
                welcome_msg = ("🚀 *加密货币与支出管理机器人*\n\n"
                               "🤖 机器人已就绪！\n\n"
                               "*群组命令:*\n"
                               "• `/s btc eth` - 查看价格\n"
                               "• `/usdt` - USDT/VND汇率\n"
                               "• `/buy btc 0.5 40000` - 购买\n"
                               "• `/sell btc 0.2` - 出售\n\n"
                               "📱 *向下滑动显示菜单*\n"
                               f"🕐 {format_vn_time()}")
            else:
                welcome_msg = ("🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*\n\n"
                               "🤖 Bot đã sẵn sàng!\n\n"
                               "*Các lệnh trong nhóm:*\n"
                               "• `/s btc eth` - Xem giá coin\n"
                               "• `/usdt` - Tỷ giá USDT/VND\n"
                               "• `/buy btc 0.5 40000` - Mua coin\n"
                               "• `/sell btc 0.2` - Bán coin\n\n"
                               "📱 *Vuốt xuống để hiện menu*\n"
                               f"🕐 {format_vn_time()}")
            await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard(user_id))
        else:
            if lang == 'ZH':
                welcome_msg = ("🚀 *加密货币与支出管理机器人*\n\n"
                               "🤖 机器人支持:\n\n"
                               "*💎 加密货币投资:*\n"
                               "• 查看价格\n• 前10加密货币\n• 投资组合管理\n• 利润计算\n• 价格提醒\n\n"
                               "*💰 支出管理:*\n"
                               "• 记录收入/支出\n• 多币种支持\n• 预算管理\n• 日报/月报/年报\n\n"
                               f"🕐 *当前时间:* `{format_vn_time()}`\n\n"
                               "👇 *请选择功能*")
            else:
                welcome_msg = ("🚀 *ĐẦU TƯ COIN & QUẢN LÝ CHI TIÊU*\n\n"
                               "🤖 Bot hỗ trợ:\n\n"
                               "*💎 ĐẦU TƯ COIN:*\n"
                               "• Xem giá coin\n• Top 10 coin\n• Quản lý danh mục\n• Tính lợi nhuận\n• Cảnh báo giá\n\n"
                               "*💰 QUẢN LÝ CHI TIÊU:*\n"
                               "• Ghi chép thu/chi\n• Đa tiền tệ\n• Quản lý ngân sách\n• Báo cáo ngày/tháng/năm\n\n"
                               f"🕐 *Hiện tại:* `{format_vn_time()}`\n\n"
                               "👇 *Chọn chức năng bên dưới*")
            await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard(user_id))

    @auto_update_user
    async def menu_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("👇 *Chọn chức năng bên dưới*", parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_keyboard())

    @auto_update_user
    async def hide_keyboard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("✅ Đã ẩn bàn phím. Gõ /menu để hiện lại.", reply_markup=ReplyKeyboardRemove())

    @auto_update_user
    async def lang_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Chọn ngôn ngữ: /lang [vi/zh]"""
        user_id = update.effective_user.id
        
        if not ctx.args:
            # Hiển thị menu chọn ngôn ngữ
            keyboard = [
                [InlineKeyboardButton("🇻🇳 Tiếng Việt", callback_data="lang_vi")],
                [InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh")],
            ]
            
            current = "Tiếng Việt" if get_lang(user_id) == 'VI' else "中文"
            msg = (f"🌐 *CHỌN NGÔN NGỮ*\n━━━━━━━━━━━━━━━━\n\n"
                   f"Hiện tại: {current}\n\n"
                   f"Vui lòng chọn ngôn ngữ:")
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, 
                                           reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        lang = ctx.args[0].lower()
        if lang in ['vi', 'vietnamese']:
            LANGUAGE[user_id] = 'VI'
            await update.message.reply_text("✅ Đã chuyển sang Tiếng Việt!")
        elif lang in ['zh', 'chinese', 'cn']:
            LANGUAGE[user_id] = 'ZH'
            await update.message.reply_text("✅ 已切换到中文！")
        else:
            await update.message.reply_text("❌ Ngôn ngữ không hỗ trợ! /lang vi hoặc /lang zh")
        
    @auto_update_user
    async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        help_msg = "📘 *HƯỚNG DẪN SỬ DỤNG BOT*\n━━━━━━━━━━━━━━━━\n\n"
        
        help_msg += (
            "*💰 ĐẦU TƯ COIN:*\n"
            "• `/s btc eth` - Xem giá coin\n"
            "• `/usdt` - Tỷ giá USDT/VND\n"
            "• `/buy btc 0.5 40000` - Mua coin\n"
            "• `/sell btc 0.2` - Bán coin\n"
            "• `/alert BTC above 50000` - Cảnh báo giá\n\n"
            
            "*💸 QUẢN LÝ CHI TIÊU:*\n"
            "• `tn 500000` - Thêm thu nhập\n"
            "• `dm Ăn uống 3000000` - Tạo danh mục\n"
            "• `ct 1 50000` - Thêm chi tiêu\n"
            "• `ds` - Xem giao dịch gần đây\n"
            "• `/balance` - Xem cân đối thu chi\n\n"
            
            "*🔐 QUẢN LÝ NHÓM:*\n"
            "• `/grant @user view` - Cấp quyền xem\n"
            "• `/myperm` - Kiểm tra quyền của bạn\n"
            "• `/groupinfo` - Thông tin nhóm\n\n"
            
            f"🕐 {format_vn_time()}"
        )
        
        await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    @require_permission('view')
    async def usdt_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = await update.message.reply_text("🔄 Đang tra cứu...")
        rate_data = get_usdt_vnd_rate()
        vnd = rate_data['vnd']
        
        text = ("💱 *TỶ GIÁ USDT/VND*\n━━━━━━━━━━━━━━━━\n\n"
                f"🇺🇸 *1 USDT* = `{fmt_vnd(vnd)}`\n"
                f"🇻🇳 *1,000,000 VND* = `{1000000/vnd:.4f} USDT`\n\n"
                f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
                f"📊 *Nguồn:* `{rate_data['source']}`")
        
        keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                    [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        
        await msg.delete()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

    @auto_update_user
    @require_permission('view')
    async def s_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not ctx.args:
            return await update.message.reply_text("❌ /s btc eth doge")
        
        msg = await update.message.reply_text("🔄 Đang tra cứu...")
        
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

    @auto_update_user
    @require_permission('edit')
    async def buy_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Xác định user_id thực sự cần thêm giao dịch
        chat_type = update.effective_chat.type
        current_user_id = update.effective_user.id
        
        if chat_type == 'private':
            # Private chat: thêm cho chính mình
            target_user_id = current_user_id
            logger.info(f"💬 PRIVATE: mua coin cho user {target_user_id}")
        else:
            # Group chat: thêm cho chủ sở hữu (nếu có quyền)
            target_user_id = ctx.bot_data.get('effective_user_id', current_user_id)
            logger.info(f"👥 GROUP: mua coin cho owner {target_user_id}")
        
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
        
        if add_transaction(target_user_id, symbol, amount, buy_price):
            current_price = price_data['p']
            profit = (current_price - buy_price) * amount
            profit_percent = ((current_price - buy_price) / buy_price) * 100
            
            added_by = f" (thêm bởi @{update.effective_user.username})" if update.effective_user.username else ""
            
            # Thông báo ai là người sở hữu
            owner_info = ""
            if chat_type != 'private' and target_user_id != current_user_id:
                owner_info = f"\n📌 Dữ liệu thuộc về chủ sở hữu group"
            
            msg = (f"✅ *ĐÃ MUA {symbol}*{added_by}\n━━━━━━━━━━━━━━━━\n\n"
                   f"📊 SL: `{amount:.4f}`\n"
                   f"💰 Giá mua: `{fmt_price(buy_price)}`\n"
                   f"💵 Vốn: `{fmt_price(amount * buy_price)}`\n"
                   f"📈 Giá hiện: `{fmt_price(current_price)}`\n"
                   f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_percent:+.2f}%){owner_info}\n\n"
                   f"🕐 {format_vn_time()}")
            await update.message.reply_text(msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"❌ Lỗi khi thêm giao dịch *{symbol}*", parse_mode='Markdown')

    @auto_update_user
    @require_permission('edit')
    async def sell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Xác định user_id thực sự
        chat_type = update.effective_chat.type
        current_user_id = update.effective_user.id
        
        if chat_type == 'private':
            target_user_id = current_user_id
            logger.info(f"💬 PRIVATE: bán coin cho user {target_user_id}")
        else:
            target_user_id = ctx.bot_data.get('effective_user_id', current_user_id)
            logger.info(f"👥 GROUP: bán coin cho owner {target_user_id}")
        
        # Hiển thị hướng dẫn nếu không có tham số
        if len(ctx.args) < 1:
            keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
            msg = (
                "📝 *HƯỚNG DẪN BÁN COIN*\n"
                "━━━━━━━━━━━━━━━━\n\n"
                "*Cách 1: Bán theo số lượng (giá thị trường)*\n"
                "`/sell [coin] [số lượng]`\n"
                "📌 Ví dụ: `/sell btc 0.5`\n\n"
                "*Cách 2: Bán theo số lượng với giá chỉ định*\n"
                "`/sell [coin] [số lượng] [giá bán]`\n"
                "📌 Ví dụ: `/sell btc 0.5 45000`\n\n"
                "*Cách 3: Bán toàn bộ*\n"
                "`/sell [coin] all`\n"
                "📌 Ví dụ: `/sell btc all`\n\n"
                "*Cách 4: Bán theo giá trị USD*\n"
                "`/sell [coin] $[giá trị]`\n"
                "📌 Ví dụ: `/sell btc $1000`\n\n"
                f"🕐 {format_vn_time_short()}"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        symbol = ctx.args[0].upper()
        
        # Kiểm tra coin có tồn tại không
        price_data = get_price(symbol)
        if not price_data:
            await update.message.reply_text(f"❌ Không thể lấy giá *{symbol}*", parse_mode='Markdown')
            return
        
        current_price = price_data['p']
        
        # Lấy portfolio
        portfolio_data = get_portfolio(target_user_id)
        if not portfolio_data:
            await update.message.reply_text("📭 Danh mục trống!")
            return
        
        # Chuyển đổi portfolio thành list dict
        portfolio = []
        for row in portfolio_data:
            portfolio.append({
                'symbol': row[0], 
                'amount': row[1], 
                'buy_price': row[2], 
                'buy_date': row[3], 
                'total_cost': row[4]
            })
        
        # Lọc các giao dịch của coin cần bán
        symbol_txs = [tx for tx in portfolio if tx['symbol'] == symbol]
        if not symbol_txs:
            await update.message.reply_text(f"❌ Không có *{symbol}* trong danh mục", parse_mode='Markdown')
            return
        
        total_amount = sum(tx['amount'] for tx in symbol_txs)
        
        # Xác định số lượng cần bán
        sell_amount = 0
        sell_price = current_price  # Mặc định là giá thị trường
        
        # Xử lý tham số
        if len(ctx.args) >= 2:
            amount_arg = ctx.args[1].lower()
            
            # Kiểm tra nếu là "all" - bán toàn bộ
            if amount_arg == 'all':
                sell_amount = total_amount
            
            # Kiểm tra nếu bán theo giá trị USD (có dấu $)
            elif amount_arg.startswith('$'):
                try:
                    usd_value = float(amount_arg[1:].replace(',', ''))
                    sell_amount = usd_value / current_price
                    if sell_amount > total_amount:
                        await update.message.reply_text(f"❌ Giá trị ${usd_value:,.2f} tương đương {sell_amount:.4f} {symbol}\n📊 Bạn chỉ có {total_amount:.4f} {symbol}")
                        return
                except ValueError:
                    await update.message.reply_text("❌ Giá trị USD không hợp lệ!")
                    return
            
            # Bán theo số lượng
            else:
                try:
                    sell_amount = float(amount_arg)
                except ValueError:
                    await update.message.reply_text("❌ Số lượng không hợp lệ!")
                    return
        
        # Nếu có tham số giá bán
        if len(ctx.args) >= 3:
            try:
                sell_price = float(ctx.args[2])
                if sell_price <= 0:
                    await update.message.reply_text("❌ Giá bán phải > 0!")
                    return
                
                # Cảnh báo nếu giá bán khác xa giá thị trường
                price_diff_percent = ((sell_price - current_price) / current_price) * 100
                if abs(price_diff_percent) > 10:
                    warning_msg = await update.message.reply_text(
                        f"⚠️ *CẢNH BÁO*\nGiá bán của bạn ({fmt_price(sell_price)}) "
                        f"{'cao hơn' if price_diff_percent > 0 else 'thấp hơn'} "
                        f"{abs(price_diff_percent):.1f}% so với giá thị trường ({fmt_price(current_price)})!\n\n"
                        f"Bạn có chắc muốn bán ở mức giá này?",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    # Xóa cảnh báo sau 10 giây
                    asyncio.create_task(auto_delete_message(ctx, update.effective_chat.id, warning_msg.message_id, 10))
            except ValueError:
                await update.message.reply_text("❌ Giá bán không hợp lệ!")
                return
        
        # Kiểm tra số lượng bán
        if sell_amount <= 0:
            await update.message.reply_text("❌ Số lượng phải > 0")
            return
        
        if sell_amount > total_amount:
            await update.message.reply_text(f"❌ Bạn chỉ có {total_amount:.4f} {symbol}")
            return
        
        # Xác nhận trước khi bán (nếu bán số lượng lớn > 10% portfolio)
        if sell_amount > total_amount * 0.1 and len(ctx.args) < 3:
            keyboard = [[
                InlineKeyboardButton("✅ Xác nhận", callback_data=f"confirm_sell_{symbol}_{sell_amount}_{sell_price}"),
                InlineKeyboardButton("❌ Hủy", callback_data="cancel_sell")
            ]]
            
            msg = (
                f"⚠️ *XÁC NHẬN BÁN {symbol}*\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"📊 Số lượng: `{sell_amount:.4f}`\n"
                f"💰 Giá bán: `{fmt_price(sell_price)}`\n"
                f"💵 Tổng giá trị: `{fmt_price(sell_amount * sell_price)}`\n"
                f"📈 Giá thị trường: `{fmt_price(current_price)}`\n"
                f"📊 Tỷ lệ trong portfolio: `{(sell_amount/total_amount*100):.1f}%`\n\n"
                f"Bạn có chắc muốn bán?"
            )
            
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Thực hiện bán
        await execute_sell(update, ctx, target_user_id, symbol, sell_amount, sell_price, current_price, portfolio, current_user_id)

    async def execute_sell(update, ctx, target_user_id, symbol, sell_amount, sell_price, current_price, portfolio, current_user_id):
        """Thực thi lệnh bán coin"""
        remaining_sell = sell_amount
        new_portfolio = []
        sold_value = 0
        sold_cost = 0
        sold_transactions = []
        
        # Sắp xếp giao dịch theo FIFO
        portfolio.sort(key=lambda x: x['buy_date'])
        
        for tx in portfolio:
            if tx['symbol'] == symbol and remaining_sell > 0:
                if tx['amount'] <= remaining_sell:
                    # Bán toàn bộ giao dịch này
                    sold_cost += tx['total_cost']
                    sold_value += tx['amount'] * sell_price
                    remaining_sell -= tx['amount']
                    
                    sold_transactions.append({
                        'amount': tx['amount'],
                        'buy_price': tx['buy_price'],
                        'profit': (sell_price - tx['buy_price']) * tx['amount']
                    })
                else:
                    # Bán một phần
                    sell_part = remaining_sell
                    part_cost = sell_part * tx['buy_price']
                    sold_cost += part_cost
                    sold_value += sell_part * sell_price
                    
                    tx['amount'] -= sell_part
                    tx['total_cost'] = tx['amount'] * tx['buy_price']
                    new_portfolio.append(tx)
                    
                    sold_transactions.append({
                        'amount': sell_part,
                        'buy_price': tx['buy_price'],
                        'profit': (sell_price - tx['buy_price']) * sell_part
                    })
                    
                    remaining_sell = 0
            else:
                new_portfolio.append(tx)
        
        # Cập nhật database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Xóa tất cả giao dịch cũ
        c.execute("DELETE FROM portfolio WHERE user_id = ?", (target_user_id,))
        
        # Thêm lại các giao dịch còn lại
        for tx in new_portfolio:
            c.execute('''INSERT INTO portfolio (user_id, symbol, amount, buy_price, buy_date, total_cost) 
                        VALUES (?, ?, ?, ?, ?, ?)''',
                      (target_user_id, tx['symbol'], tx['amount'], tx['buy_price'], tx['buy_date'], tx['total_cost']))
        
        # ===== THÊM CODE LƯU LỊCH SỬ BÁN =====
        profit = sold_value - sold_cost
        profit_percent = (profit / sold_cost * 100) if sold_cost > 0 else 0
        
        # Tính giá vốn trung bình
        avg_buy_price = sold_cost / sell_amount if sell_amount > 0 else 0
        
        created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
        sell_date = get_vn_time().strftime("%Y-%m-%d")
        
        c.execute('''INSERT INTO sell_history 
                    (user_id, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_percent, sell_date, created_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (target_user_id, symbol, sell_amount, sell_price, avg_buy_price, 
                   sold_value, sold_cost, profit, profit_percent, sell_date, created_at))
        
        conn.commit()
        conn.close()
        
        # Tính toán lợi nhuận
        profit = sold_value - sold_cost
        profit_percent = (profit / sold_cost * 100) if sold_cost > 0 else 0
        
        # Thông tin người bán
        sold_by = f" (bán bởi @{update.effective_user.username})" if update.effective_user.username else ""
        
        # Thông tin chủ sở hữu
        owner_info = ""
        if update.effective_chat.type != 'private' and target_user_id != current_user_id:
            owner_info = f"\n📌 Dữ liệu thuộc về chủ sở hữu group"
        
        # Tạo message chi tiết
        msg = (f"✅ *ĐÃ BÁN {sell_amount:.4f} {symbol}*{sold_by}\n"
               f"━━━━━━━━━━━━━━━━\n\n"
               f"💰 Giá bán: `{fmt_price(sell_price)}`\n"
               f"💵 Giá trị bán: `{fmt_price(sold_value)}`\n"
               f"📊 Vốn gốc: `{fmt_price(sold_cost)}`\n"
               f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n")
        
        # Thêm chi tiết từng giao dịch nếu bán nhiều
        if len(sold_transactions) > 1:
            msg += f"\n*📋 CHI TIẾT GIAO DỊCH:*\n"
            for i, tx in enumerate(sold_transactions, 1):
                tx_profit = tx['profit']
                tx_profit_pct = ((sell_price - tx['buy_price']) / tx['buy_price']) * 100
                msg += f"{i}. SL: `{tx['amount']:.4f}` - Giá mua: `{fmt_price(tx['buy_price'])}`\n"
                msg += f"   {'✅' if tx_profit>=0 else '❌'} LN: `{fmt_price(tx_profit)}` ({tx_profit_pct:+.2f}%)\n"
        
        # So sánh với giá thị trường
        if abs(sell_price - current_price) > 0.01:
            price_diff = sell_price - current_price
            price_diff_pct = (price_diff / current_price) * 100
            if price_diff > 0:
                msg += f"\n📈 Bán *cao hơn* thị trường: `{fmt_price(abs(price_diff))}` ({price_diff_pct:+.2f}%)"
            else:
                msg += f"\n📉 Bán *thấp hơn* thị trường: `{fmt_price(abs(price_diff))}` ({price_diff_pct:+.2f}%)"
        
        msg += f"{owner_info}\n\n🕐 {format_vn_time()}"
        
        await update.message.reply_text(msg, parse_mode='Markdown')
        
        # Ghi log giao dịch
        logger.info(f"💰 SELL: User {target_user_id} bán {sell_amount} {symbol} @ {sell_price}, profit: {profit}")

    @auto_update_user
    @require_permission('view')
    async def sells_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xem lịch sử bán: /sells"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        sells = get_sell_history(user_id, 20)
        
        if not sells:
            await update.message.reply_text("📭 Chưa có lịch sử bán nào!")
            return
        
        msg = "📋 *LỊCH SỬ BÁN*\n━━━━━━━━━━━━━━━━\n\n"
        for sell in sells:
            sell_id, symbol, amount, sell_price, buy_price, profit, profit_pct, sell_date, created = sell
            emoji = "✅" if profit >= 0 else "❌"
            msg += f"{emoji} *#{sell_id}* {sell_date}: {symbol}\n"
            msg += f"   SL: `{amount:.4f}` @ `{fmt_price(sell_price)}`\n"
            msg += f"   LN: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n\n"
        
        msg += f"🕐 {format_vn_time_short()}"
        
        # Thêm nút xem chi tiết
        keyboard = []
        row = []
        for sell in sells[:5]:
            sell_id = sell[0]
            row.append(InlineKeyboardButton(f"#{sell_id}", callback_data=f"sell_detail_{sell_id}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
    
    @auto_update_user
    @require_permission('delete')
    async def delete_sell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xóa lịch sử bán: /delsell [id]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        # Nếu không có tham số, hiển thị danh sách để chọn
        if not ctx.args:
            sells = get_sell_history(user_id, 10)
            if not sells:
                await update.message.reply_text("📭 Không có lịch sử bán nào để xóa!")
                return
            
            msg = "🗑 *CHỌN LỆNH BÁN CẦN XÓA*\n━━━━━━━━━━━━━━━━\n\n"
            keyboard = []
            row = []
            
            for sell in sells:
                sell_id, symbol, amount, sell_price, buy_price, profit, profit_pct, sell_date, created = sell
                emoji = "✅" if profit >= 0 else "❌"
                msg += f"• {emoji} #{sell_id} {sell_date}: {symbol} {amount:.4f} @ {fmt_price(sell_price)} ({profit_pct:+.2f}%)\n"
                
                row.append(InlineKeyboardButton(f"🗑 #{sell_id}", callback_data=f"del_sell_{sell_id}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            
            if row:
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
            
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Nếu có tham số ID
        try:
            sell_id = int(ctx.args[0])
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ! Vui lòng nhập số.")
            return
        
        # Kiểm tra lệnh bán có tồn tại không
        sell = get_sell_detail(sell_id, user_id)
        if not sell:
            await update.message.reply_text(f"❌ Không tìm thấy lệnh bán #{sell_id}")
            return
        
        # Hỏi xác nhận
        id, user, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_pct, sell_date, created = sell
        emoji = "✅" if profit >= 0 else "❌"
        
        msg = (f"⚠️ *XÁC NHẬN XÓA LỆNH BÁN*\n━━━━━━━━━━━━━━━━\n\n"
               f"• ID: #{sell_id}\n"
               f"• Coin: {symbol}\n"
               f"• Ngày bán: {sell_date}\n"
               f"• Số lượng: {amount:.4f}\n"
               f"• Giá bán: {fmt_price(sell_price)}\n"
               f"• Giá vốn: {fmt_price(buy_price)}\n"
               f"• {emoji} Lợi nhuận: {fmt_price(profit)} ({profit_pct:+.2f}%)\n\n"
               f"Bạn có chắc chắn muốn xóa?")
        
        keyboard = [[
            InlineKeyboardButton("✅ Xác nhận", callback_data=f"confirm_del_sell_{sell_id}"),
            InlineKeyboardButton("❌ Hủy", callback_data="cancel_del_sell")
        ]]
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
    
    
    @auto_update_user
    @require_permission('edit')
    async def edit_sell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Sửa lịch sử bán: /editsell [id] [số lượng] [giá]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        if len(ctx.args) < 1:
            # Hiển thị danh sách để chọn
            sells = get_sell_history(user_id, 10)
            if not sells:
                await update.message.reply_text("📭 Không có lịch sử bán nào để sửa!")
                return
            
            msg = "✏️ *CHỌN LỆNH BÁN CẦN SỬA*\n━━━━━━━━━━━━━━━━\n\n"
            keyboard = []
            row = []
            
            for sell in sells:
                sell_id, symbol, amount, sell_price, buy_price, profit, profit_pct, sell_date, created = sell
                msg += f"• #{sell_id} {sell_date}: {symbol} {amount:.4f} @ {fmt_price(sell_price)} ({profit_pct:+.2f}%)\n"
                
                row.append(InlineKeyboardButton(f"✏️ #{sell_id}", callback_data=f"edit_sell_{sell_id}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            
            if row:
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
            
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        if len(ctx.args) == 1:
            # Xem chi tiết
            try:
                sell_id = int(ctx.args[0])
                sell = get_sell_detail(sell_id, user_id)
                if not sell:
                    await update.message.reply_text(f"❌ Không tìm thấy lệnh bán #{sell_id}")
                    return
                
                id, user, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_pct, sell_date, created = sell
                
                msg = (f"📝 *LỆNH BÁN #{sell_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📅 Ngày bán: {sell_date}\n"
                       f"📊 Số lượng: `{amount:.4f}`\n"
                       f"💰 Giá bán: `{fmt_price(sell_price)}`\n"
                       f"💵 Giá vốn: `{fmt_price(buy_price)}`\n"
                       f"💎 Giá trị bán: `{fmt_price(total_sold)}`\n"
                       f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n\n"
                       f"*Sửa:* `/editsell {sell_id} [sl] [giá]`\n"
                       f"*Xóa:* `/delsell {sell_id}`\n\n"
                       f"🕐 {format_vn_time()}")
                
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        elif len(ctx.args) == 3:
            # Thực hiện sửa
            try:
                sell_id = int(ctx.args[0])
                new_amount = float(ctx.args[1])
                new_price = float(ctx.args[2])
                
                if new_amount <= 0 or new_price <= 0:
                    await update.message.reply_text("❌ Số lượng và giá phải > 0")
                    return
                
                success, message = update_sell_history(sell_id, user_id, new_amount, new_price)
                
                if success:
                    sell = get_sell_detail(sell_id, user_id)
                    if sell:
                        id, user, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_pct, sell_date, created = sell
                        msg = (f"✅ *ĐÃ SỬA LỆNH BÁN #{sell_id}*\n━━━━━━━━━━━━━━━━\n\n"
                               f"*{symbol}*\n"
                               f"📊 SL mới: `{amount:.4f}`\n"
                               f"💰 Giá mới: `{fmt_price(sell_price)}`\n"
                               f"{'✅' if profit>=0 else '❌'} LN: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n\n"
                               f"🕐 {format_vn_time()}")
                        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
                else:
                    await update.message.reply_text(f"❌ {message}")
                    
            except ValueError:
                await update.message.reply_text("❌ /editsell [id] [số lượng] [giá]")

    @auto_update_user
    @require_permission('edit')
    async def addsell_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Thêm lịch sử bán thủ công: /addsell [coin] [sl] [giá bán] [giá vốn] [ngày]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        if len(ctx.args) < 4:
            await update.message.reply_text(
                "❌ *HƯỚNG DẪN THÊM LỊCH SỬ BÁN*\n\n"
                "Cú pháp: `/addsell [coin] [số lượng] [giá bán] [giá vốn] [ngày]`\n\n"
                "*Ví dụ:*\n"
                "• `/addsell BTC 0.5 45000 40000 2024-01-15`\n"
                "• `/addsell ETH 2 2500 2200 2024-01-14`\n"
                "• `/addsell SOL 10 100 120 2024-01-13`\n\n"
                "📌 Nếu không nhập ngày, sẽ lấy ngày hiện tại."
            )
            return
        
        symbol = ctx.args[0].upper()
        try:
            amount = float(ctx.args[1])
            sell_price = float(ctx.args[2])
            buy_price = float(ctx.args[3])
            sell_date = ctx.args[4] if len(ctx.args) > 4 else get_vn_time().strftime("%Y-%m-%d")
        except ValueError:
            await update.message.reply_text("❌ Số liệu không hợp lệ!")
            return
        
        if amount <= 0 or sell_price <= 0 or buy_price <= 0:
            await update.message.reply_text("❌ Số lượng và giá phải > 0")
            return
        
        if add_sell_history_manual(user_id, symbol, amount, sell_price, buy_price, sell_date):
            profit = (sell_price - buy_price) * amount
            profit_pct = ((sell_price - buy_price) / buy_price) * 100
            emoji = "✅" if profit >= 0 else "❌"
            await update.message.reply_text(
                f"✅ *ĐÃ THÊM LỊCH SỬ BÁN*\n━━━━━━━━━━━━━━━━\n\n"
                f"• Coin: {symbol}\n"
                f"• Số lượng: {amount:.4f}\n"
                f"• Giá bán: {fmt_price(sell_price)}\n"
                f"• Giá vốn: {fmt_price(buy_price)}\n"
                f"• {emoji} Lợi nhuận: {fmt_price(profit)} ({profit_pct:+.2f}%)\n"
                f"• Ngày bán: {sell_date}\n\n"
                f"🕐 {format_vn_time()}"
            )
        else:
            await update.message.reply_text("❌ Lỗi khi thêm lịch sử bán!")
        
    @auto_update_user
    @require_permission('edit')
    async def edit_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Xác định user_id thực sự cần sửa
        chat_type = update.effective_chat.type
        current_user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        if chat_type == 'private':
            # Private chat: sửa dữ liệu của chính mình
            target_user_id = current_user_id
            logger.info(f"💬 PRIVATE: sửa giao dịch cho user {target_user_id}")
        else:
            # Group chat: sửa dữ liệu của chủ sở hữu (nếu có quyền)
            target_user_id = ctx.bot_data.get('effective_user_id', current_user_id)
            logger.info(f"👥 GROUP: sửa giao dịch cho owner {target_user_id}")
        
        logger.info(f"✏️ edit_command: target_user_id={target_user_id}, current_user={current_user_id}")
        
        # Kiểm tra quyền admin trong group
        is_admin = False
        if chat_type in ['group', 'supergroup']:
            is_admin = check_permission(chat_id, current_user_id, 'edit') or \
                       check_permission(chat_id, current_user_id, 'delete') or \
                       check_permission(chat_id, current_user_id, 'manage')
        
        logger.info(f"🔑 is_admin: {is_admin}")
        
        # Nếu không có tham số, hiển thị danh sách giao dịch
        if not ctx.args:
            # Lấy danh sách giao dịch của target_user
            transactions = get_transaction_detail(target_user_id)
            
            if not transactions:
                await update.message.reply_text("📭 Danh mục trống!")
                return
    
            msg = "📝 *CHỌN GIAO DỊCH*\n━━━━━━━━━━━━━━━━\n\n"
            keyboard = []
            row = []
    
            for i, tx in enumerate(transactions, 1):
                tx_id, symbol, amount, price, date, total = tx
                short_date = date.split()[0] if date else "N/A"
                msg += f"*{i}.* #{tx_id}: {symbol} - {amount:.4f} @ {fmt_price(price)} - {short_date}\n"
    
                row.append(InlineKeyboardButton(f"✏️ #{tx_id}", callback_data=f"edit_{tx_id}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
    
            if row:
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
    
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, 
                                          reply_markup=InlineKeyboardMarkup(keyboard))
            return
    
        # Xem chi tiết 1 giao dịch
        if len(ctx.args) == 1:
            try:
                tx_id = int(ctx.args[0])
                
                # Lấy chi tiết giao dịch từ database
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost, user_id 
                            FROM portfolio WHERE id = ?''', (tx_id,))
                tx = c.fetchone()
                conn.close()
                
                if not tx:
                    await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
                    return
                
                tx_id, symbol, amount, price, date, total, tx_owner_id = tx
                
                # Kiểm tra quyền xem
                if tx_owner_id != target_user_id and not is_admin:
                    await update.message.reply_text("❌ Bạn không có quyền xem giao dịch này!")
                    return
                
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else 0
                profit = (current_price - price) * amount if current_price else 0
                profit_percent = ((current_price - price) / price) * 100 if price and current_price else 0
                
                msg = (f"📝 *GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📅 Ngày mua: {date}\n"
                       f"📊 Số lượng: `{amount:.4f}`\n"
                       f"💰 Giá mua: `{fmt_price(price)}`\n"
                       f"💵 Tổng vốn: `{fmt_price(total)}`\n"
                       f"📈 Giá hiện tại: `{fmt_price(current_price)}`\n"
                       f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n")
                
                # Thêm hướng dẫn sửa/xóa
                if tx_owner_id == target_user_id or is_admin:
                    msg += f"*Sửa:* `/edit {tx_id} [sl] [giá]`\n"
                    msg += f"*Xóa:* `/del {tx_id}`\n\n"
                else:
                    msg += f"*Chỉ xem, không được sửa/xóa*\n\n"
                
                msg += f"🕐 {format_vn_time()}"
                
                # Tạo keyboard
                keyboard = []
                if tx_owner_id == target_user_id or is_admin:
                    keyboard.append([
                        InlineKeyboardButton("✏️ Sửa", callback_data=f"edit_{tx_id}"),
                        InlineKeyboardButton("🗑 Xóa", callback_data=f"del_{tx_id}")
                    ])
                keyboard.append([InlineKeyboardButton("🔙 Về danh sách", callback_data="edit_transactions")])
                
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, 
                                              reply_markup=InlineKeyboardMarkup(keyboard))
                                              
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ")
        
        # Sửa giao dịch
        elif len(ctx.args) == 3:
            try:
                tx_id = int(ctx.args[0])
                new_amount = float(ctx.args[1])
                new_price = float(ctx.args[2])
                
                if new_amount <= 0 or new_price <= 0:
                    await update.message.reply_text("❌ SL và giá phải > 0")
                    return
                
                # Kiểm tra giao dịch có tồn tại không
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT user_id FROM portfolio WHERE id = ?''', (tx_id,))
                result = c.fetchone()
                
                if not result:
                    await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
                    conn.close()
                    return
                
                tx_owner_id = result[0]
                
                # Kiểm tra quyền sửa
                can_edit = False
                if tx_owner_id == target_user_id:
                    can_edit = True
                    logger.info(f"✅ User {current_user_id} là chủ, được sửa #{tx_id}")
                elif is_admin:
                    can_edit = True
                    logger.info(f"✅ Admin {current_user_id} có quyền, được sửa #{tx_id}")
                else:
                    logger.info(f"❌ User {current_user_id} không có quyền sửa #{tx_id}")
                
                if not can_edit:
                    await update.message.reply_text("❌ Bạn không có quyền sửa giao dịch này!")
                    conn.close()
                    return
                
                # Thực hiện sửa
                new_total = new_amount * new_price
                c.execute('''UPDATE portfolio SET amount = ?, buy_price = ?, total_cost = ? 
                            WHERE id = ?''', (new_amount, new_price, new_total, tx_id))
                conn.commit()
                affected = c.rowcount
                conn.close()
                
                if affected > 0:
                    msg = (f"✅ *ĐÃ SỬA GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                           f"📊 SL mới: `{new_amount:.4f}`\n"
                           f"💰 Giá mới: `{fmt_price(new_price)}`\n"
                           f"💵 Vốn mới: `{fmt_price(new_total)}`\n\n"
                           f"🕐 {format_vn_time()}")
                    await update.message.reply_text(msg, parse_mode='Markdown')
                else:
                    await update.message.reply_text(f"❌ Không thể sửa giao dịch #{tx_id}")
                    
            except ValueError:
                await update.message.reply_text("❌ /edit [id] [sl] [giá]")
        else:
            await update.message.reply_text("❌ /edit - Xem DS\n/edit [id] - Xem chi tiết\n/edit [id] [sl] [giá] - Sửa")

    @auto_update_user
    @require_permission('delete')
    async def delete_tx_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Xác định user_id thực sự cần xóa
        chat_type = update.effective_chat.type
        current_user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        if chat_type == 'private':
            # Private chat: xóa dữ liệu của chính mình
            target_user_id = current_user_id
            logger.info(f"💬 PRIVATE: xóa giao dịch cho user {target_user_id}")
        else:
            # Group chat: xóa dữ liệu của chủ sở hữu (nếu có quyền)
            target_user_id = ctx.bot_data.get('effective_user_id', current_user_id)
            logger.info(f"👥 GROUP: xóa giao dịch cho owner {target_user_id}")
        
        logger.info(f"🗑 delete_tx_command: target_user_id={target_user_id}, current_user={current_user_id}")
        
        # Kiểm tra quyền admin trong group
        is_admin = False
        if chat_type in ['group', 'supergroup']:
            is_admin = check_permission(chat_id, current_user_id, 'delete') or \
                       check_permission(chat_id, current_user_id, 'manage')
        
        if not ctx.args:
            # Hiển thị danh sách giao dịch để chọn xóa
            transactions = get_transaction_detail(target_user_id)
            
            if not transactions:
                await update.message.reply_text("📭 Danh mục trống!")
                return
    
            msg = "🗑 *CHỌN GIAO DỊCH CẦN XÓA*\n━━━━━━━━━━━━━━━━\n\n"
            keyboard = []
            row = []
    
            for i, tx in enumerate(transactions, 1):
                tx_id, symbol, amount, price, date, total = tx
                short_date = date.split()[0] if date else "N/A"
                msg += f"*{i}.* #{tx_id}: {symbol} - {amount:.4f} @ {fmt_price(price)} - {short_date}\n"
    
                row.append(InlineKeyboardButton(f"🗑 #{tx_id}", callback_data=f"del_{tx_id}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
    
            if row:
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")])
    
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, 
                                          reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            tx_id = int(ctx.args[0])
            
            # Kiểm tra giao dịch có tồn tại không
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT user_id FROM portfolio WHERE id = ?''', (tx_id,))
            result = c.fetchone()
            conn.close()
            
            if not result:
                await update.message.reply_text(f"❌ Không tìm thấy giao dịch #{tx_id}")
                return
            
            tx_owner_id = result[0]
            
            # Kiểm tra quyền xóa
            can_delete = False
            
            if tx_owner_id == target_user_id:
                can_delete = True
                logger.info(f"✅ User {current_user_id} là chủ, được xóa #{tx_id}")
            elif is_admin:
                can_delete = True
                logger.info(f"✅ Admin {current_user_id} có quyền delete, được xóa #{tx_id}")
            else:
                logger.info(f"❌ User {current_user_id} không có quyền xóa #{tx_id}")
            
            if not can_delete:
                await update.message.reply_text("❌ Bạn không có quyền xóa giao dịch này!")
                return
            
            # Hỏi xác nhận
            keyboard = [[InlineKeyboardButton("✅ Có", callback_data=f"confirm_del_{tx_id}"),
                         InlineKeyboardButton("❌ Không", callback_data="edit_transactions")]]
            
            await update.message.reply_text(f"⚠️ *Xác nhận xóa giao dịch #{tx_id}?*\n\n🕐 {format_vn_time_short()}", 
                                           parse_mode=ParseMode.MARKDOWN,
                                           reply_markup=InlineKeyboardMarkup(keyboard))
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ")
        
    @auto_update_user
    @require_permission('view')
    async def alert_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
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
        
        price_data = get_price(symbol)
        if not price_data:
            return await update.message.reply_text(f"❌ Không tìm thấy coin *{symbol}*", parse_mode='Markdown')
        
        if add_alert(owner_id, symbol, target_price, condition):
            msg = (f"✅ *ĐÃ TẠO CẢNH BÁO*\n━━━━━━━━━━━━━━━━\n\n"
                   f"• Coin: *{symbol}*\n"
                   f"• Mốc giá: `{fmt_price(target_price)}`\n"
                   f"• Giá hiện tại: `{fmt_price(price_data['p'])}`\n"
                   f"• Điều kiện: {'📈 Lên trên' if condition == 'above' else '📉 Xuống dưới'}\n\n"
                   f"🕐 {format_vn_time()}")
            await update.message.reply_text(msg, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Lỗi khi tạo cảnh báo!")

    @auto_update_user
    @require_permission('view')
    async def alerts_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = ctx.bot_data.get('effective_user_id', update.effective_user.id)
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

    @auto_update_user
    @require_permission('view')
    async def stats_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        msg = await update.message.reply_text("🔄 Đang tính toán thống kê...")
        
        stats = get_portfolio_stats(uid)
        
        if not stats:
            await msg.edit_text("📭 Danh mục trống!")
            return
        
        stats_msg = (f"📊 *THỐNG KÊ DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                     f"*TỔNG QUAN*\n"
                     f"• Vốn: `{fmt_price(stats['total_invest'])}`\n"
                     f"• Giá trị: `{fmt_price(stats['total_value'])}`\n"
                     f"• Lợi nhuận: `{fmt_price(stats['total_profit'])}`\n"
                     f"• Tỷ suất: `{stats['total_profit_percent']:+.2f}%`\n\n"
                     f"*📈 TOP COIN LỜI NHẤT*\n")
        
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

    def generate_detailed_portfolio_csv(user_id):
        """
        Tạo file CSV chi tiết cho portfolio đầu tư
        """
        try:
            import csv
            import io
            from datetime import datetime
            
            # Tạo buffer để chứa CSV
            output = io.StringIO()
            writer = csv.writer(output)
            
            # =========================================
            # 1. THÔNG TIN TỔNG QUAN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['BÁO CÁO ĐẦU TƯ CHI TIẾT'])
            writer.writerow(['='*80])
            writer.writerow(['Ngày xuất:', format_vn_time()])
            writer.writerow(['User ID:', user_id])
            writer.writerow([])
            
            # Lấy dữ liệu
            transactions = get_transaction_detail(user_id)
            if not transactions:
                writer.writerow(['KHÔNG CÓ DỮ LIỆU'])
                return output.getvalue()
            
            # =========================================
            # 2. DANH SÁCH GIAO DỊCH CHI TIẾT
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH GIAO DỊCH'])
            writer.writerow(['='*80])
            writer.writerow([
                'ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 
                'Ngày mua', 'Tổng vốn (USD)', 'Giá hiện tại (USD)',
                'Giá trị hiện tại (USD)', 'Lợi nhuận (USD)', 'Lợi nhuận (%)', 'Thời gian nắm giữ (ngày)'
            ])
            
            total_invest = 0
            total_current = 0
            all_coins = {}
            
            for tx in transactions:
                tx_id, symbol, amount, buy_price, buy_date, total_cost = tx
                
                # Lấy giá hiện tại
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else buy_price
                current_value = amount * current_price
                profit = current_value - total_cost
                profit_pct = (profit / total_cost * 100) if total_cost > 0 else 0
                
                # Tính thời gian nắm giữ
                try:
                    buy_datetime = datetime.strptime(buy_date, "%Y-%m-%d %H:%M:%S")
                    hold_days = (get_vn_time() - buy_datetime).days
                except:
                    hold_days = 0
                
                # Ghi dòng giao dịch
                writer.writerow([
                    tx_id, symbol, f"{amount:.8f}", f"${buy_price:,.2f}",
                    buy_date[:10], f"${total_cost:,.2f}", f"${current_price:,.2f}",
                    f"${current_value:,.2f}", f"${profit:,.2f}", f"{profit_pct:+.2f}%",
                    hold_days
                ])
                
                # Cộng dồn cho tổng hợp
                total_invest += total_cost
                total_current += current_value
                
                # Tổng hợp theo coin
                if symbol not in all_coins:
                    all_coins[symbol] = {
                        'amount': 0,
                        'invest': 0,
                        'current': 0,
                        'profit': 0,
                        'transactions': 0
                    }
                all_coins[symbol]['amount'] += amount
                all_coins[symbol]['invest'] += total_cost
                all_coins[symbol]['current'] += current_value
                all_coins[symbol]['profit'] += profit
                all_coins[symbol]['transactions'] += 1
            
            writer.writerow([])
            
            # =========================================
            # 3. TỔNG KẾT DANH MỤC
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['TỔNG KẾT DANH MỤC'])
            writer.writerow(['='*80])
            total_profit = total_current - total_invest
            total_profit_pct = (total_profit / total_invest * 100) if total_invest > 0 else 0
            
            writer.writerow(['Tổng vốn đầu tư:', f"${total_invest:,.2f}"])
            writer.writerow(['Tổng giá trị hiện tại:', f"${total_current:,.2f}"])
            writer.writerow(['Tổng lợi nhuận:', f"${total_profit:,.2f}"])
            writer.writerow(['Tỷ suất lợi nhuận:', f"{total_profit_pct:+.2f}%"])
            writer.writerow(['Số loại coin:', len(all_coins)])
            writer.writerow(['Tổng số giao dịch:', len(transactions)])
            writer.writerow([])
            
            # =========================================
            # 4. PHÂN TÍCH THEO TỪNG COIN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH THEO TỪNG COIN'])
            writer.writerow(['='*80])
            writer.writerow([
                'Mã coin', 'Số lượng', 'Tổng vốn', 'Giá trị hiện tại',
                'Lợi nhuận', 'Tỷ suất', 'Số giao dịch', 'Tỷ trọng'
            ])
            
            for symbol, data in all_coins.items():
                weight = (data['current'] / total_current * 100) if total_current > 0 else 0
                profit_pct = (data['profit'] / data['invest'] * 100) if data['invest'] > 0 else 0
                
                writer.writerow([
                    symbol,
                    f"{data['amount']:.8f}",
                    f"${data['invest']:,.2f}",
                    f"${data['current']:,.2f}",
                    f"${data['profit']:,.2f}",
                    f"{profit_pct:+.2f}%",
                    data['transactions'],
                    f"{weight:.1f}%"
                ])
            
            writer.writerow([])
            
            # =========================================
            # 5. TOP COIN LỜI NHẤT
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['TOP 5 COIN LỜI NHẤT'])
            writer.writerow(['='*80])
            
            sorted_coins = sorted(all_coins.items(), key=lambda x: x[1]['profit'], reverse=True)
            writer.writerow(['Mã coin', 'Lợi nhuận', 'Tỷ suất', 'Giá trị'])
            
            count = 0
            for symbol, data in sorted_coins:
                if data['profit'] > 0:
                    count += 1
                    profit_pct = (data['profit'] / data['invest'] * 100) if data['invest'] > 0 else 0
                    writer.writerow([
                        symbol,
                        f"${data['profit']:,.2f}",
                        f"{profit_pct:+.2f}%",
                        f"${data['current']:,.2f}"
                    ])
                    if count >= 5:
                        break
            
            if count == 0:
                writer.writerow(['Không có coin lời'])
            
            writer.writerow([])
            
            # =========================================
            # 6. TOP COIN LỖ NHẤT
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['TOP 5 COIN LỖ NHẤT'])
            writer.writerow(['='*80])
            
            writer.writerow(['Mã coin', 'Lỗ', 'Tỷ suất', 'Giá trị'])
            count = 0
            for symbol, data in reversed(sorted_coins):
                if data['profit'] < 0:
                    count += 1
                    profit_pct = (data['profit'] / data['invest'] * 100) if data['invest'] > 0 else 0
                    writer.writerow([
                        symbol,
                        f"${data['profit']:,.2f}",
                        f"{profit_pct:+.2f}%",
                        f"${data['current']:,.2f}"
                    ])
                    if count >= 5:
                        break
            
            if count == 0:
                writer.writerow(['Không có coin lỗ'])
            
            writer.writerow([])
            
            # =========================================
            # 7. PHÂN TÍCH THEO THỜI GIAN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH THEO THỜI GIAN'])
            writer.writerow(['='*80])
            
            # Phân tích theo tháng
            monthly_data = {}
            for tx in transactions:
                tx_id, symbol, amount, buy_price, buy_date, total_cost = tx
                month = buy_date[:7]  # YYYY-MM
                
                if month not in monthly_data:
                    monthly_data[month] = {'invest': 0, 'transactions': 0}
                monthly_data[month]['invest'] += total_cost
                monthly_data[month]['transactions'] += 1
            
            writer.writerow(['Vốn đầu tư theo tháng:'])
            writer.writerow(['Tháng', 'Số tiền đầu tư', 'Số giao dịch'])
            for month in sorted(monthly_data.keys()):
                writer.writerow([
                    month,
                    f"${monthly_data[month]['invest']:,.2f}",
                    monthly_data[month]['transactions']
                ])
            
            writer.writerow([])
            
            # =========================================
            # 8. DỰ BÁO LỢI NHUẬN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DỰ BÁO LỢI NHUẬN (KỊCH BẢN)'])
            writer.writerow(['='*80])
            writer.writerow(['*Dự báo dựa trên giả định thị trường tăng/giảm*'])
            writer.writerow([])
            
            scenarios = [-30, -20, -10, 10, 20, 30]
            writer.writerow(['Kịch bản', 'Giá trị danh mục', 'Lợi nhuận', 'Tỷ suất'])
            
            for scenario in scenarios:
                multiplier = 1 + (scenario / 100)
                projected_value = total_current * multiplier
                projected_profit = projected_value - total_invest
                projected_pct = (projected_profit / total_invest * 100) if total_invest > 0 else 0
                
                emoji = "📈" if scenario > 0 else "📉" if scenario < 0 else "➡️"
                writer.writerow([
                    f"{emoji} {scenario:+.0f}%",
                    f"${projected_value:,.2f}",
                    f"${projected_profit:,.2f}",
                    f"{projected_pct:+.2f}%"
                ])
            
            writer.writerow([])
            
            # =========================================
            # 9. ĐÁNH GIÁ & KHUYẾN NGHỊ
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['ĐÁNH GIÁ & KHUYẾN NGHỊ'])
            writer.writerow(['='*80])
            
            # Đánh giá dựa trên lợi nhuận
            if total_profit > 0:
                if total_profit_pct > 50:
                    rating = "🚀 XUẤT SẮC - Lợi nhuận rất cao"
                elif total_profit_pct > 20:
                    rating = "✅ TỐT - Lợi nhuận khả quan"
                elif total_profit_pct > 0:
                    rating = "📊 TẠM ỔN - Có lợi nhuận nhẹ"
            else:
                if total_profit_pct < -50:
                    rating = "🔴 RỦI RO CAO - Lỗ nặng"
                elif total_profit_pct < -20:
                    rating = "⚠️ CẦN XEM XÉT - Lỗ đáng kể"
                else:
                    rating = "📉 ĐANG LỖ NHẸ"
            
            writer.writerow(['Đánh giá tổng quan:', rating])
            writer.writerow([])
            
            # Khuyến nghị
            writer.writerow(['KHUYẾN NGHỊ:'])
            
            # Coin lời nhiều nhất
            if sorted_coins and sorted_coins[0][1]['profit'] > 0:
                best_coin = sorted_coins[0]
                writer.writerow([f'• {best_coin[0]} đang lời nhất (${best_coin[1]["profit"]:,.2f}) - Có thể cân nhắc chốt lời một phần'])
            
            # Coin lỗ nhiều nhất
            if sorted_coins and sorted_coins[-1][1]['profit'] < 0:
                worst_coin = sorted_coins[-1]
                writer.writerow([f'• {worst_coin[0]} đang lỗ nhất (${abs(worst_coin[1]["profit"]):,.2f}) - Cân nhắc cắt lỗ hoặc đợi hồi'])
            
            # Đa dạng hóa
            if len(all_coins) < 3:
                writer.writerow(['• Danh mục chưa đa dạng, nên đầu tư thêm các coin khác để phân tán rủi ro'])
            elif len(all_coins) > 10:
                writer.writerow(['• Danh mục khá đa dạng, theo dõi sát các coin nhỏ'])
            
            # Tỷ trọng
            for symbol, data in all_coins.items():
                weight = (data['current'] / total_current * 100) if total_current > 0 else 0
                if weight > 50:
                    writer.writerow([f'• {symbol} chiếm {weight:.1f}% danh mục - Rủi ro tập trung cao'])
                elif weight > 30:
                    writer.writerow([f'• {symbol} chiếm {weight:.1f}% danh mục - Tỷ trọng khá lớn'])
            
            writer.writerow([])
            writer.writerow(['='*80])
            writer.writerow(['KẾT THÚC BÁO CÁO'])
            writer.writerow(['='*80])
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo CSV chi tiết: {e}")
            import traceback
            traceback.print_exc()
            return None

    def generate_detailed_expense_csv(user_id):
        """
        Tạo file CSV chi tiết cho quản lý thu chi
        Bao gồm: thu nhập, chi tiêu, phân tích danh mục, cân đối
        """
        try:
            import csv
            import io
            from datetime import datetime
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # =========================================
            # 1. THÔNG TIN TỔNG QUAN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['BÁO CÁO THU CHI CHI TIẾT'])
            writer.writerow(['='*80])
            writer.writerow(['Ngày xuất:', format_vn_time()])
            writer.writerow(['User ID:', user_id])
            writer.writerow([])
            
            # Lấy dữ liệu
            incomes = get_recent_incomes(user_id, 5000)  # Lấy tối đa 5000 giao dịch
            expenses = get_recent_expenses(user_id, 5000)
            
            if not incomes and not expenses:
                writer.writerow(['KHÔNG CÓ DỮ LIỆU'])
                return output.getvalue()
            
            # =========================================
            # 2. DANH SÁCH THU NHẬP
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH THU NHẬP'])
            writer.writerow(['='*80])
            
            if incomes:
                writer.writerow(['ID', 'Ngày', 'Nguồn', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                total_income = {}
                income_by_month = {}
                income_by_source = {}
                
                for inc in incomes:
                    inc_id, amount, source, note, date, currency = inc
                    writer.writerow([inc_id, date, source, f"{amount:,.0f}", currency, note or ''])
                    
                    # Tổng theo loại tiền
                    if currency not in total_income:
                        total_income[currency] = 0
                    total_income[currency] += amount
                    
                    # Tổng theo tháng
                    month = date[:7]  # YYYY-MM
                    if month not in income_by_month:
                        income_by_month[month] = {}
                    if currency not in income_by_month[month]:
                        income_by_month[month][currency] = 0
                    income_by_month[month][currency] += amount
                    
                    # Tổng theo nguồn
                    key = f"{source}_{currency}"
                    if key not in income_by_source:
                        income_by_source[key] = {'source': source, 'currency': currency, 'total': 0, 'count': 0}
                    income_by_source[key]['total'] += amount
                    income_by_source[key]['count'] += 1
                
                writer.writerow([])
                writer.writerow(['TỔNG THU THEO LOẠI TIỀN:'])
                for currency, total in total_income.items():
                    writer.writerow([currency, f"{total:,.0f}"])
                
                writer.writerow([])
                writer.writerow(['THU NHẬP THEO NGUỒN:'])
                writer.writerow(['Nguồn', 'Loại tiền', 'Tổng', 'Số lần'])
                for key, data in income_by_source.items():
                    writer.writerow([data['source'], data['currency'], f"{data['total']:,.0f}", data['count']])
            else:
                writer.writerow(['Không có dữ liệu thu nhập'])
            
            writer.writerow([])
            
            # =========================================
            # 3. DANH SÁCH CHI TIÊU
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH CHI TIÊU'])
            writer.writerow(['='*80])
            
            if expenses:
                writer.writerow(['ID', 'Ngày', 'Danh mục', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                total_expense = {}
                expense_by_month = {}
                category_stats = {}
                
                for exp in expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    writer.writerow([exp_id, date, cat_name, f"{amount:,.0f}", currency, note or ''])
                    
                    # Tổng theo loại tiền
                    if currency not in total_expense:
                        total_expense[currency] = 0
                    total_expense[currency] += amount
                    
                    # Tổng theo tháng
                    month = date[:7]
                    if month not in expense_by_month:
                        expense_by_month[month] = {}
                    if currency not in expense_by_month[month]:
                        expense_by_month[month][currency] = 0
                    expense_by_month[month][currency] += amount
                    
                    # Thống kê theo danh mục
                    key = f"{cat_name}_{currency}"
                    if key not in category_stats:
                        # Lấy budget từ database
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute('''SELECT budget FROM expense_categories WHERE name = ? AND user_id = ?''', (cat_name, user_id))
                        budget = c.fetchone()
                        conn.close()
                        
                        category_stats[key] = {
                            'category': cat_name,
                            'currency': currency,
                            'total': 0,
                            'count': 0,
                            'budget': budget[0] if budget else 0
                        }
                    category_stats[key]['total'] += amount
                    category_stats[key]['count'] += 1
                
                writer.writerow([])
                writer.writerow(['TỔNG CHI THEO LOẠI TIỀN:'])
                for currency, total in total_expense.items():
                    writer.writerow([currency, f"{total:,.0f}"])
            else:
                writer.writerow(['Không có dữ liệu chi tiêu'])
            
            writer.writerow([])
            
            # =========================================
            # 4. PHÂN TÍCH CHI TIÊU THEO DANH MỤC
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH CHI TIÊU THEO DANH MỤC'])
            writer.writerow(['='*80])
            
            if category_stats:
                writer.writerow(['Danh mục', 'Loại tiền', 'Tổng chi', 'Số lần', 'Budget', '% Budget', 'Đánh giá'])
                
                # Sắp xếp theo tổng chi giảm dần
                sorted_cats = sorted(category_stats.items(), key=lambda x: x[1]['total'], reverse=True)
                
                for key, data in sorted_cats:
                    percent = (data['total'] / data['budget'] * 100) if data['budget'] > 0 else 0
                    
                    if data['budget'] > 0:
                        if percent > 100:
                            status = "🔴 VƯỢT BUDGET"
                        elif percent > 80:
                            status = "⚠️ GẦN HẾT"
                        else:
                            status = "✅ TRONG BUDGET"
                    else:
                        status = "📊 KHÔNG BUDGET"
                    
                    writer.writerow([
                        data['category'],
                        data['currency'],
                        f"{data['total']:,.0f}",
                        data['count'],
                        f"{data['budget']:,.0f}",
                        f"{percent:.1f}%",
                        status
                    ])
            else:
                writer.writerow(['Không có dữ liệu chi tiêu'])
            
            writer.writerow([])
            
            # =========================================
            # 5. CÂN ĐỐI THU CHI
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['CÂN ĐỐI THU CHI'])
            writer.writerow(['='*80])
            
            all_currencies = set(list(total_income.keys()) + list(total_expense.keys()))
            
            writer.writerow(['Loại tiền', 'Tổng thu', 'Tổng chi', 'Cân đối', 'Đánh giá'])
            for currency in sorted(all_currencies):
                income = total_income.get(currency, 0)
                expense = total_expense.get(currency, 0)
                balance = income - expense
                
                if balance > 0:
                    eval_ = "✅ TIẾT KIỆM"
                elif balance < 0:
                    eval_ = "❌ THÂM HỤT"
                else:
                    eval_ = "➖ CÂN BẰNG"
                
                writer.writerow([
                    currency,
                    f"{income:,.0f}",
                    f"{expense:,.0f}",
                    f"{balance:,.0f}",
                    eval_
                ])
            
            writer.writerow([])
            
            # =========================================
            # 6. PHÂN TÍCH THEO THÁNG
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH THEO THÁNG'])
            writer.writerow(['='*80])
            
            all_months = sorted(set(list(income_by_month.keys()) + list(expense_by_month.keys())))
            
            writer.writerow(['Tháng', 'Thu (VND)', 'Chi (VND)', 'Cân đối', 'Thu (USD)', 'Chi (USD)', 'Cân đối'])
            
            for month in all_months:
                # VND
                income_vnd = income_by_month.get(month, {}).get('VND', 0)
                expense_vnd = expense_by_month.get(month, {}).get('VND', 0)
                balance_vnd = income_vnd - expense_vnd
                
                # USD
                income_usd = income_by_month.get(month, {}).get('USD', 0)
                expense_usd = expense_by_month.get(month, {}).get('USD', 0)
                balance_usd = income_usd - expense_usd
                
                # USDT (nếu có)
                income_usdt = income_by_month.get(month, {}).get('USDT', 0)
                expense_usdt = expense_by_month.get(month, {}).get('USDT', 0)
                balance_usdt = income_usdt - expense_usdt
                
                # KHR (nếu có)
                income_khr = income_by_month.get(month, {}).get('KHR', 0)
                expense_khr = expense_by_month.get(month, {}).get('KHR', 0)
                balance_khr = income_khr - expense_khr
                
                # LKR (nếu có)
                income_lkr = income_by_month.get(month, {}).get('LKR', 0)
                expense_lkr = expense_by_month.get(month, {}).get('LKR', 0)
                balance_lkr = income_lkr - expense_lkr
                
                # Chỉ hiển thị các cột có dữ liệu
                row = [month, f"{income_vnd:,.0f}", f"{expense_vnd:,.0f}", f"{balance_vnd:,.0f}"]
                
                if income_usd > 0 or expense_usd > 0:
                    row.extend([f"{income_usd:,.2f}", f"{expense_usd:,.2f}", f"{balance_usd:,.2f}"])
                else:
                    row.extend(['0', '0', '0'])
                    
                if income_usdt > 0 or expense_usdt > 0:
                    row.extend([f"{income_usdt:,.2f}", f"{expense_usdt:,.2f}", f"{balance_usdt:,.2f}"])
                
                if income_khr > 0 or expense_khr > 0:
                    row.extend([f"{income_khr:,.0f}", f"{expense_khr:,.0f}", f"{balance_khr:,.0f}"])
                
                if income_lkr > 0 or expense_lkr > 0:
                    row.extend([f"{income_lkr:,.0f}", f"{expense_lkr:,.0f}", f"{balance_lkr:,.0f}"])
                
                writer.writerow(row)
            
            writer.writerow([])
            
            # =========================================
            # 7. THỐNG KÊ TỔNG HỢP
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['THỐNG KÊ TỔNG HỢP'])
            writer.writerow(['='*80])
            
            total_income_all = sum(total_income.values())
            total_expense_all = sum(total_expense.values())
            
            writer.writerow(['Tổng số khoản thu:', len(incomes)])
            writer.writerow(['Tổng số khoản chi:', len(expenses)])
            writer.writerow(['Tổng thu (quy đổi VND):', f"{total_income_all:,.0f} VND"])
            writer.writerow(['Tổng chi (quy đổi VND):', f"{total_expense_all:,.0f} VND"])
            writer.writerow(['Tổng cân đối:', f"{total_income_all - total_expense_all:,.0f} VND"])
            
            # Trung bình chi tiêu
            if expenses:
                avg_expense = total_expense_all / len(expenses)
                writer.writerow(['Trung bình mỗi khoản chi:', f"{avg_expense:,.0f} VND"])
            
            # Tỷ lệ chi theo danh mục
            if total_expense_all > 0 and category_stats:
                writer.writerow([])
                writer.writerow(['TỶ LỆ CHI THEO DANH MỤC:'])
                
                for key, data in sorted_cats[:5]:  # Top 5 danh mục chi nhiều nhất
                    percent = (data['total'] / total_expense_all * 100)
                    writer.writerow([f"• {data['category']}: {percent:.1f}% ({data['total']:,.0f} {data['currency']})"])
            
            writer.writerow([])
            
            # =========================================
            # 8. ĐÁNH GIÁ & KHUYẾN NGHỊ
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['ĐÁNH GIÁ & KHUYẾN NGHỊ'])
            writer.writerow(['='*80])
            
            total_balance = total_income_all - total_expense_all
            
            # Đánh giá tổng quan
            if total_balance > 0:
                savings_rate = (total_balance / total_income_all * 100) if total_income_all > 0 else 0
                if savings_rate > 30:
                    rating = "🚀 XUẤT SẮC - Tiết kiệm rất tốt"
                elif savings_rate > 15:
                    rating = "✅ TỐT - Có quỹ tiết kiệm"
                else:
                    rating = "📊 ỔN ĐỊNH - Cân đối tốt"
            else:
                deficit_rate = (abs(total_balance) / total_expense_all * 100) if total_expense_all > 0 else 0
                if deficit_rate > 30:
                    rating = "🔴 NGUY CẤP - Chi tiêu vượt quá nhiều"
                elif deficit_rate > 15:
                    rating = "⚠️ CẢNH BÁO - Đang chi vượt thu"
                else:
                    rating = "📉 THÂM HỤT NHẸ - Cần điều chỉnh"
            
            writer.writerow(['Đánh giá tổng quan:', rating])
            writer.writerow([])
            
            # Khuyến nghị
            writer.writerow(['KHUYẾN NGHỊ:'])
            
            # Danh mục chi nhiều nhất
            if category_stats:
                max_cat = max(category_stats.items(), key=lambda x: x[1]['total'])
                writer.writerow([f'• Danh mục "{max_cat[1]["category"]}" chi nhiều nhất: {max_cat[1]["total"]:,.0f} {max_cat[1]["currency"]}'])
            
            # Danh mục vượt budget
            over_budget = []
            for key, data in category_stats.items():
                if data['budget'] > 0 and data['total'] > data['budget']:
                    over_budget.append(data)
            
            if over_budget:
                writer.writerow([f'• Có {len(over_budget)} danh mục vượt budget:'])
                for data in over_budget[:3]:  # Chỉ hiển thị 3 danh mục vượt nhiều nhất
                    writer.writerow([f'  - {data["category"]}: vượt {data["total"] - data["budget"]:,.0f} {data["currency"]}'])
            else:
                writer.writerow(['• Tốt! Không có danh mục nào vượt budget'])
            
            # Tỷ lệ chi so với thu
            if total_income_all > 0:
                expense_ratio = (total_expense_all / total_income_all * 100)
                if expense_ratio > 90:
                    writer.writerow(['• Cảnh báo: Chi tiêu chiếm >90% thu nhập'])
                elif expense_ratio > 70:
                    writer.writerow(['• Chi tiêu chiếm ~70% thu nhập - Ổn định'])
                else:
                    writer.writerow([f'• Chi tiêu chỉ chiếm {expense_ratio:.1f}% thu nhập - Tốt'])
            
            # Gợi ý tiết kiệm
            if total_balance > 0:
                writer.writerow([f'• Bạn đang tiết kiệm được {total_balance:,.0f} VND - Có thể đầu tư thêm'])
            else:
                need_to_save = abs(total_balance)
                writer.writerow([f'• Cần cắt giảm ~{need_to_save:,.0f} VND để cân bằng thu chi'])
            
            writer.writerow([])
            writer.writerow(['='*80])
            writer.writerow(['KẾT THÚC BÁO CÁO'])
            writer.writerow(['='*80])
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo detailed expense CSV: {e}")
            import traceback
            traceback.print_exc()
            return None

    def generate_expense_master_report(user_id, password=None):
        """
        Tạo báo cáo MASTER cho quản lý chi tiêu
        Bao gồm TẤT CẢ thông tin và được mã hóa
        """
        try:
            import csv
            import io
            from datetime import datetime
            
            # Tạo buffer cho CSV
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            # =========================================
            # 1. THÔNG TIN TỔNG QUAN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['BÁO CÁO THU CHI MASTER'])
            writer.writerow(['='*80])
            writer.writerow(['Ngày xuất:', format_vn_time()])
            writer.writerow(['User ID:', user_id])
            writer.writerow(['Định dạng:', 'CSV MASTER - Bao gồm tất cả phân tích'])
            writer.writerow(['Mã hóa:', 'AES-256' if password else 'Không mã hóa'])
            writer.writerow([])
            
            # Lấy dữ liệu
            incomes = get_recent_incomes(user_id, 5000)  # Lấy nhiều nhất
            expenses = get_recent_expenses(user_id, 5000)
            
            if not incomes and not expenses:
                writer.writerow(['KHÔNG CÓ DỮ LIỆU'])
                csv_content = csv_buffer.getvalue()
                
                # Nếu có password, tạo ZIP
                if password and HAS_PYZIPPER:
                    return create_encrypted_zip(csv_content, f"expense_empty_{user_id}.csv", password)
                return csv_content
            
            # =========================================
            # 2. DANH SÁCH THU NHẬP
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH THU NHẬP'])
            writer.writerow(['='*80])
            
            if incomes:
                writer.writerow(['ID', 'Ngày', 'Nguồn', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                total_income = {}
                income_by_month = {}
                
                for inc in incomes:
                    inc_id, amount, source, note, date, currency = inc
                    writer.writerow([inc_id, date, source, f"{amount:,.0f}", currency, note or ''])
                    
                    # Tổng theo loại tiền
                    if currency not in total_income:
                        total_income[currency] = 0
                    total_income[currency] += amount
                    
                    # Tổng theo tháng
                    month = date[:7]
                    if month not in income_by_month:
                        income_by_month[month] = {}
                    if currency not in income_by_month[month]:
                        income_by_month[month][currency] = 0
                    income_by_month[month][currency] += amount
                
                writer.writerow([])
                writer.writerow(['TỔNG THU THEO LOẠI TIỀN:'])
                for currency, total in total_income.items():
                    writer.writerow([currency, f"{total:,.0f}"])
            else:
                writer.writerow(['Không có dữ liệu thu nhập'])
            
            writer.writerow([])
            
            # =========================================
            # 3. DANH SÁCH CHI TIÊU
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH CHI TIÊU'])
            writer.writerow(['='*80])
            
            if expenses:
                writer.writerow(['ID', 'Ngày', 'Danh mục', 'Số tiền', 'Loại tiền', 'Ghi chú'])
                total_expense = {}
                expense_by_month = {}
                category_stats = {}
                
                for exp in expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    writer.writerow([exp_id, date, cat_name, f"{amount:,.0f}", currency, note or ''])
                    
                    # Tổng theo loại tiền
                    if currency not in total_expense:
                        total_expense[currency] = 0
                    total_expense[currency] += amount
                    
                    # Tổng theo tháng
                    month = date[:7]
                    if month not in expense_by_month:
                        expense_by_month[month] = {}
                    if currency not in expense_by_month[month]:
                        expense_by_month[month][currency] = 0
                    expense_by_month[month][currency] += amount
                    
                    # Thống kê theo danh mục
                    key = f"{cat_name}_{currency}"
                    if key not in category_stats:
                        # Lấy budget từ database
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute('''SELECT budget FROM expense_categories WHERE name = ? AND user_id = ?''', (cat_name, user_id))
                        budget = c.fetchone()
                        conn.close()
                        
                        category_stats[key] = {
                            'category': cat_name,
                            'currency': currency,
                            'total': 0,
                            'count': 0,
                            'budget': budget[0] if budget else 0
                        }
                    category_stats[key]['total'] += amount
                    category_stats[key]['count'] += 1
                
                writer.writerow([])
                writer.writerow(['TỔNG CHI THEO LOẠI TIỀN:'])
                for currency, total in total_expense.items():
                    writer.writerow([currency, f"{total:,.0f}"])
            else:
                writer.writerow(['Không có dữ liệu chi tiêu'])
            
            writer.writerow([])
            
            # =========================================
            # 4. PHÂN TÍCH THEO DANH MỤC
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH CHI TIÊU THEO DANH MỤC'])
            writer.writerow(['='*80])
            
            if category_stats:
                writer.writerow(['Danh mục', 'Loại tiền', 'Tổng chi', 'Số lần', 'Budget', '% Budget', 'Đánh giá'])
                
                for key, data in category_stats.items():
                    percent = (data['total'] / data['budget'] * 100) if data['budget'] > 0 else 0
                    
                    if data['budget'] > 0:
                        if percent > 100:
                            status = "🔴 VƯỢT BUDGET"
                        elif percent > 80:
                            status = "⚠️ GẦN HẾT"
                        else:
                            status = "✅ TRONG BUDGET"
                    else:
                        status = "📊 KHÔNG BUDGET"
                    
                    writer.writerow([
                        data['category'],
                        data['currency'],
                        f"{data['total']:,.0f}",
                        data['count'],
                        f"{data['budget']:,.0f}",
                        f"{percent:.1f}%",
                        status
                    ])
            else:
                writer.writerow(['Không có dữ liệu chi tiêu'])
            
            writer.writerow([])
            
            # =========================================
            # 5. CÂN ĐỐI THU CHI
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['CÂN ĐỐI THU CHI'])
            writer.writerow(['='*80])
            
            all_currencies = set(list(total_income.keys()) + list(total_expense.keys()))
            
            writer.writerow(['Loại tiền', 'Tổng thu', 'Tổng chi', 'Cân đối', 'Đánh giá'])
            for currency in sorted(all_currencies):
                income = total_income.get(currency, 0)
                expense = total_expense.get(currency, 0)
                balance = income - expense
                
                if balance > 0:
                    eval_ = "✅ TIẾT KIỆM"
                elif balance < 0:
                    eval_ = "❌ THÂM HỤT"
                else:
                    eval_ = "➖ CÂN BẰNG"
                
                writer.writerow([
                    currency,
                    f"{income:,.0f}",
                    f"{expense:,.0f}",
                    f"{balance:,.0f}",
                    eval_
                ])
            
            writer.writerow([])
            
            # =========================================
            # 6. PHÂN TÍCH THEO THÁNG
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH THEO THÁNG'])
            writer.writerow(['='*80])
            
            all_months = sorted(set(list(income_by_month.keys()) + list(expense_by_month.keys())))
            
            writer.writerow(['Tháng', 'Thu (VND)', 'Chi (VND)', 'Cân đối', 'Thu (USD)', 'Chi (USD)', 'Cân đối'])
            
            for month in all_months:
                # VND
                income_vnd = income_by_month.get(month, {}).get('VND', 0)
                expense_vnd = expense_by_month.get(month, {}).get('VND', 0)
                balance_vnd = income_vnd - expense_vnd
                
                # USD
                income_usd = income_by_month.get(month, {}).get('USD', 0)
                expense_usd = expense_by_month.get(month, {}).get('USD', 0)
                balance_usd = income_usd - expense_usd
                
                writer.writerow([
                    month,
                    f"{income_vnd:,.0f}",
                    f"{expense_vnd:,.0f}",
                    f"{balance_vnd:,.0f}",
                    f"{income_usd:,.2f}",
                    f"{expense_usd:,.2f}",
                    f"{balance_usd:,.2f}"
                ])
            
            writer.writerow([])
            
            # =========================================
            # 7. THỐNG KÊ TỔNG HỢP
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['THỐNG KÊ TỔNG HỢP'])
            writer.writerow(['='*80])
            
            total_income_all = sum(total_income.values())
            total_expense_all = sum(total_expense.values())
            
            writer.writerow(['Tổng số khoản thu:', len(incomes)])
            writer.writerow(['Tổng số khoản chi:', len(expenses)])
            writer.writerow(['Tổng thu (quy đổi VND):', f"{total_income_all:,.0f} VND"])
            writer.writerow(['Tổng chi (quy đổi VND):', f"{total_expense_all:,.0f} VND"])
            writer.writerow(['Tổng cân đối:', f"{total_income_all - total_expense_all:,.0f} VND"])
            
            # Tỷ lệ chi theo danh mục
            if total_expense_all > 0 and category_stats:
                writer.writerow([])
                writer.writerow(['TỶ LỆ CHI THEO DANH MỤC:'])
                
                # Sắp xếp theo tổng chi giảm dần
                sorted_cats = sorted(category_stats.items(), key=lambda x: x[1]['total'], reverse=True)
                for key, data in sorted_cats[:5]:  # Top 5
                    percent = (data['total'] / total_expense_all * 100)
                    writer.writerow([f"• {data['category']}: {percent:.1f}% ({data['total']:,.0f} {data['currency']})"])
            
            writer.writerow([])
            
            # =========================================
            # 8. ĐÁNH GIÁ & KHUYẾN NGHỊ
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['ĐÁNH GIÁ & KHUYẾN NGHỊ'])
            writer.writerow(['='*80])
            
            total_balance = total_income_all - total_expense_all
            
            # Đánh giá tổng quan
            if total_balance > 0:
                savings_rate = (total_balance / total_income_all * 100) if total_income_all > 0 else 0
                if savings_rate > 30:
                    rating = "🚀 XUẤT SẮC - Tiết kiệm rất tốt"
                elif savings_rate > 15:
                    rating = "✅ TỐT - Có quỹ tiết kiệm"
                else:
                    rating = "📊 ỔN ĐỊNH - Cân đối tốt"
            else:
                deficit_rate = (abs(total_balance) / total_expense_all * 100) if total_expense_all > 0 else 0
                if deficit_rate > 30:
                    rating = "🔴 NGUY CẤP - Chi tiêu vượt quá nhiều"
                elif deficit_rate > 15:
                    rating = "⚠️ CẢNH BÁO - Đang chi vượt thu"
                else:
                    rating = "📉 THÂM HỤT NHẸ - Cần điều chỉnh"
            
            writer.writerow(['Đánh giá tổng quan:', rating])
            writer.writerow([])
            
            # Khuyến nghị
            writer.writerow(['KHUYẾN NGHỊ:'])
            
            # Danh mục chi nhiều nhất
            if category_stats:
                # Tìm danh mục chi nhiều nhất
                max_cat = max(category_stats.items(), key=lambda x: x[1]['total'])
                writer.writerow([f'• Danh mục "{max_cat[1]["category"]}" chi nhiều nhất: {max_cat[1]["total"]:,.0f} {max_cat[1]["currency"]}'])
            
            # Danh mục vượt budget
            over_budget = []
            for key, data in category_stats.items():
                if data['budget'] > 0 and data['total'] > data['budget']:
                    over_budget.append(data)
            
            if over_budget:
                writer.writerow([f'• Có {len(over_budget)} danh mục vượt budget:'])
                for data in over_budget[:3]:
                    writer.writerow([f'  - {data["category"]}: vượt {data["total"] - data["budget"]:,.0f} {data["currency"]}'])
            else:
                writer.writerow(['• Tốt! Không có danh mục nào vượt budget'])
            
            # Tỷ lệ chi
            if total_income_all > 0:
                expense_ratio = (total_expense_all / total_income_all * 100)
                if expense_ratio > 90:
                    writer.writerow(['• Cảnh báo: Chi tiêu chiếm >90% thu nhập'])
                elif expense_ratio > 70:
                    writer.writerow(['• Chi tiêu chiếm ~70% thu nhập - Ổn định'])
                else:
                    writer.writerow([f'• Chi tiêu chỉ chiếm {expense_ratio:.1f}% thu nhập - Tốt'])
            
            writer.writerow([])
            writer.writerow(['='*80])
            writer.writerow(['KẾT THÚC BÁO CÁO'])
            writer.writerow(['='*80])
            
            # Lấy nội dung CSV
            csv_content = csv_buffer.getvalue()
            
            # Nếu có password, tạo ZIP mã hóa
            if password and HAS_PYZIPPER:
                return create_encrypted_zip(csv_content, f"expense_master_{user_id}.csv", password)
            
            return csv_content
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo expense master report: {e}")
            import traceback
            traceback.print_exc()
            return None

    def generate_master_report(user_id, password=None):
        """
        Tạo file báo cáo MASTER bao gồm TẤT CẢ thông tin:
        - Dữ liệu giao dịch mua
        - Dữ liệu giao dịch bán (đã chốt lời/lỗ)
        - Phân tích lợi nhuận đã thực hiện
        - Dự báo
        - Khuyến nghị
        """
        try:
            import csv
            import io  # <-- THÊM DÒNG NÀY
            from datetime import datetime
            
            # Tạo buffer cho CSV
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            # =========================================
            # 1. THÔNG TIN TỔNG QUAN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['BÁO CÁO ĐẦU TƯ MASTER'])
            writer.writerow(['='*80])
            writer.writerow(['Ngày xuất:', format_vn_time()])
            writer.writerow(['User ID:', user_id])
            writer.writerow(['Định dạng:', 'CSV MASTER - Bao gồm tất cả phân tích'])
            writer.writerow(['Mã hóa:', 'AES-256' if password else 'Không mã hóa'])
            writer.writerow([])
            
            # =========================================
            # 2. LẤY DỮ LIỆU THỰC TẾ
            # =========================================
            # Lấy giao dịch mua
            buy_transactions = get_transaction_detail(user_id)
            
            # Lấy lịch sử bán từ bảng sell_history
            sell_transactions = get_sell_history(user_id, 5000)  # Lấy tối đa 5000 giao dịch bán
            
            # Lấy portfolio hiện tại
            portfolio_data = get_portfolio(user_id)
            
            if not buy_transactions and not sell_transactions and not portfolio_data:
                writer.writerow(['KHÔNG CÓ DỮ LIỆU'])
                csv_content = csv_buffer.getvalue()
                if password and HAS_PYZIPPER:
                    return create_encrypted_zip(csv_content, f"report_empty_{user_id}.csv", password)
                return csv_content
            
            # =========================================
            # 3. DANH SÁCH GIAO DỊCH MUA (ĐANG NẮM GIỮ)
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH GIAO DỊCH MUA - ĐANG NẮM GIỮ'])
            writer.writerow(['='*80])
            writer.writerow([
                'ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 
                'Ngày mua', 'Tổng vốn (USD)', 'Giá hiện tại (USD)',
                'Giá trị hiện tại (USD)', 'Lợi nhuận (USD)', 'Lợi nhuận (%)', 'Thời gian nắm giữ (ngày)'
            ])
            
            total_invest = 0  # Tổng vốn đang nắm giữ
            total_current = 0  # Tổng giá trị hiện tại
            all_coins = {}  # Dict tổng hợp theo coin
            
            for tx in buy_transactions:
                tx_id, symbol, amount, buy_price, buy_date, total_cost = tx
                
                # Lấy giá hiện tại
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else buy_price
                current_value = amount * current_price
                profit = current_value - total_cost
                profit_pct = (profit / total_cost * 100) if total_cost > 0 else 0
                
                # Tính thời gian nắm giữ
                try:
                    buy_datetime = datetime.strptime(buy_date, "%Y-%m-%d %H:%M:%S")
                    hold_days = (get_vn_time() - buy_datetime).days
                except:
                    hold_days = 0
                
                writer.writerow([
                    tx_id, symbol, f"{amount:.8f}", f"${buy_price:,.2f}",
                    buy_date[:10], f"${total_cost:,.2f}", f"${current_price:,.2f}",
                    f"${current_value:,.2f}", f"${profit:,.2f}", f"{profit_pct:+.2f}%",
                    hold_days
                ])
                
                total_invest += total_cost
                total_current += current_value
                
                # Tổng hợp theo coin
                if symbol not in all_coins:
                    all_coins[symbol] = {
                        'amount': 0,
                        'invest': 0,
                        'current': 0,
                        'unrealized_profit': 0,
                        'realized_profit': 0,
                        'buy_count': 0,
                        'sell_count': 0,
                        'buy_transactions': []
                    }
                all_coins[symbol]['amount'] += amount
                all_coins[symbol]['invest'] += total_cost
                all_coins[symbol]['current'] += current_value
                all_coins[symbol]['unrealized_profit'] += profit
                all_coins[symbol]['buy_count'] += 1
                all_coins[symbol]['buy_transactions'].append({
                    'id': tx_id,
                    'amount': amount,
                    'price': buy_price,
                    'date': buy_date
                })
            
            writer.writerow([])
            
            # =========================================
            # 4. DANH SÁCH GIAO DỊCH BÁN - LỢI NHUẬN ĐÃ CHỐT
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DANH SÁCH GIAO DỊCH BÁN - LỢI NHUẬN ĐÃ CHỐT'])
            writer.writerow(['='*80])
            writer.writerow([
                'ID', 'Mã coin', 'Số lượng bán', 'Giá bán (USD)', 'Giá vốn TB (USD)',
                'Giá trị bán (USD)', 'Vốn gốc (USD)', 'Lợi nhuận (USD)', 
                'Tỷ suất (%)', 'Ngày bán'
            ])
            
            total_realized_profit = 0
            total_sold_value = 0
            total_sold_cost = 0
            
            if sell_transactions:
                for sell in sell_transactions:
                    # sell = (id, symbol, amount, sell_price, buy_price, profit, profit_pct, sell_date, created_at)
                    sell_id, symbol, amount, sell_price, buy_price, profit, profit_pct, sell_date, created_at = sell
                    
                    total_sold = amount * sell_price
                    total_cost = amount * buy_price
                    
                    writer.writerow([
                        sell_id, symbol, f"{amount:.8f}", f"${sell_price:,.2f}", f"${buy_price:,.2f}",
                        f"${total_sold:,.2f}", f"${total_cost:,.2f}", f"${profit:,.2f}", 
                        f"{profit_pct:+.2f}%", sell_date
                    ])
                    
                    total_sold_value += total_sold
                    total_sold_cost += total_cost
                    total_realized_profit += profit
                    
                    # Cập nhật thông tin coin
                    if symbol in all_coins:
                        all_coins[symbol]['realized_profit'] += profit
                        all_coins[symbol]['sell_count'] += 1
                    else:
                        # Coin đã bán hết, không còn trong portfolio
                        all_coins[symbol] = {
                            'amount': 0,
                            'invest': 0,
                            'current': 0,
                            'unrealized_profit': 0,
                            'realized_profit': profit,
                            'buy_count': 0,
                            'sell_count': 1,
                            'buy_transactions': []
                        }
            else:
                writer.writerow(['CHƯA CÓ GIAO DỊCH BÁN NÀO'])
            
            writer.writerow([])
            
            # =========================================
            # 5. TỔNG KẾT DANH MỤC
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['TỔNG KẾT DANH MỤC'])
            writer.writerow(['='*80])
            
            total_unrealized_profit = total_current - total_invest
            total_unrealized_pct = (total_unrealized_profit / total_invest * 100) if total_invest > 0 else 0
            total_overall_profit = total_realized_profit + total_unrealized_profit
            total_capital = total_invest + total_sold_cost  # Tổng vốn đã đầu tư (cả đã bán và đang giữ)
            total_overall_pct = (total_overall_profit / total_capital * 100) if total_capital > 0 else 0
            
            writer.writerow(['A. VỐN ĐANG ĐẦU TƯ (ĐANG NẮM GIỮ):'])
            writer.writerow([f'   • Tổng vốn đang nắm giữ: ${total_invest:,.2f}'])
            writer.writerow([f'   • Tổng giá trị hiện tại: ${total_current:,.2f}'])
            writer.writerow([f'   • Lợi nhuận chưa chốt: ${total_unrealized_profit:,.2f}'])
            writer.writerow([f'   • Tỷ suất chưa chốt: {total_unrealized_pct:+.2f}%'])
            writer.writerow([])
            
            writer.writerow(['B. LỢI NHUẬN ĐÃ CHỐT (ĐÃ BÁN):'])
            writer.writerow([f'   • Tổng vốn đã bán: ${total_sold_cost:,.2f}'])
            writer.writerow([f'   • Tổng giá trị đã bán: ${total_sold_value:,.2f}'])
            writer.writerow([f'   • Lợi nhuận đã chốt: ${total_realized_profit:,.2f}'])
            writer.writerow([])
            
            writer.writerow(['='*40])
            writer.writerow(['C. TỔNG HỢP:'])
            writer.writerow([f'   • Tổng vốn đã đầu tư: ${total_capital:,.2f}'])
            writer.writerow([f'   • Tổng lợi nhuận (đã chốt + chưa chốt): ${total_overall_profit:,.2f}'])
            writer.writerow([f'   • Tỷ suất lợi nhuận tổng thể: {total_overall_pct:+.2f}%'])
            writer.writerow(['='*40])
            writer.writerow([])
            
            writer.writerow(['THỐNG KÊ GIAO DỊCH:'])
            writer.writerow([f'   • Tổng số giao dịch mua: {len(buy_transactions)}'])
            writer.writerow([f'   • Tổng số giao dịch bán: {len(sell_transactions) if sell_transactions else 0}'])
            writer.writerow([f'   • Số loại coin đã giao dịch: {len(all_coins)}'])
            writer.writerow([f'   • Số loại coin đang nắm giữ: {len([c for c in all_coins.values() if c["amount"] > 0])}'])
            writer.writerow([])
            
            # =========================================
            # 6. PHÂN TÍCH THEO TỪNG COIN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['PHÂN TÍCH CHI TIẾT THEO TỪNG COIN'])
            writer.writerow(['='*80])
            writer.writerow([
                'Mã coin', 'Số lượng nắm giữ', 'Vốn đang nắm', 'Giá trị hiện tại',
                'LN chưa chốt', 'LN đã chốt', 'Tổng LN', 'Số lần mua', 'Số lần bán', 'Tỷ trọng vốn'
            ])
            
            for symbol, data in all_coins.items():
                total_profit_coin = data['unrealized_profit'] + data['realized_profit']
                weight = ((data['invest'] + data['realized_profit']) / total_capital * 100) if total_capital > 0 else 0
                
                writer.writerow([
                    symbol,
                    f"{data['amount']:.8f}" if data['amount'] > 0 else "0",
                    f"${data['invest']:,.2f}" if data['invest'] > 0 else "$0",
                    f"${data['current']:,.2f}" if data['current'] > 0 else "$0",
                    f"${data['unrealized_profit']:,.2f}" if data['unrealized_profit'] != 0 else "$0",
                    f"${data['realized_profit']:,.2f}" if data['realized_profit'] != 0 else "$0",
                    f"${total_profit_coin:,.2f}",
                    data['buy_count'],
                    data['sell_count'],
                    f"{weight:.1f}%"
                ])
            
            writer.writerow([])
            
            # =========================================
            # 7. CHI TIẾT LỢI NHUẬN ĐÃ CHỐT THEO COIN
            # =========================================
            if sell_transactions:
                writer.writerow(['='*80])
                writer.writerow(['CHI TIẾT LỢI NHUẬN ĐÃ CHỐT THEO TỪNG COIN'])
                writer.writerow(['='*80])
                
                # Lấy danh sách coin có lợi nhuận đã chốt
                coins_with_realized = [(s, d) for s, d in all_coins.items() if d['realized_profit'] != 0]
                
                if coins_with_realized:
                    for symbol, data in coins_with_realized:
                        writer.writerow([f'📌 {symbol}: Tổng lợi nhuận đã chốt = ${data["realized_profit"]:,.2f}'])
                        
                        # Lấy chi tiết các giao dịch bán của coin này
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute('''SELECT id, amount, sell_price, buy_price, profit, profit_percent, sell_date 
                                    FROM sell_history 
                                    WHERE user_id = ? AND symbol = ? 
                                    ORDER BY sell_date DESC''', (user_id, symbol))
                        coin_sells = c.fetchall()
                        conn.close()
                        
                        if coin_sells:
                            writer.writerow(['   ID', 'Số lượng', 'Giá bán', 'Giá vốn', 'Lợi nhuận', 'Tỷ suất', 'Ngày bán'])
                            for sell in coin_sells:
                                s_id, s_amount, s_price, s_buy_price, s_profit, s_profit_pct, s_date = sell
                                writer.writerow([
                                    f'   {s_id}',
                                    f'{s_amount:.8f}',
                                    f'${s_price:,.2f}',
                                    f'${s_buy_price:,.2f}',
                                    f'${s_profit:,.2f}',
                                    f'{s_profit_pct:+.2f}%',
                                    s_date
                                ])
                        writer.writerow([])
                else:
                    writer.writerow(['Chưa có lợi nhuận đã chốt từ bán coin'])
            
            # =========================================
            # 8. TOP COIN THEO LỢI NHUẬN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['TOP COIN THEO LỢI NHUẬN'])
            writer.writerow(['='*80])
            
            # Sắp xếp coin theo tổng lợi nhuận
            sorted_coins = sorted(all_coins.items(), key=lambda x: x[1]['unrealized_profit'] + x[1]['realized_profit'], reverse=True)
            
            writer.writerow(['🏆 TOP 5 COIN LỜI NHẤT (TỔNG LỢI NHUẬN)'])
            writer.writerow(['Mã coin', 'Lợi nhuận chưa chốt', 'Lợi nhuận đã chốt', 'Tổng lợi nhuận', 'Tỷ suất TB'])
            
            count = 0
            for symbol, data in sorted_coins:
                total_profit = data['unrealized_profit'] + data['realized_profit']
                if total_profit > 0:
                    count += 1
                    total_invest_coin = data['invest'] + abs(data['realized_profit'])
                    avg_return = (total_profit / total_invest_coin * 100) if total_invest_coin > 0 else 0
                    writer.writerow([
                        symbol,
                        f"${data['unrealized_profit']:,.2f}",
                        f"${data['realized_profit']:,.2f}",
                        f"${total_profit:,.2f}",
                        f"{avg_return:+.2f}%"
                    ])
                    if count >= 5:
                        break
            
            if count == 0:
                writer.writerow(['Không có coin lời'])
            
            writer.writerow([])
            writer.writerow(['📉 TOP 5 COIN LỖ NHẤT (TỔNG LỢI NHUẬN)'])
            writer.writerow(['Mã coin', 'Lỗ chưa chốt', 'Lỗ đã chốt', 'Tổng lỗ', 'Tỷ suất TB'])
            
            count = 0
            for symbol, data in reversed(sorted_coins):
                total_profit = data['unrealized_profit'] + data['realized_profit']
                if total_profit < 0:
                    count += 1
                    total_invest_coin = data['invest'] + abs(data['realized_profit'])
                    avg_return = (total_profit / total_invest_coin * 100) if total_invest_coin > 0 else 0
                    writer.writerow([
                        symbol,
                        f"${abs(data['unrealized_profit']):,.2f}",
                        f"${abs(data['realized_profit']):,.2f}",
                        f"${abs(total_profit):,.2f}",
                        f"{avg_return:+.2f}%"
                    ])
                    if count >= 5:
                        break
            
            if count == 0:
                writer.writerow(['Không có coin lỗ'])
            
            writer.writerow([])
            
            # =========================================
            # 9. DỰ BÁO LỢI NHUẬN
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['DỰ BÁO LỢI NHUẬN (KỊCH BẢN)'])
            writer.writerow(['='*80])
            writer.writerow(['*Dự báo dựa trên giả định thị trường tăng/giảm*'])
            writer.writerow([])
            
            scenarios = [-50, -30, -20, -10, 10, 20, 30, 50]
            writer.writerow(['Kịch bản', 'Giá trị danh mục', 'Lợi nhuận chưa chốt', 'Tổng lợi nhuận (kể cả đã chốt)'])
            
            for scenario in scenarios:
                multiplier = 1 + (scenario / 100)
                projected_value = total_current * multiplier
                projected_profit = projected_value - total_invest
                total_profit_with_realized = projected_profit + total_realized_profit
                
                emoji = "📈" if scenario > 0 else "📉" if scenario < 0 else "➡️"
                writer.writerow([
                    f"{emoji} {scenario:+.0f}%",
                    f"${projected_value:,.2f}",
                    f"${projected_profit:,.2f}",
                    f"${total_profit_with_realized:,.2f}"
                ])
            
            writer.writerow([])
            
            # =========================================
            # 10. ĐÁNH GIÁ & KHUYẾN NGHỊ
            # =========================================
            writer.writerow(['='*80])
            writer.writerow(['ĐÁNH GIÁ & KHUYẾN NGHỊ'])
            writer.writerow(['='*80])
            
            # Đánh giá dựa trên tổng lợi nhuận
            if total_overall_profit > 0:
                if total_overall_profit > total_capital * 0.5:
                    rating = "🚀 XUẤT SẮC - Tổng lợi nhuận >50%"
                elif total_overall_profit > total_capital * 0.2:
                    rating = "✅ TỐT - Lợi nhuận khả quan"
                elif total_overall_profit > 0:
                    rating = "📊 TẠM ỔN - Có lợi nhuận"
            else:
                if total_overall_profit < -total_capital * 0.5:
                    rating = "🔴 RỦI RO CAO - Lỗ nặng"
                elif total_overall_profit < -total_capital * 0.2:
                    rating = "⚠️ CẦN XEM XÉT - Lỗ đáng kể"
                else:
                    rating = "📉 ĐANG LỖ NHẸ"
            
            writer.writerow(['Đánh giá tổng quan:', rating])
            writer.writerow([])
            
            writer.writerow(['📊 PHÂN TÍCH LỢI NHUẬN:'])
            writer.writerow([f'   • Lợi nhuận đã chốt (thực tế): ${total_realized_profit:,.2f}'])
            writer.writerow([f'   • Lợi nhuận chưa chốt (trên giấy): ${total_unrealized_profit:,.2f}'])
            writer.writerow([f'   • Tổng lợi nhuận: ${total_overall_profit:,.2f}'])
            writer.writerow([f'   • Tỷ suất lợi nhuận tổng thể: {total_overall_pct:+.2f}%'])
            writer.writerow([])
            
            # Khuyến nghị chi tiết
            writer.writerow(['💡 KHUYẾN NGHỊ:'])
            
            # Đánh giá về lợi nhuận đã chốt
            if total_realized_profit > 0:
                writer.writerow([f'   ✅ Bạn đã chốt lời thành công ${total_realized_profit:,.2f} - Rất tốt!'])
            else:
                writer.writerow(['   ⚠️ Bạn chưa chốt lời lần nào - Cân nhắc chốt lời một phần khi thị trường tăng'])
            
            # Top coin lời nhất (chưa chốt)
            profitable_coins = [(s, d) for s, d in all_coins.items() if d['amount'] > 0 and d['unrealized_profit'] > 0]
            if profitable_coins:
                best_coin = max(profitable_coins, key=lambda x: x[1]['unrealized_profit'])
                profit = best_coin[1]['unrealized_profit']
                writer.writerow([f'   💰 {best_coin[0]} đang lời nhiều nhất: ${profit:,.2f} - Có thể chốt lời một phần'])
            
            # Top coin lỗ nhất (chưa chốt)
            losing_coins = [(s, d) for s, d in all_coins.items() if d['amount'] > 0 and d['unrealized_profit'] < 0]
            if losing_coins:
                worst_coin = min(losing_coins, key=lambda x: x[1]['unrealized_profit'])
                loss = abs(worst_coin[1]['unrealized_profit'])
                writer.writerow([f'   ⚠️ {worst_coin[0]} đang lỗ nhiều nhất: ${loss:,.2f} - Cân nhắc cắt lỗ hoặc đợi hồi'])
            
            # Đa dạng hóa
            active_coins = len([s for s, d in all_coins.items() if d['amount'] > 0])
            if active_coins < 3:
                writer.writerow([f'   📌 Danh mục chưa đa dạng (chỉ {active_coins} coin) - Nên đầu tư thêm'])
            elif active_coins > 10:
                writer.writerow([f'   📌 Danh mục khá đa dạng ({active_coins} coin) - Theo dõi sát các coin nhỏ'])
            
            # Tỷ trọng lớn
            for symbol, data in all_coins.items():
                if data['amount'] > 0:
                    weight = (data['current'] / total_current * 100) if total_current > 0 else 0
                    if weight > 50:
                        writer.writerow([f'   ⚠️ {symbol} chiếm {weight:.1f}% danh mục - Rủi ro tập trung cao'])
                    elif weight > 30:
                        writer.writerow([f'   📊 {symbol} chiếm {weight:.1f}% danh mục - Tỷ trọng khá lớn'])
            
            writer.writerow([])
            writer.writerow(['='*80])
            writer.writerow(['KẾT THÚC BÁO CÁO'])
            writer.writerow(['='*80])
            
            # Lấy nội dung CSV
            csv_content = csv_buffer.getvalue()
            
            # Nếu có password, tạo ZIP mã hóa
            if password and HAS_PYZIPPER:
                return create_encrypted_zip(csv_content, f"master_report_{user_id}.csv", password)
            
            return csv_content
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo master report: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_encrypted_zip(csv_content, filename, password):
        """Tạo ZIP AES-256 từ nội dung CSV"""
        try:
            import pyzipper
            import io
            
            zip_buffer = io.BytesIO()
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            zip_filename = f"master_report_{timestamp}.zip"
            
            with pyzipper.AESZipFile(
                zip_buffer, 'w',
                compression=pyzipper.ZIP_DEFLATED,
                encryption=pyzipper.WZ_AES
            ) as zip_file:
                zip_file.setpassword(password.encode('utf-8'))
                zip_file.writestr(filename, csv_content.encode('utf-8-sig'))
            
            zip_buffer.seek(0)
            return {'content': zip_buffer, 'filename': zip_filename}
        except Exception as e:
            logger.error(f"❌ Lỗi tạo ZIP: {e}")
            return None

    @auto_update_user
    async def view_portfolio_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'view'):
            await update.message.reply_text("❌ Bạn không có quyền xem dữ liệu!")
            return
        
        if not ctx.args:
            await update.message.reply_text("❌ /view [@username hoặc ID]")
            return
        
        target = ctx.args[0]
        target_user_id = None
        
        if target.startswith('@'):
            username = target[1:]
            target_user_id = get_user_id_by_username(username)
        else:
            try:
                target_user_id = int(target)
            except:
                pass
        
        if not target_user_id:
            await update.message.reply_text(f"❌ Không tìm thấy user {target}")
            return
        
        portfolio_data = get_portfolio(target_user_id)
        
        if not portfolio_data:
            await update.message.reply_text(f"📭 Danh mục của {target} trống!")
            return
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (target_user_id,))
        user_info = c.fetchone()
        conn.close()
        
        display_name = user_info[0] if user_info and user_info[0] else f"User {target_user_id}"
        
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
        
        msg = f"📊 *DANH MỤC CỦA {display_name}*\n━━━━━━━━━━━━\n\n"
        
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
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    async def list_users_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'view'):
            await update.message.reply_text("❌ Bạn không có quyền xem danh sách!")
            return
        
        try:
            admins = await ctx.bot.get_chat_administrators(chat_id)
            
            msg = "👥 *THÀNH VIÊN TRONG NHÓM*\n━━━━━━━━━━━━━━━━\n\n"
            
            for admin in admins:
                user = admin.user
                status = "👑 Admin" if admin.status in ['administrator', 'creator'] else "👤 Member"
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT last_seen FROM users WHERE user_id = ?", (user.id,))
                db_user = c.fetchone()
                conn.close()
                
                last_seen = db_user[0][:10] if db_user else "Chưa từng"
                
                msg += f"• {status}\n"
                msg += f"  ID: `{user.id}`\n"
                msg += f"  Username: @{user.username if user.username else 'None'}\n"
                msg += f"  Tên: {user.first_name} {user.last_name or ''}\n"
                msg += f"  Lần cuối: {last_seen}\n\n"
            
            msg += f"🕐 {format_vn_time()}"
            
            if len(msg) > 4000:
                chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
                for chunk in chunks:
                    await update.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi: {e}")

    @auto_update_user
    async def sync_admins_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền thực hiện lệnh này!")
            return
        
        msg = await update.message.reply_text("🔄 Đang đồng bộ danh sách admin...")
        
        try:
            admins = await ctx.bot.get_chat_administrators(chat_id)
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            granted_count = 0
            updated_count = 0
            
            for admin in admins:
                if admin.user:
                    await update_user_info_async(admin.user)
                    
                    c.execute("SELECT * FROM permissions WHERE group_id = ? AND user_id = ?", (chat_id, admin.user.id))
                    exists = c.fetchone()
                    
                    if not exists:
                        permissions = {'view': 1, 'edit': 0, 'delete': 0, 'manage': 0}
                        role = 'user'
                        
                        if admin.status == 'creator':
                            permissions = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 1}
                            role = 'staff'
                        elif admin.status == 'administrator':
                            permissions = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 0}
                            role = 'staff'
                        
                        created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
                        c.execute('''INSERT INTO permissions (group_id, user_id, granted_by, is_approved, role, can_view_all, can_edit_all, can_delete_all, can_manage_perms, created_at, approved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                  (chat_id, admin.user.id, user_id, 1, role,
                                   permissions['view'], permissions['edit'], permissions['delete'], permissions['manage'],
                                   created_at, created_at))
                        granted_count += 1
                    else:
                        updated_count += 1
            
            conn.commit()
            conn.close()
            
            await msg.edit_text(f"✅ *ĐỒNG BỘ ADMIN THÀNH CÔNG*\n━━━━━━━━━━━━━━━━\n\n📊 Kết quả:\n• Tổng số admin trong group: {len(admins)}\n• Đã cấp quyền mới: {granted_count}\n• Đã cập nhật: {updated_count}\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await msg.edit_text(f"❌ Lỗi: {e}")

    @auto_update_user
    async def new_chat_members(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        for new_member in update.message.new_chat_members:
            await update_user_info_async(new_member)
            
            if new_member.is_bot:
                continue
            
            chat_id = update.effective_chat.id

            # === MULTI-GROUP: Tự động kick nếu bị cross-ban ===
            if mg_is_cross_banned(chat_id, new_member.id):
                try:
                    await ctx.bot.ban_chat_member(chat_id=chat_id, user_id=new_member.id)
                    await update.message.reply_text(
                        f"🚫 {escape_markdown(new_member.first_name)} bị kick tự động do nằm trong danh sách ban của hệ thống.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logger.info(f"🚫 Auto-kicked cross-banned user {new_member.id} from {chat_id}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Không kick được {new_member.id}: {e}")

            # === MODERATION: Federation ban check ===
            if await mod_check_fed_ban(ctx, chat_id, new_member.id):
                continue

            # === MODERATION: CAPTCHA + Welcome ===
            await mod_on_new_member(update, ctx, new_member)

            try:
                admins = await ctx.bot.get_chat_administrators(chat_id)
                for admin in admins:
                    if admin.user.id == new_member.id:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        
                        c.execute("SELECT * FROM permissions WHERE group_id = ? AND user_id = ?", (chat_id, new_member.id))
                        exists = c.fetchone()
                        
                        if not exists:
                            permissions = {'view': 1, 'edit': 0, 'delete': 0, 'manage': 0}
                            
                            if admin.status == 'creator':
                                permissions = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 1}
                            
                            c.execute('''INSERT INTO permissions (group_id, user_id, granted_by, can_view_all, can_edit_all, can_delete_all, can_manage_perms, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                      (chat_id, new_member.id, new_member.id,
                                       permissions['view'], permissions['edit'], permissions['delete'], permissions['manage'],
                                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
                            conn.commit()
                            
                            logger.info(f"✅ Auto-granted permissions for new admin @{new_member.username} in {chat_id}")
                        
                        conn.close()
                        break
            except Exception as e:
                logger.error(f"❌ Lỗi xử lý new member: {e}")

    @auto_update_user
    async def check_perm_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        target_id = user_id
        target_name = "bạn"
        
        if update.message.reply_to_message:
            target_id = update.message.reply_to_message.from_user.id
            target_name = f"@{update.message.reply_to_message.from_user.username or target_id}"
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, target_id))
        result = c.fetchone()
        conn.close()
        
        if not result:
            msg = f"❌ *{target_name}* chưa được cấp quyền trong group này!"
        else:
            can_view, can_edit, can_delete, can_manage = result
            msg = f"🔐 *QUYỀN CỦA {target_name}*\n━━━━━━━━━━━━━━━━\n\n"
            msg += f"• 👁 Xem: {'✅' if can_view else '❌'}\n"
            msg += f"• ✏️ Sửa: {'✅' if can_edit else '❌'}\n"
            msg += f"• 🗑 Xóa: {'✅' if can_delete else '❌'}\n"
            msg += f"• 🔐 Quản lý: {'✅' if can_manage else '❌'}\n"
        
        msg += f"\n🕐 {format_vn_time()}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    async def sync_data_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền thực hiện lệnh này!")
            return
        
        msg = await update.message.reply_text("🔄 Đang đồng bộ dữ liệu user...")
        
        try:
            admins = await ctx.bot.get_chat_administrators(chat_id)
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            synced = 0
            for admin in admins:
                if admin.user:
                    current_time = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
                    
                    c.execute('''INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, last_seen) VALUES (?, ?, ?, ?, ?)''',
                              (admin.user.id, admin.user.username, admin.user.first_name, admin.user.last_name, current_time))
                    synced += 1
            
            conn.commit()
            conn.close()
            
            username_cache.clear()
            
            await msg.edit_text(f"✅ *ĐỒNG BỘ DỮ LIỆU THÀNH CÔNG*\n━━━━━━━━━━━━━━━━\n\n📊 Đã đồng bộ: {synced} user\n💾 Cache đã được làm mới\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await msg.edit_text(f"❌ Lỗi: {e}")

    @auto_update_user
    async def debug_perm_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ Owner mới có quyền sử dụng lệnh này!")
            return
        
        chat_id = update.effective_chat.id
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='permissions'")
            if not c.fetchone():
                await update.message.reply_text("❌ Bảng permissions chưa được tạo!")
                conn.close()
                return
            
            c.execute("PRAGMA table_info(permissions)")
            columns = c.fetchall()
            
            msg = "🔧 *DEBUG PERMISSIONS*\n"
            msg += f"Group ID: `{chat_id}`\n"
            msg += f"User ID: `{user_id}`\n"
            msg += "━━━━━━━━━━━━━━━━\n\n"
            
            msg += "*CẤU TRÚC BẢNG:*\n"
            for col in columns:
                msg += f"• `{col[1]}` ({col[2]})"
                if col[5] == 1:
                    msg += " PRIMARY KEY"
                if col[3] == 1:
                    msg += " NOT NULL"
                if col[4] is not None:
                    msg += f" DEFAULT '{col[4]}'"
                msg += "\n"
            
            c.execute("SELECT * FROM permissions WHERE group_id = ?", (chat_id,))
            rows = c.fetchall()
            
            msg += f"\n*DỮ LIỆU ({len(rows)} rows):*\n"
            if rows:
                for row in rows:
                    msg += f"• `{row}`\n"
            else:
                msg += "• Không có dữ liệu\n"
            
            c.execute("SELECT * FROM permissions WHERE group_id = ? AND user_id = ?", (chat_id, user_id))
            user_perm = c.fetchone()
            
            msg += f"\n*QUYỀN CỦA BẠN:*\n"
            if user_perm:
                msg += f"• {user_perm}\n"
            else:
                msg += "• Chưa có quyền trong group này\n"
            
            conn.close()
            
            if len(msg) > 4000:
                chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
                for i, chunk in enumerate(chunks, 1):
                    await update.message.reply_text(f"{chunk}\n\n*(Phần {i}/{len(chunks)})*", parse_mode=ParseMode.MARKDOWN)
            else:
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi: {str(e)}")

    @auto_update_user
    async def setup_group_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ Chỉ chủ sở hữu bot mới có thể setup group!")
            return
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong group!")
            return
        
        if set_group_owner(chat_id, OWNER_ID):
            await update.message.reply_text(f"✅ *THIẾT LẬP GROUP THÀNH CÔNG*\n━━━━━━━━━━━━━━━━\n\n• Group này đã được đặt dưới quyền sở hữu của bạn\n• Tất cả dữ liệu trong group sẽ là của bạn\n• Bạn có thể thêm admin để cùng quản lý\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi khi thiết lập group!")

    @auto_update_user
    async def group_info_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong group!")
            return
        
        owner_id = get_group_owner(chat_id)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (owner_id,))
        owner_info = c.fetchone()
        conn.close()
        
        owner_display = f"@{owner_info[0]}" if owner_info and owner_info[0] else (owner_info[1] if owner_info else f"User {owner_id}")
        
        msg = (f"ℹ️ *THÔNG TIN GROUP*\n━━━━━━━━━━━━━━━━\n\n"
               f"• Group ID: `{chat_id}`\n"
               f"• Chủ sở hữu: {owner_display} (`{owner_id}`)\n"
               f"• Bạn: {update.effective_user.first_name} (`{update.effective_user.id}`)\n\n"
               f"🕐 {format_vn_time()}")
        
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    async def add_group_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not is_group_owner(chat_id, user_id):
            await update.message.reply_text("❌ Chỉ chủ sở hữu group mới có thể thêm admin!")
            return
        
        if not ctx.args:
            await update.message.reply_text("❌ /addadmin @username [view/edit/delete/manage]")
            return
        
        target = ctx.args[0]
        perm_type = ctx.args[1] if len(ctx.args) > 1 else 'view'
        
        target_id = await resolve_user_id(target, ctx)
        if not target_id:
            await update.message.reply_text(f"❌ Không tìm thấy user {target}")
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
            await update.message.reply_text(f"✅ Đã thêm @{target} làm admin với quyền {perm_type}!")
        else:
            await update.message.reply_text("❌ Lỗi khi thêm admin!")

    @auto_update_user
    @require_group_permission('manage')
    async def add_admin_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not ctx.args:
            await update.message.reply_text("📝 *HƯỚNG DẪN THÊM ADMIN*\n\n• `/addadmin @user view` - Thêm quyền xem\n• `/addadmin @user edit` - Thêm quyền sửa\n• `/addadmin @user delete` - Thêm quyền xóa\n• `/addadmin @user manage` - Thêm quyền quản lý\n• `/addadmin @user full` - Thêm toàn quyền\n\nVí dụ: `/addadmin @john view`", parse_mode=ParseMode.MARKDOWN)
            return
        
        target = ctx.args[0]
        perm_type = ctx.args[1] if len(ctx.args) > 1 else 'view'
        
        if target.startswith('@'):
            username = target[1:]
            admin_id = get_user_id_by_username(username)
            if not admin_id:
                await update.message.reply_text(f"❌ Không tìm thấy user {target}")
                return
        else:
            try:
                admin_id = int(target)
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
        
        if grant_admin_permission(chat_id, admin_id, update.effective_user.id, permissions):
            await update.message.reply_text(f"✅ Đã thêm {target} làm admin với quyền {perm_type}!")
        else:
            await update.message.reply_text("❌ Lỗi khi thêm admin!")

    def get_all_admins(group_id):
        """Lấy danh sách admin từ bảng permissions (KHÔNG phải group_admins)"""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            # Đọc từ bảng permissions - đây là bảng đang được dùng để cấp quyền
            c.execute('''
                SELECT p.user_id, p.can_view_all, p.can_edit_all, p.can_delete_all, 
                       p.can_manage_perms, u.username, u.first_name, p.created_at 
                FROM permissions p 
                LEFT JOIN users u ON p.user_id = u.user_id 
                WHERE p.group_id = ? AND p.role = 'staff'
                ORDER BY p.created_at
            ''', (group_id,))
            admins = c.fetchall()
            conn.close()
            logger.info(f"📋 Found {len(admins)} admins in group {group_id} from permissions table")
            return admins
        except Exception as e:
            logger.error(f"❌ Lỗi get_all_admins: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def grant_admin_permission(group_id, admin_id, granted_by, permissions):
        """Cấp quyền admin trong group - ĐỒNG BỘ cả 2 bảng"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            created_at = get_vn_time().strftime("%Y-%m-%d %H:%M:%S")
            
            # ===== BẢNG CŨ: group_admins (giữ để tương thích) =====
            c.execute("DELETE FROM group_admins WHERE group_id = ? AND admin_id = ?", (group_id, admin_id))
            
            c.execute('''INSERT INTO group_admins 
                        (group_id, admin_id, granted_by, can_view, can_edit, can_delete, can_manage, created_at) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, admin_id, granted_by,
                       permissions.get('view', 0),
                       permissions.get('edit', 0),
                       permissions.get('delete', 0),
                       permissions.get('manage', 0),
                       created_at))
            
            # ===== BẢNG MỚI: permissions (quan trọng) =====
            # Xóa dữ liệu cũ trong bảng permissions
            c.execute("DELETE FROM permissions WHERE group_id = ? AND user_id = ?", (group_id, admin_id))
            
            # Xác định role dựa trên quyền
            role = 'staff'
            if permissions.get('manage', 0) == 1:
                role = 'staff'  # manage là quyền cao nhất cho staff
            
            # Thêm vào bảng permissions
            c.execute('''INSERT INTO permissions 
                        (group_id, user_id, granted_by, is_approved, role, 
                         can_view_all, can_edit_all, can_delete_all, can_manage_perms, 
                         created_at, approved_at) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, admin_id, granted_by, 1, role,
                       permissions.get('view', 0),
                       permissions.get('edit', 0),
                       permissions.get('delete', 0),
                       permissions.get('manage', 0),
                       created_at, created_at))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Granted admin permissions to {admin_id} in group {group_id}")
            logger.info(f"   • View: {permissions.get('view', 0)}")
            logger.info(f"   • Edit: {permissions.get('edit', 0)}")
            logger.info(f"   • Delete: {permissions.get('delete', 0)}")
            logger.info(f"   • Manage: {permissions.get('manage', 0)}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi grant admin: {e}")
            return False
        
    def revoke_admin_permission(group_id, admin_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM group_admins WHERE group_id = ? AND admin_id = ?", (group_id, admin_id))
            conn.commit()
            affected = c.rowcount
            conn.close()
            
            if affected > 0:
                logger.info(f"✅ Revoked admin permissions from {admin_id} in group {group_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi revoke admin: {e}")
            return False

    def check_admin_permission(group_id, admin_id, permission='view'):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT can_view, can_edit, can_delete, can_manage FROM group_admins WHERE group_id = ? AND admin_id = ?''', (group_id, admin_id))
            result = c.fetchone()
            conn.close()
            
            if not result:
                return False
            
            can_view, can_edit, can_delete, can_manage = result
            
            if permission == 'view':
                return can_view == 1
            elif permission == 'edit':
                return can_edit == 1
            elif permission == 'delete':
                return can_delete == 1
            elif permission == 'manage':
                return can_manage == 1
            
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi check_admin: {e}")
            return False

    @auto_update_user
    @require_group_permission('view')
    async def list_admin_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        admins = get_all_admins(chat_id)
        
        if not admins:
            await update.message.reply_text("📭 Chưa có admin nào trong group!")
            return
        
        msg = "👑 *DANH SÁCH ADMIN*\n━━━━━━━━━━━━━━━━\n\n"
        for admin in admins:
            if len(admin) >= 7:
                admin_id, view, edit, delete, manage, username, first_name = admin
            else:
                admin_id, view, edit, delete, manage = admin[:5]
                username = None
                first_name = None
            
            if username:
                display = f"@{username}"
            elif first_name:
                display = first_name
            else:
                display = f"User {admin_id}"
            
            permissions = []
            if view: permissions.append("👁 Xem")
            if edit: permissions.append("✏️ Sửa")
            if delete: permissions.append("🗑 Xóa")
            if manage: permissions.append("🔐 Quản lý")
            
            msg += f"• {display} (`{admin_id}`)\n"
            msg += f"  Quyền: {', '.join(permissions)}\n"
            msg += f"  Ngày thêm: {admin[7][:10] if len(admin) > 7 else 'N/A'}\n\n"
        
        msg += f"🕐 {format_vn_time()}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    @auto_update_user
    @require_group_permission('manage')
    async def remove_admin_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        if not ctx.args:
            await update.message.reply_text("📝 *HƯỚNG DẪN XÓA ADMIN*\n\n• `/removeadmin @user` - Xóa admin\n• `/removeadmin ID` - Xóa admin bằng ID\n\nVí dụ: `/removeadmin @john`", parse_mode=ParseMode.MARKDOWN)
            return
        
        target = ctx.args[0]
        
        if target.startswith('@'):
            username = target[1:]
            admin_id = get_user_id_by_username(username)
            if not admin_id:
                await update.message.reply_text(f"❌ Không tìm thấy user {target}")
                return
        else:
            try:
                admin_id = int(target)
            except:
                await update.message.reply_text("❌ ID không hợp lệ!")
                return
        
        if admin_id == update.effective_user.id:
            await update.message.reply_text("❌ Không thể tự xóa quyền admin của chính mình!")
            return
        
        if admin_id == get_group_owner(chat_id):
            await update.message.reply_text("❌ Không thể xóa chủ sở hữu group!")
            return
        
        if revoke_admin_permission(chat_id, admin_id):
            await update.message.reply_text(f"✅ Đã xóa {target} khỏi danh sách admin!")
        else:
            await update.message.reply_text(f"❌ Không tìm thấy {target} trong danh sách admin!")

    @auto_update_user
    @require_group_permission('delete')
    async def delete_category_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        if not ctx.args:
            categories = get_expense_categories(owner_id)
            
            if not categories:
                await update.message.reply_text("📭 Chưa có danh mục nào để xóa!")
                return
            
            msg = "🗑 *CHỌN DANH MỤC CẦN XÓA*\n━━━━━━━━━━━━━━━━\n\n"
            keyboard = []
            row = []
            
            for i, cat in enumerate(categories, 1):
                cat_id, name, budget, created = cat
                safe_name = escape_markdown(name)  # THÊM DÒNG NÀY
                msg += f"{i}. *{safe_name}* - {format_currency_simple(budget, 'VND')}\n"
                
                # ĐẢM BẢO callback_data là string
                callback_data = f"del_cat_{cat_id}"
                row.append(InlineKeyboardButton(f"{i}", callback_data=callback_data))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")])
            
            msg += f"\n🕐 {format_vn_time_short()}"
            
            # SỬA DÒNG NÀY - dùng safe_edit_message thay vì reply_text trực tiếp
            safe_msg = escape_markdown(msg)
            await update.message.reply_text(safe_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        try:
            category_id = int(ctx.args[0])
            
            keyboard = [[InlineKeyboardButton("✅ Xác nhận xóa", callback_data=f"confirm_del_cat_{category_id}"),
                         InlineKeyboardButton("❌ Hủy", callback_data="expense_categories")]]
            
            categories = get_expense_categories(owner_id)
            category_name = "Không xác định"
            for cat in categories:
                if cat[0] == category_id:
                    category_name = cat[1]
                    break
            
            # ESCAPE tên danh mục
            safe_category_name = escape_markdown(category_name)
            
            msg = (f"⚠️ *CẢNH BÁO: XÓA DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                   f"📋 Danh mục: *{safe_category_name}* (ID: {category_id})\n\n"
                   f"❗️ Hành động này sẽ xóa:\n"
                   f"• Danh mục *{safe_category_name}*\n"
                   f"• Tất cả chi tiêu trong danh mục này\n\n"
                   f"❌ *Không thể khôi phục!*\n\n"
                   f"Bạn có chắc chắn muốn xóa?")
            
            safe_msg = escape_markdown(msg)
            await update.message.reply_text(safe_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            
        except ValueError:
            await update.message.reply_text("❌ ID không hợp lệ!")

    @auto_update_user
    @require_group_permission('delete')
    async def quick_delete_category(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not update.message.reply_to_message:
            await update.message.reply_text("❌ Hãy reply tin nhắn chứa ID danh mục cần xóa!")
            return
        
        reply_text = update.message.reply_to_message.text
        match = re.search(r'\*(\d+)\.\*', reply_text) or re.search(r'ID: (\d+)', reply_text)
        
        if not match:
            await update.message.reply_text("❌ Không tìm thấy ID danh mục trong tin nhắn được reply!")
            return
        
        category_id = int(match.group(1))
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        keyboard = [[InlineKeyboardButton("✅ Xác nhận xóa", callback_data=f"confirm_del_cat_{category_id}"),
                     InlineKeyboardButton("❌ Hủy", callback_data="expense_categories")]]
        
        msg = f"⚠️ *XÁC NHẬN XÓA DANH MỤC #{category_id}*\n\nBạn có chắc chắn muốn xóa?"
        safe_msg = escape_markdown(msg)
        await update.message.reply_text(safe_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

    @auto_update_user
    @require_permission('view')
    async def balance_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        owner_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type in ['group', 'supergroup']:
            current_user = update.effective_user.id
            if current_user != owner_id and not check_permission(chat_id, current_user, 'view'):
                await update.message.reply_text("❌ Bạn không có quyền xem dữ liệu!")
                return
        
        period = 'month'
        if ctx.args:
            arg = ctx.args[0].lower()
            if arg in ['day', 'ngay', 'hôm nay', 'today', 'd']:
                period = 'day'
            elif arg in ['month', 'thang', 'tháng', 'this month', 'm']:
                period = 'month'
            elif arg in ['year', 'nam', 'năm', 'this year', 'y']:
                period = 'year'
            elif arg in ['all', 'tat ca', 'tất cả', 'all time', 'a']:
                period = 'all'
        
        msg = await update.message.reply_text("🔄 Đang tính toán cân đối...")
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (owner_id,))
        user_info = c.fetchone()
        conn.close()
        
        user_name = f"@{user_info[0]}" if user_info and user_info[0] else (user_info[1] if user_info else "")
        
        balance_data = get_balance_summary(owner_id, period)
        
        if not balance_data:
            await msg.edit_text("❌ Không thể tính cân đối!")
            return
        
        balance_msg = format_balance_message(balance_data, user_name)
        
        keyboard = [
            [InlineKeyboardButton("📅 Hôm nay", callback_data="balance_day"),
             InlineKeyboardButton("📅 Tháng này", callback_data="balance_month")],
            [InlineKeyboardButton("📅 Năm nay", callback_data="balance_year"),
             InlineKeyboardButton("📊 Tất cả", callback_data="balance_all")],
            [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]
        ]
        
        await msg.edit_text(balance_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

    @auto_update_user
    async def show_portfolio_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        current_user = query.from_user.id
        chat_id = query.message.chat.id
        chat_type = query.message.chat.type
        
        if chat_type in ['group', 'supergroup']:
            owner_id = get_group_owner(chat_id)
            
            if current_user != owner_id and not check_permission(chat_id, current_user, 'view'):
                await query.edit_message_text("❌ Bạn không có quyền xem portfolio!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]))
                return
            
            target_user_id = owner_id
            target_name = "của group"
        else:
            target_user_id = current_user
            target_name = "của bạn"
        
        portfolio_data = get_portfolio(target_user_id)
        
        if not portfolio_data:
            await query.edit_message_text(f"📭 Danh mục {target_name} trống!\n\n🕐 {format_vn_time()}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]))
            return
        
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
        
        msg = f"📊 *DANH MỤC {target_name}*\n━━━━━━━━━━━━━━━━\n\n"
        
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
        
        msg += "━━━━━━━━━━━━━━━━\n"
        msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
        msg += f"💰 GT: `{fmt_price(total_value)}`\n"
        msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)\n\n"
        msg += f"🕐 {format_vn_time()}"
        
        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
        
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

    async def perm_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        
        if chat_type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!")
            return
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM permissions WHERE group_id = ?", (chat_id,))
        admin_count = c.fetchone()[0]
        
        if admin_count == 0:
            permissions = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 1}
            if grant_permission(chat_id, user_id, user_id, permissions):
                await update.message.reply_text("👑 *BẠN LÀ ADMIN ĐẦU TIÊN*\n\n✅ Đã tự động cấp toàn quyền!\nDùng `/perm list` để xem danh sách.", parse_mode=ParseMode.MARKDOWN)
                await update_user_info_async(update.effective_user)
                conn.close()
                return
        
        conn.close()
        
        if not check_permission(chat_id, user_id, 'manage'):
            await update.message.reply_text("❌ Bạn không có quyền quản lý phân quyền!")
            return
        
        if not ctx.args:
            msg = ("🔐 *QUẢN LÝ PHÂN QUYỀN*\n━━━━━━━━━━━━━━━━\n\n"
                   "*Các lệnh:*\n"
                   "• `/perm list` - Xem danh sách admin\n"
                   "• `/perm grant @user view` - Cấp quyền xem\n"
                   "• `/perm grant @user edit` - Cấp quyền sửa\n"
                   "• `/perm grant @user delete` - Cấp quyền xóa\n"
                   "• `/perm grant @user manage` - Cấp quyền quản lý\n"
                   "• `/perm grant @user full` - Cấp toàn quyền\n"
                   "• `/perm revoke @user` - Thu hồi quyền\n\n"
                   f"🕐 {format_vn_time_short()}")
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
            return
        
        if ctx.args[0] == "list":
            admins = get_all_admins(chat_id)
            if not admins:
                # Thử lấy từ bảng cũ nếu không có
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT COUNT(*) FROM group_admins WHERE group_id = ?''', (chat_id,))
                old_count = c.fetchone()[0]
                conn.close()
                
                if old_count > 0:
                    await update.message.reply_text("⚠️ Phát hiện dữ liệu admin cũ! Đang migrate...")
                    migrate_admin_data()
                    admins = get_all_admins(chat_id)  # Thử lại
                    
                if not admins:
                    await update.message.reply_text("📭 Chưa có admin nào được cấp quyền!")
                    return
            
            msg = "👑 *DANH SÁCH ADMIN*\n━━━━━━━━━━━━━━━━\n\n"
            for admin in admins:
                if len(admin) >= 7:
                    user_id, view, edit, delete, manage, username, first_name = admin
                else:
                    user_id, view, edit, delete, manage = admin[:5]
                    username = None
                    first_name = None
                
                if username:
                    display = f"`{user_id}` @{username}"
                elif first_name:
                    display = f"`{user_id}` {first_name}"
                else:
                    display = f"`{user_id}`"
                
                permissions = []
                if view: permissions.append("👁 Xem")
                if edit: permissions.append("✏️ Sửa")
                if delete: permissions.append("🗑 Xóa")
                if manage: permissions.append("🔐 Quản lý")
                
                msg += f"• {display}: {', '.join(permissions)}\n"
            
            msg += f"\n🕐 {format_vn_time_short()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        elif ctx.args[0] == "grant" and len(ctx.args) >= 3:
            target = ctx.args[1]
            perm_type = ctx.args[2].lower()
            
            target_id = None
            
            if target.startswith('@'):
                username = target[1:]
                target_id = get_user_id_by_username(username)
                
                if not target_id:
                    try:
                        chat = await ctx.bot.get_chat(username)
                        if chat:
                            target_id = chat.id
                            await update_user_info_async(chat)
                    except Exception as e:
                        logger.error(f"Lỗi get_chat: {e}")
                    
                    if not target_id:
                        await update.message.reply_text(f"❌ Không tìm thấy user {target}\n\n💡 *Cách khắc phục:*\n1. Yêu cầu user @{username} nhắn tin cho bot\n2. Hoặc dùng ID trực tiếp: `/perm grant [ID] {perm_type}`\n3. Dùng `/whoami` để xem ID của bạn\n4. Hoặc reply tin nhắn của họ và dùng: `/permgrant {perm_type}`", parse_mode=ParseMode.MARKDOWN)
                        return
            else:
                try:
                    target_id = int(target)
                    if not get_user_id_by_username(str(target_id)):
                        try:
                            chat = await ctx.bot.get_chat(target_id)
                            if chat:
                                await update_user_info_async(chat)
                        except:
                            pass
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

    async def expense_shortcut_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        # Xác định user_id thực sự
        chat_type = update.effective_chat.type
        current_user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        if chat_type == 'private':
            # Private chat: thêm cho chính mình
            target_user_id = current_user_id
            logger.info(f"💬 PRIVATE: thêm chi tiêu cho user {target_user_id}")
        else:
            # Group chat: thêm cho chủ sở hữu (nếu có quyền)
            target_user_id = ctx.bot_data.get('effective_user_id', current_user_id)
            logger.info(f"👥 GROUP: thêm chi tiêu cho owner {target_user_id}")
        
        text = update.message.text.strip()
        
        # ==================== THÊM THU NHẬP (tn) ====================
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
                
                if add_income(target_user_id, amount, source, currency, note):
                    # Thông báo ai là người sở hữu
                    owner_info = ""
                    if chat_type != 'private' and target_user_id != current_user_id:
                        owner_info = "\n📌 Dữ liệu thuộc về chủ sở hữu group"
                    
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM THU NHẬP*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: *{format_currency_simple(amount, currency)}*\n"
                        f"📌 Nguồn: *{source}*\n"
                        f"📝 Ghi chú: *{note if note else 'Không có'}*{owner_info}\n\n"
                        f"🕐 {format_vn_time()}", 
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm thu nhập!")
            except ValueError:
                await update.message.reply_text("❌ Số tiền không hợp lệ!")
        
        # ==================== THÊM DANH MỤC (dm) ====================
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
            
            if add_expense_category(target_user_id, name, budget):
                owner_info = ""
                if chat_type != 'private' and target_user_id != current_user_id:
                    owner_info = "\n📌 Dữ liệu thuộc về chủ sở hữu group"
                
                await update.message.reply_text(
                    f"✅ *ĐÃ THÊM DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                    f"📋 Tên: *{name.upper()}*\n"
                    f"💰 Budget: {format_currency_simple(budget, 'VND')}{owner_info}\n\n"
                    f"🕐 {format_vn_time()}", 
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("❌ Lỗi khi thêm danh mục!")
        
        # ==================== THÊM CHI TIÊU (ct) ====================
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
                
                categories = get_expense_categories(target_user_id)
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
                
                if add_expense(target_user_id, category_id, amount, currency, note):
                    owner_info = ""
                    if chat_type != 'private' and target_user_id != current_user_id:
                        owner_info = "\n📌 Dữ liệu thuộc về chủ sở hữu group"
                    
                    await update.message.reply_text(
                        f"✅ *ĐÃ THÊM CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                        f"💰 Số tiền: *{format_currency_simple(amount, currency)}*\n"
                        f"📂 Danh mục: *{category_name}*\n"
                        f"📝 Ghi chú: *{note if note else 'Không có'}*{owner_info}\n\n"
                        f"🕐 {format_vn_time()}", 
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ Lỗi khi thêm chi tiêu!")
            except ValueError:
                await update.message.reply_text("❌ ID hoặc số tiền không hợp lệ!")
        
        # ==================== XEM GIAO DỊCH GẦN ĐÂY (ds) ====================
        elif text == 'ds':
            recent_incomes = get_recent_incomes(target_user_id, 10)
            recent_expenses = get_recent_expenses(target_user_id, 10)
            
            if not recent_incomes and not recent_expenses:
                await update.message.reply_text("📭 Chưa có giao dịch nào!")
                return
            
            msg = "🔄 *GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
            
            # Thêm thông tin chủ sở hữu nếu đang ở group
            if chat_type != 'private' and target_user_id != current_user_id:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (target_user_id,))
                owner_info = c.fetchone()
                conn.close()
                owner_name = f"@{owner_info[0]}" if owner_info and owner_info[0] else (owner_info[1] if owner_info else f"User {target_user_id}")
                msg += f"📌 Dữ liệu của: {owner_name}\n\n"
            
            if recent_incomes:
                msg += "*💰 THU NHẬP:*\n"
                for inc in recent_incomes:
                    inc_id, amount, source, note, date, currency = inc
                    msg += f"• #{inc_id} {date}: {format_currency_simple(amount, currency)} - {source}\n"
                    if note:
                        msg += f"  📝 {note}\n"
                msg += "\n"
            
            if recent_expenses:
                msg += "*💸 CHI TIÊU:*\n"
                for exp in recent_expenses:
                    exp_id, cat_name, amount, note, date, currency = exp
                    msg += f"• #{exp_id} {date}: {format_currency_simple(amount, currency)} - {cat_name}\n"
                    if note:
                        msg += f"  📝 {note}\n"
            
            msg += f"\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        
        # ==================== BÁO CÁO THÁNG (bc) ====================
        elif text == 'bc':
            incomes_data = get_income_by_period(target_user_id, 'month')
            expenses_data = get_expenses_by_period(target_user_id, 'month')
            
            msg = f"📊 *BÁO CÁO THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
            
            # Thêm thông tin chủ sở hữu nếu đang ở group
            if chat_type != 'private' and target_user_id != current_user_id:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (target_user_id,))
                owner_info = c.fetchone()
                conn.close()
                owner_name = f"@{owner_info[0]}" if owner_info and owner_info[0] else (owner_info[1] if owner_info else f"User {target_user_id}")
                msg += f"📌 Dữ liệu của: {owner_name}\n\n"
            
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
                msg += "📭 Chưa có thu nhập trong tháng này.\n\n"
            
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
        
        # ==================== XÓA CHI TIÊU (xoa chi) ====================
        elif text.startswith('xoa chi '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa chi 5`")
                return
            
            try:
                expense_id = int(parts[2])
                if delete_expense(expense_id, target_user_id):
                    owner_info = ""
                    if chat_type != 'private' and target_user_id != current_user_id:
                        owner_info = " của chủ sở hữu"
                    await update.message.reply_text(
                        f"✅ Đã xóa khoản chi{owner_info} #{expense_id}\n\n🕐 {format_vn_time_short()}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản chi #{expense_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        # ==================== XÓA THU NHẬP (xoa thu) ====================
        elif text.startswith('xoa thu '):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("❌ Cần có ID! VD: `xoa thu 3`")
                return
            
            try:
                income_id = int(parts[2])
                if delete_income(income_id, target_user_id):
                    owner_info = ""
                    if chat_type != 'private' and target_user_id != current_user_id:
                        owner_info = " của chủ sở hữu"
                    await update.message.reply_text(
                        f"✅ Đã xóa khoản thu{owner_info} #{income_id}\n\n🕐 {format_vn_time_short()}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(f"❌ Không tìm thấy khoản thu #{income_id}")
            except ValueError:
                await update.message.reply_text("❌ ID không hợp lệ!")
        
        # ==================== SỬA THU NHẬP (edit thu / sua thu) ====================
        elif text.startswith('edit thu ') or text.startswith('sua thu '):
            # Chuyển thành lệnh /editthu
            if text.startswith('edit thu '):
                fake_args = text.replace('edit thu ', '').split()
            else:
                fake_args = text.replace('sua thu ', '').split()
            ctx.args = fake_args
            await edit_income_command(update, ctx)
        
        # ==================== SỬA CHI TIÊU (edit chi / sua chi) ====================
        elif text.startswith('edit chi ') or text.startswith('sua chi '):
            # Chuyển thành lệnh /editchi
            if text.startswith('edit chi '):
                fake_args = text.replace('edit chi ', '').split()
            else:
                fake_args = text.replace('sua chi ', '').split()
            ctx.args = fake_args
            await edit_expense_command(update, ctx)

    async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user:
            await update_user_info_async(update.effective_user)
        
        text = update.message.text.strip()
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        logger.info(f"📨 Tin nhắn từ user {user_id} Group ID: {chat_id} trong {chat_type}: '{text}'")
        
        # TRONG GROUP: Kiểm tra quyền trước khi xử lý
        if chat_type in ['group', 'supergroup']:
            if not check_permission(chat_id, user_id, 'view'):
                logger.info(f"⛔ User {user_id} không có quyền trong group, bỏ qua")
                return
            # Moderation: flood / filter / custom commands
            if await mod_on_message(update, ctx):
                return
        
        # Xử lý tính toán nếu có
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
        
        # Xử lý các lệnh tắt (tn, dm, ct, ds, bc)
        if text.startswith(('tn ', 'dm ', 'ct ', 'ds', 'bc', 'xoa chi ', 'xoa thu ')):
            await expense_shortcut_handler(update, ctx)
            return
        
        # Xử lý menu chính - SO SÁNH CHÍNH XÁC
        if text == "💰 ĐẦU TƯ COIN":
            logger.info(f"💰 User {user_id} chọn menu ĐẦU TƯ COIN")
            await update.message.reply_text(
                f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}", 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=get_invest_menu_keyboard(user_id, chat_id, chat_type)
            )
            return
            
        if text == "💵 QUẢN LÝ CHI TIÊU":
            logger.info(f"💰 User {user_id} chọn menu QUẢN LÝ CHI TIÊU")
            await update.message.reply_text(
                f"💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}", 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=get_expense_menu_keyboard()
            )
            return

        if text == "⚙️ CÀI ĐẶT":
            logger.info(f"⚙️ User {user_id} chọn menu CÀI ĐẶT")
            
            # Kiểm tra quyền: chỉ admin mới vào được cài đặt
            if chat_type in ['group', 'supergroup']:
                if not check_permission(chat_id, user_id, 'manage'):
                    await update.message.reply_text("❌ Bạn không có quyền quản lý cài đặt nhóm!")
                    return
            
            keyboard = [
                [InlineKeyboardButton("👥 QUẢN LÝ THÀNH VIÊN", callback_data="settings_members")],
                [InlineKeyboardButton("🔐 PHÂN QUYỀN CHI TIẾT", callback_data="settings_permissions")],
                [InlineKeyboardButton("📋 DANH SÁCH QUYỀN", callback_data="settings_list")],
                [InlineKeyboardButton("🔄 ĐỒNG BỘ ADMIN", callback_data="settings_sync")],
                [InlineKeyboardButton("🌐 NGÔN NGỮ", callback_data="lang_menu")],
                [InlineKeyboardButton("🔙 VỀ MENU CHÍNH", callback_data="back_to_main")]
            ]
            
            msg = f"⚙️ *CÀI ĐẶT NHÓM*\n━━━━━━━━━━━━━━━━\n\nChọn chức năng quản lý:\n\n🕐 {format_vn_time()}"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
            return
    
        if text == "🤔 HƯỚNG DẪN":
            logger.info(f"❓ User {user_id} chọn HƯỚNG DẪN")
            await help_command(update, ctx)
            return
        
        # Nếu không khớp với bất kỳ điều kiện nào
        logger.info(f"❓ Tin nhắn không xác định: '{text}'")

    async def export_csv_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = ctx.bot_data.get('effective_user_id', query.from_user.id)
        
        await query.edit_message_text("🔄 Đang tạo file CSV...")
        
        try:
            if query.data == "export_csv":
                transactions = get_transaction_detail(user_id)
                if not transactions:
                    await query.edit_message_text(
                        "📭 Không có dữ liệu portfolio để xuất!", 
                        parse_mode=None,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]])
                    )
                    return
                
                timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                filename = f"portfolio_{user_id}_{timestamp}.csv"
                filepath = os.path.join(EXPORT_DIR, filename)
                
                logger.info(f"📝 Đang tạo file CSV: {filepath}")
                
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 'Ngày mua', 'Tổng vốn (USD)'])
                    for tx in transactions:
                        writer.writerow([tx[0], tx[1], tx[2], tx[3], tx[4], tx[5]])
                
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    logger.info(f"✅ File đã tạo: {filepath}, kích thước: {file_size} bytes")
                    
                    with open(filepath, 'rb') as f:
                        await query.message.reply_document(document=f, filename=filename, caption=f"📊 *BÁO CÁO DANH MỤC ĐẦU TƯ*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công {len(transactions)} giao dịch!\n📁 File: `{filename}`\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
                    
                    os.remove(filepath)
                    logger.info(f"🗑 Đã xóa file tạm: {filepath}")
                else:
                    logger.error(f"❌ Không tìm thấy file sau khi tạo: {filepath}")
                    await query.edit_message_text("❌ Lỗi: Không thể tạo file CSV!")
                    return
                
                await query.edit_message_text(f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN, reply_markup=get_invest_menu_keyboard(user_id, query.message.chat.id))
                
            elif query.data == "expense_export":
                expenses = get_recent_expenses(user_id, 1000)
                incomes = get_recent_incomes(user_id, 1000)
                
                if not expenses and not incomes:
                    await query.edit_message_text(
                        "📭 Không có dữ liệu chi tiêu để xuất!", 
                        parse_mode=None,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]])
                    )
                    return
                
                timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
                filename = f"expense_report_{user_id}_{timestamp}.csv"
                filepath = os.path.join(EXPORT_DIR, filename)
                
                logger.info(f"📝 Đang tạo file CSV: {filepath}")
                
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
                
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    logger.info(f"✅ File đã tạo: {filepath}, kích thước: {file_size} bytes")
                    
                    with open(filepath, 'rb') as f:
                        await query.message.reply_document(document=f, filename=filename, caption=f"📊 *BÁO CÁO THU CHI*\n━━━━━━━━━━━━━━━━\n\n✅ Xuất thành công!\n• Thu nhập: {len(incomes)} giao dịch\n• Chi tiêu: {len(expenses)} giao dịch\n📁 File: `{filename}`\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
                    
                    os.remove(filepath)
                    logger.info(f"🗑 Đã xóa file tạm: {filepath}")
                else:
                    logger.error(f"❌ Không tìm thấy file sau khi tạo: {filepath}")
                    await query.edit_message_text("❌ Lỗi: Không thể tạo file CSV!")
                    return
                
                await query.edit_message_text(f"💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN, reply_markup=get_expense_menu_keyboard())
        except Exception as e:
            logger.error(f"❌ Lỗi export CSV: {e}", exc_info=True)
            await query.edit_message_text(
                f"❌ Lỗi khi xuất CSV: {str(e)[:200]}", 
                parse_mode=None,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_main")]])
            )
            
            try:
                if 'filepath' in locals() and os.path.exists(filepath):
                    os.remove(filepath)
                    logger.info(f"🗑 Đã dọn dẹp file lỗi: {filepath}")
            except:
                pass

    @auto_update_user
    @require_permission('view')
    async def export_secure_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xuất CSV có mật khẩu: /export_secure [password] [thời gian xóa]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        # Kiểm tra pyzipper đã được cài chưa
        if not HAS_PYZIPPER:
            await update.message.reply_text(
                "❌ *TÍNH NĂNG CHƯA SẴN SÀNG*\n\n"
                "Thư viện mã hóa chưa được cài đặt.\n"
                "Vui lòng dùng `/export_csv` để xuất không mã hóa.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Hướng dẫn nếu thiếu password
        if not ctx.args:
            keyboard = [[InlineKeyboardButton("🔙 Hủy", callback_data="back_to_invest")]]
            
            await update.message.reply_text(
                "🔐 *XUẤT CSV CÓ MẬT KHẨU*\n\n"
                "*Cú pháp:*\n"
                "`/export_secure [mật khẩu] [thời gian xóa]`\n\n"
                "*Ví dụ:*\n"
                "• `/export_secure 123456` - Xóa sau 30 giây (mặc định)\n"
                "• `/export_secure 123456 60` - Xóa sau 60 giây\n"
                "• `/export_secure 123456 0` - Không tự động xóa\n\n"
                "*Lưu ý:*\n"
                "• File sẽ được nén ZIP AES-256\n"
                "• Có thể mở bằng WinRAR, 7-Zip\n"
                "• Mật khẩu sẽ hiện trong chat\n"
                "• **Tin nhắn chứa mật khẩu sẽ tự động xóa sau thời gian bạn chọn!**\n\n"
                f"🕐 {format_vn_time_short()}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        password = ctx.args[0]
        
        # Parse thời gian xóa (mặc định 30 giây)
        delete_seconds = 30
        if len(ctx.args) >= 2:
            try:
                delete_seconds = int(ctx.args[1])
                if delete_seconds < 0:
                    delete_seconds = 30
                elif delete_seconds > 300:  # Giới hạn tối đa 5 phút
                    delete_seconds = 300
                    await update.message.reply_text("⚠️ Thời gian xóa tối đa là 300 giây (5 phút)")
            except ValueError:
                delete_seconds = 30
        
        # Cảnh báo nếu password quá ngắn
        if len(password) < 4:
            warning_msg = await update.message.reply_text("⚠️ Mật khẩu nên có ít nhất 4 ký tự để bảo mật tốt hơn!")
            # Xóa cảnh báo sau 5 giây
            asyncio.create_task(auto_delete_message(ctx, update.effective_chat.id, warning_msg.message_id, 5))
        
        msg = await update.message.reply_text("🔄 Đang tạo file bảo mật...")
        
        try:
            # Lấy dữ liệu portfolio
            transactions = get_transaction_detail(user_id)
            
            if not transactions:
                await msg.edit_text(
                    "📭 Không có dữ liệu để xuất!",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
                    ]])
                )
                return
            
            # Tạo nội dung CSV
            import csv
            import io
            
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            # Ghi header
            writer.writerow(['ID', 'Mã coin', 'Số lượng', 'Giá mua (USD)', 'Ngày mua', 'Tổng vốn (USD)'])
            
            # Ghi dữ liệu
            for tx in transactions:
                writer.writerow([
                    tx[0],  # ID
                    tx[1],  # Symbol
                    f"{tx[2]:.8f}",  # Amount
                    f"${tx[3]:,.2f}",  # Buy price
                    tx[4],  # Buy date
                    f"${tx[5]:,.2f}"  # Total cost
                ])
            
            # Tạo tên file
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            csv_filename = f"portfolio_{user_id}_{timestamp}.csv"
            zip_filename = f"portfolio_{user_id}_{timestamp}.zip"
            
            # Tạo ZIP có mật khẩu
            zip_buffer = io.BytesIO()
            
            with pyzipper.AESZipFile(
                zip_buffer, 
                'w', 
                compression=pyzipper.ZIP_DEFLATED, 
                encryption=pyzipper.WZ_AES
            ) as zip_file:
                # Đặt mật khẩu
                zip_file.setpassword(password.encode('utf-8'))
                # Thêm file CSV vào ZIP
                zip_file.writestr(csv_filename, csv_buffer.getvalue().encode('utf-8-sig'))
            
            # Chuẩn bị gửi file
            zip_buffer.seek(0)
            
            # Tạo caption với cảnh báo xóa
            time_display = "KHÔNG tự động xóa" if delete_seconds == 0 else f"{delete_seconds} giây"
            
            caption = (
                f"🔐 *FILE ĐÃ MÃ HÓA*\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"✅ *Số giao dịch:* {len(transactions)}\n"
                f"📁 *Tên file:* `{zip_filename}`\n"
                f"🔑 *Mật khẩu:* `{password}`\n\n"
                f"📊 *Cách mở file:*\n"
                f"1. Tải file ZIP về máy\n"
                f"2. Dùng WinRAR hoặc 7-Zip\n"
                f"3. Nhập mật khẩu khi giải nén\n\n"
                f"⚠️ *BẢO MẬT TỰ ĐỘNG:*\n"
                f"• Tin nhắn này sẽ tự động xóa sau **{time_display}**\n"
                f"• Hãy tải file ngay sau khi nhận!\n"
                f"• Không chia sẻ mật khẩu với người khác\n\n"
                f"🕐 {format_vn_time()}"
            )
            
            # Gửi file
            sent_message = await update.message.reply_document(
                document=zip_buffer,
                filename=zip_filename,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Xóa tin nhắn "Đang tạo..."
            await msg.delete()
            
            # Xóa tin nhắn lệnh gốc (chứa mật khẩu)
            try:
                await update.message.delete()
                logger.info(f"✅ Đã xóa tin nhắn lệnh gốc của user {user_id}")
            except Exception as e:
                logger.warning(f"⚠️ Không thể xóa tin nhắn lệnh gốc: {e}")
            
            # Tự động xóa tin nhắn chứa file và mật khẩu (nếu thời gian > 0)
            if delete_seconds > 0:
                asyncio.create_task(auto_delete_message(ctx, update.effective_chat.id, sent_message.message_id, delete_seconds))
            
            # Log thành công
            logger.info(f"✅ Exported secure ZIP for user {user_id} with {len(transactions)} transactions, auto-delete after {delete_seconds}s")
            
        except Exception as e:
            logger.error(f"❌ Lỗi export secure: {e}", exc_info=True)
            await msg.edit_text(
                f"❌ *LỖI KHI XUẤT FILE*\n\n"
                f"Lỗi: `{str(e)[:200]}`\n\n"
                f"Vui lòng thử lại hoặc dùng `/export_csv`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
                ]])
            )

    @auto_update_user
    @require_permission('view')
    async def export_master_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xuất báo cáo MASTER duy nhất: /export [password]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        # Kiểm tra nếu không có password
        if not ctx.args:
            await update.message.reply_text(
                "🔐 *XUẤT BÁO CÁO MASTER*\n\n"
                "Dùng lệnh: `/export [mật khẩu]`\n\n"
                "• `/export 123456` - File ZIP có mật khẩu (tự xóa sau 30s)\n"
                "• `/export 0` - File CSV không mã hóa\n\n"
                f"🕐 {format_vn_time_short()}",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        password = ctx.args[0]
        msg = await update.message.reply_text("🔄 Đang tạo báo cáo MASTER...")
        
        try:
            # Tạo báo cáo master
            result = generate_master_report(user_id, password if password != '0' else None)
            
            if not result:
                await msg.edit_text("❌ Không thể tạo báo cáo!")
                return
            
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            
            # Nếu có password và password != '0' -> gửi ZIP
            if password != '0' and HAS_PYZIPPER and isinstance(result, dict):
                # Gửi file ZIP
                sent_message = await update.message.reply_document(
                    document=result['content'],
                    filename=result['filename'],
                    caption=f"🔐 *BÁO CÁO MASTER ĐÃ MÃ HÓA*\n"
                            f"━━━━━━━━━━━━━━━━\n\n"
                            f"✅ *Số giao dịch:* {len(get_transaction_detail(user_id))}\n"
                            f"🔑 *Mật khẩu:* `{password}`\n"
                            f"📁 *File:* `{result['filename']}`\n\n"
                            f"📊 *Nội dung:*\n"
                            f"• Phân tích lợi nhuận chi tiết\n"
                            f"• Dự báo kịch bản thị trường\n"
                            f"• Khuyến nghị đầu tư\n\n"
                            f"⚠️ *TỰ ĐỘNG XÓA sau 30 giây*\n"
                            f"• Hãy tải file ngay!\n\n"
                            f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Xóa tin nhắn lệnh gốc (chứa mật khẩu)
                try:
                    await update.message.delete()
                    logger.info(f"✅ Đã xóa tin nhắn lệnh gốc của user {user_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Không thể xóa tin nhắn lệnh gốc: {e}")
                    
                # Tự động xóa tin nhắn chứa file sau 30s
                asyncio.create_task(auto_delete_message(ctx, update.effective_chat.id, sent_message.message_id, 30))
            
            else:
                # Gửi CSV thường
                filename = f"master_report_{user_id}_{timestamp}.csv"
                await update.message.reply_document(
                    document=io.BytesIO(result.encode('utf-8-sig')),
                    filename=filename,
                    caption=f"📊 *BÁO CÁO MASTER*\n"
                            f"━━━━━━━━━━━━━━━━\n\n"
                            f"✅ *Số giao dịch:* {len(get_transaction_detail(user_id))}\n"
                            f"📁 *File:* `{filename}`\n\n"
                            f"📊 *Nội dung:*\n"
                            f"• Phân tích lợi nhuận chi tiết\n"
                            f"• Dự báo kịch bản thị trường\n"
                            f"• Khuyến nghị đầu tư\n\n"
                            f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            await msg.delete()
            logger.info(f"✅ User {user_id} đã xuất báo cáo master thành công")
            
        except Exception as e:
            logger.error(f"❌ Lỗi export master: {e}", exc_info=True)
            await msg.edit_text(
                f"❌ *LỖI KHI XUẤT BÁO CÁO*\n\n"
                f"Lỗi: `{str(e)[:200]}`\n\n"
                f"Vui lòng thử lại sau.",
                parse_mode=ParseMode.MARKDOWN
            )

    @auto_update_user
    @require_permission('view')
    async def export_expense_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xuất báo cáo chi tiêu MASTER: /export_expense [password]"""
        user_id = ctx.bot_data.get('effective_user_id', update.effective_user.id)
        
        # Kiểm tra nếu không có password
        if not ctx.args:
            await update.message.reply_text(
                "🔐 *XUẤT BÁO CÁO CHI TIÊU MASTER*\n\n"
                "Dùng lệnh: `/export_expense [mật khẩu]`\n\n"
                "*Ví dụ:*\n"
                "• `/export_expense 123456` - File ZIP có mật khẩu (tự xóa sau 30s)\n"
                "• `/export_expense 0` - File CSV không mã hóa\n\n"
                "*File báo cáo bao gồm:*\n"
                "✅ Danh sách thu nhập chi tiết\n"
                "✅ Danh sách chi tiêu chi tiết\n"
                "✅ Phân tích theo danh mục\n"
                "✅ Cân đối theo loại tiền\n"
                "✅ Phân tích theo tháng\n"
                "✅ Đánh giá budget & khuyến nghị\n\n"
                f"🕐 {format_vn_time_short()}",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        password = ctx.args[0]
        msg = await update.message.reply_text("🔄 Đang tạo báo cáo chi tiêu MASTER...")
        
        try:
            # Tạo báo cáo master
            result = generate_expense_master_report(user_id, password if password != '0' else None)
            
            if not result:
                await msg.edit_text("❌ Không thể tạo báo cáo!")
                return
            
            timestamp = get_vn_time().strftime('%Y%m%d_%H%M%S')
            
            # Nếu có password và password != '0' -> gửi ZIP
            if password != '0' and HAS_PYZIPPER and isinstance(result, dict):
                # Gửi file ZIP
                sent_message = await update.message.reply_document(
                    document=result['content'],
                    filename=result['filename'],
                    caption=f"🔐 *BÁO CÁO CHI TIÊU MASTER ĐÃ MÃ HÓA*\n"
                            f"━━━━━━━━━━━━━━━━\n\n"
                            f"✅ *Số khoản thu:* {len(get_recent_incomes(user_id, 5000))}\n"
                            f"✅ *Số khoản chi:* {len(get_recent_expenses(user_id, 5000))}\n"
                            f"🔑 *Mật khẩu:* `{password}`\n"
                            f"📁 *File:* `{result['filename']}`\n\n"
                            f"📊 *Nội dung:*\n"
                            f"• Phân tích thu chi chi tiết\n"
                            f"• Thống kê theo danh mục\n"
                            f"• Đánh giá budget\n"
                            f"• Khuyến nghị tài chính\n\n"
                            f"⚠️ *TỰ ĐỘNG XÓA sau 30 giây*\n"
                            f"• Hãy tải file ngay!\n\n"
                            f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Xóa tin nhắn lệnh gốc (chứa mật khẩu)
                try:
                    await update.message.delete()
                    logger.info(f"✅ Đã xóa tin nhắn lệnh gốc của user {user_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Không thể xóa tin nhắn lệnh gốc: {e}")
                    
                # Tự động xóa tin nhắn chứa file sau 30s
                asyncio.create_task(auto_delete_message(ctx, update.effective_chat.id, sent_message.message_id, 30))
            
            else:
                # Gửi CSV thường
                filename = f"expense_master_{user_id}_{timestamp}.csv"
                await update.message.reply_document(
                    document=io.BytesIO(result.encode('utf-8-sig')),
                    filename=filename,
                    caption=f"📊 *BÁO CÁO CHI TIÊU MASTER*\n"
                            f"━━━━━━━━━━━━━━━━\n\n"
                            f"✅ *Số khoản thu:* {len(get_recent_incomes(user_id, 5000))}\n"
                            f"✅ *Số khoản chi:* {len(get_recent_expenses(user_id, 5000))}\n"
                            f"📁 *File:* `{filename}`\n\n"
                            f"📊 *Nội dung:*\n"
                            f"• Phân tích thu chi chi tiết\n"
                            f"• Thống kê theo danh mục\n"
                            f"• Đánh giá budget\n"
                            f"• Khuyến nghị tài chính\n\n"
                            f"🕐 {format_vn_time()}",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            await msg.delete()
            logger.info(f"✅ User {user_id} đã xuất báo cáo chi tiêu master thành công")
            
        except Exception as e:
            logger.error(f"❌ Lỗi export expense master: {e}", exc_info=True)
            await msg.edit_text(
                f"❌ *LỖI KHI XUẤT BÁO CÁO*\n\n"
                f"Lỗi: `{str(e)[:200]}`\n\n"
                f"Vui lòng thử lại sau.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        # Log chi tiết
        logger.info("=" * 50)
        logger.info(f"🔔 CALLBACK NHẬN ĐƯỢC: {query.data}")
        logger.info(f"   • User: {query.from_user.id} (@{query.from_user.username})")
        logger.info(f"   • Chat: {query.message.chat.id}")
        logger.info(f"   • Message ID: {query.message.message_id}")
        logger.info("=" * 50)
        
        if query.from_user:
            await update_user_info_async(query.from_user)
        
        data = query.data

        # === MULTI-GROUP CALLBACK ROUTING ===
        if data.startswith("mg_"):
            await handle_mg_callback(update, ctx)
            return

        # === MODERATION CALLBACK ROUTING ===
        if data.startswith("mod_"):
            await handle_mod_callback(update, ctx)
            return
        
        try:
            # Lấy thông tin cơ bản
            current_user_id = query.from_user.id
            chat_id = query.message.chat.id
            chat_type = query.message.chat.type
            
            # ===== QUAN TRỌNG: XÁC ĐỊNH TARGET USER DỰA TRÊN CHAT TYPE =====
            if chat_type == 'private':
                target_user_id = current_user_id
                is_admin = False
                is_owner_user = False
                owner_id = current_user_id
                logger.info(f"💬 PRIVATE CALLBACK: xử lý cho user {target_user_id}")
            else:
                # GROUP CHAT: Lấy thông tin từ context
                owner_id = ctx.bot_data.get('group_owner_id', get_group_owner(chat_id))
                is_admin = ctx.bot_data.get('is_admin', False)
                is_owner_user = (current_user_id == owner_id)
                
                # Log chi tiết
                logger.info(f"👥 GROUP CALLBACK - Chi tiết:")
                logger.info(f"   • current_user: {current_user_id}")
                logger.info(f"   • owner_id từ context: {owner_id}")
                logger.info(f"   • is_admin: {is_admin}")
                logger.info(f"   • is_owner_user: {is_owner_user}")
                
                # Kiểm tra quyền trong group
                if not check_permission(chat_id, current_user_id, 'view'):
                    logger.warning(f"⛔ User {current_user_id} không có quyền view trong group")
                    await safe_edit_message(query, "❌ Bạn không có quyền sử dụng bot trong nhóm này!")
                    return
                
                # Xác định target_user_id
                if is_admin or is_owner_user:
                    target_user_id = owner_id
                    logger.info(f"👑 Admin/owner thao tác: target = owner {target_user_id}")
                else:
                    target_user_id = current_user_id
                    logger.info(f"👤 User thường tự thao tác: target = self {target_user_id}")
            
            logger.info(f"🎯 Target user: {target_user_id}, IsAdmin: {is_admin}, IsOwner: {is_owner_user}")
            
            # ===========================================
            # NHÓM 1: XỬ LÝ XÓA DANH MỤC (ƯU TIÊN CAO NHẤT)
            # ===========================================
            
            if data.startswith("confirm_del_cat_"):
                cat_id = data.replace("confirm_del_cat_", "")
                logger.info(f"📂 Xác nhận xóa danh mục ID: {cat_id}")
                
                try:
                    category_id = int(cat_id)
                except ValueError:
                    await safe_edit_message(query, "❌ ID danh mục không hợp lệ!")
                    return
                
                await query.edit_message_text("🔄 Đang xóa danh mục...", parse_mode=None)
                
                try:
                    success, result, deleted_count = delete_category(category_id, owner_id)
                    
                    if success:
                        safe_result = escape_markdown(str(result))
                        msg = (f"✅ *ĐÃ XÓA DANH MỤC*\n"
                               f"━━━━━━━━━━━━━━━━\n\n"
                               f"📋 Đã xóa danh mục: *{safe_result}*\n"
                               f"💰 Đã xóa *{deleted_count}* khoản chi\n\n"
                               f"🕐 {format_vn_time()}")
                    else:
                        safe_result = escape_markdown(str(result))
                        msg = (f"❌ *LỖI*\n"
                               f"━━━━━━━━━━━━━━━━\n\n"
                               f"{safe_result}\n\n"
                               f"🕐 {format_vn_time()}")
                    
                    safe_msg = escape_markdown(msg)
                    keyboard = [[
                        InlineKeyboardButton("📋 Xem danh mục", callback_data="expense_categories"),
                        InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")
                    ]]
                    
                    await safe_edit_message(query, safe_msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    
                except Exception as e:
                    logger.error(f"❌ Lỗi xóa danh mục: {e}")
                    await query.edit_message_text(f"❌ Lỗi: {str(e)[:100]}", parse_mode=None)
                
                return
            
            if data.startswith("del_cat_"):
                cat_id = data.replace("del_cat_", "")
                logger.info(f"📂 Xóa danh mục ID: {cat_id}")
                
                try:
                    category_id = int(cat_id)
                except ValueError:
                    await safe_edit_message(query, "❌ ID danh mục không hợp lệ!")
                    return
                
                categories = get_expense_categories(owner_id)
                category_name = "Không xác định"
                for cat in categories:
                    if cat[0] == category_id:
                        category_name = cat[1]
                        break
                
                safe_category_name = escape_markdown(category_name)
                
                keyboard = [[
                    InlineKeyboardButton("✅ Xác nhận xóa", callback_data=f"confirm_del_cat_{category_id}"),
                    InlineKeyboardButton("❌ Hủy", callback_data="expense_categories")
                ]]
                
                msg = (f"⚠️ *CẢNH BÁO: XÓA DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                       f"📋 Danh mục: *{safe_category_name}* (ID: {category_id})\n\n"
                       f"❗️ Hành động này sẽ xóa:\n"
                       f"• Danh mục *{safe_category_name}*\n"
                       f"• Tất cả chi tiêu trong danh mục này\n\n"
                       f"❌ *Không thể khôi phục!*\n\n"
                       f"Bạn có chắc chắn muốn xóa?")
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 2: XỬ LÝ XÓA GIAO DỊCH COIN (ƯU TIÊN CAO)
            # ===========================================
            
            # ===========================================
            # XỬ LÝ XÓA GIAO DỊCH MUA (COIN)
            # ===========================================
            if data.startswith("confirm_del_") and not data.startswith("confirm_del_sell_") and not data.startswith("confirm_del_cat_"):
                tx_id_str = data.replace("confirm_del_", "")
                logger.info(f"💰 Xác nhận xóa giao dịch mua: {tx_id_str}")
                
                if tx_id_str.isdigit():
                    tx_id = int(tx_id_str)
                    
                    # Kiểm tra giao dịch
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute('''SELECT user_id, symbol, amount FROM portfolio WHERE id = ?''', (tx_id,))
                    result = c.fetchone()
                    
                    if not result:
                        conn.close()
                        await safe_edit_message(query, f"❌ Không tìm thấy giao dịch #{tx_id}")
                        return
                    
                    tx_owner_id, symbol, amount = result
                    
                    # Kiểm tra quyền xóa
                    can_delete = False
                    if tx_owner_id == target_user_id:
                        can_delete = True
                    elif is_admin and chat_type != 'private':
                        can_delete = True
                    
                    if not can_delete:
                        conn.close()
                        await safe_edit_message(query, "❌ Bạn không có quyền xóa giao dịch này!")
                        return
                    
                    # Thực hiện xóa
                    c.execute('''DELETE FROM portfolio WHERE id = ?''', (tx_id,))
                    conn.commit()
                    conn.close()
                    
                    msg = (f"✅ *ĐÃ XÓA GIAO DỊCH MUA #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                           f"• Coin: {symbol}\n"
                           f"• Số lượng: {amount:.4f}\n\n"
                           f"🕐 {format_vn_time()}")
                    
                    keyboard = [[InlineKeyboardButton("🔙 Về danh sách", callback_data="edit_transactions")]]
                    
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                else:
                    await safe_edit_message(query, "❌ ID không hợp lệ!")
                    return

            # ===========================================
            # XỬ LÝ XÓA LỊCH SỬ BÁN (SELL)
            # ===========================================
            if data.startswith("confirm_del_sell_"):
                sell_id = int(data.replace("confirm_del_sell_", ""))
                logger.info(f"💰 Xác nhận xóa lệnh bán: {sell_id}")
                
                if delete_sell_history(sell_id, target_user_id):
                    await safe_edit_message(
                        query, 
                        f"✅ *Đã xóa lệnh bán #{sell_id}*",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("📋 Xem lịch sử bán", callback_data="show_sells"),
                            InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
                        ]])
                    )
                else:
                    await safe_edit_message(query, f"❌ Không thể xóa lệnh bán #{sell_id}")
                return

            # ===========================================
            # XỬ LÝ XÓA DANH MỤC CHI TIÊU
            # ===========================================
            if data.startswith("confirm_del_cat_"):
                cat_id = data.replace("confirm_del_cat_", "")
                logger.info(f"📂 Xác nhận xóa danh mục ID: {cat_id}")
                
                try:
                    category_id = int(cat_id)
                except ValueError:
                    await safe_edit_message(query, "❌ ID danh mục không hợp lệ!")
                    return
                
                await query.edit_message_text("🔄 Đang xóa danh mục...", parse_mode=None)
                
                try:
                    success, result, deleted_count = delete_category(category_id, owner_id)
                    
                    if success:
                        safe_result = escape_markdown(str(result))
                        msg = (f"✅ *ĐÃ XÓA DANH MỤC*\n"
                               f"━━━━━━━━━━━━━━━━\n\n"
                               f"📋 Đã xóa danh mục: *{safe_result}*\n"
                               f"💰 Đã xóa *{deleted_count}* khoản chi\n\n"
                               f"🕐 {format_vn_time()}")
                    else:
                        safe_result = escape_markdown(str(result))
                        msg = (f"❌ *LỖI*\n"
                               f"━━━━━━━━━━━━━━━━\n\n"
                               f"{safe_result}\n\n"
                               f"🕐 {format_vn_time()}")
                    
                    safe_msg = escape_markdown(msg)
                    keyboard = [[
                        InlineKeyboardButton("📋 Xem danh mục", callback_data="expense_categories"),
                        InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")
                    ]]
                    
                    await safe_edit_message(query, safe_msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    
                except Exception as e:
                    logger.error(f"❌ Lỗi xóa danh mục: {e}")
                    await query.edit_message_text(f"❌ Lỗi: {str(e)[:100]}", parse_mode=None)
                
                return
            
            # ===========================================
            # NHÓM 3: CÁC CALLBACK CHÍNH XÁC (MENU CHÍNH)
            # ===========================================
            
            if data == "edit_transactions":
                logger.info("📋 Hiển thị danh sách sửa/xóa giao dịch")
                
                # QUAN TRỌNG: Kiểm tra chat type
                chat_type = query.message.chat.type
                
                # Nếu là private chat, chỉ quản lý dữ liệu của chính mình
                if chat_type == 'private':
                    target_user_id = current_user_id
                    logger.info(f"💬 Private chat: quản lý giao dịch cá nhân {target_user_id}")
                else:
                    # Trong group, chỉ cho phép chủ sở hữu hoặc admin
                    if not is_owner_user and not is_admin:
                        await safe_edit_message(query, "❌ Bạn không có quyền quản lý giao dịch!")
                        return
                    target_user_id = owner_id
                
                transactions = get_transaction_detail(target_user_id)
                
                if not transactions:
                    msg = f"📭 Không có giao dịch!\n\n🕐 {format_vn_time()}"
                    keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                
                msg = "✏️ *CHỌN GIAO DỊCH*\n━━━━━━━━━━━━━━━━\n\n"
                keyboard = []
                row = []
                
                for tx in transactions:
                    tx_id, symbol, amount, price, date, total = tx
                    short_date = date.split()[0] if date else "N/A"
                    amount_str = f"{amount:.4f}".rstrip('0').rstrip('.') if '.' in f"{amount:.4f}" else f"{amount:.4f}"
                    
                    msg += f"• #{tx_id}: {symbol} {amount_str} @ {fmt_price(price)} ({short_date})\n"
                    
                    row.append(InlineKeyboardButton(f"#{tx_id}", callback_data=f"edit_{tx_id}"))
                    
                    if len(row) == 4:
                        keyboard.append(row)
                        row = []
                
                if row:
                    keyboard.append(row)
                
                # Thay vì "Xem user khác", quay về menu chính
                keyboard.append([InlineKeyboardButton("🔙 Về menu đầu tư", callback_data="back_to_invest")])
                
                msg += f"\n🕐 {format_vn_time_short()}"
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "back_to_main":
                msg = f"💰 *MENU CHÍNH*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}"
                await safe_edit_message(query, msg, reply_markup=None)
                await query.message.reply_text("👇 Chọn chức năng:", reply_markup=get_main_keyboard())
                return
            
            if data == "back_to_invest":
                uid = query.from_user.id
                gid = query.message.chat.id
                msg = f"💰 *MENU ĐẦU TƯ COIN*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}"
                await safe_edit_message(query, msg, reply_markup=get_invest_menu_keyboard(uid, gid, chat_type))
                return
            
            if data == "back_to_expense":
                msg = f"💰 *QUẢN LÝ CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n🕐 {format_vn_time()}"
                await safe_edit_message(query, msg, reply_markup=get_expense_menu_keyboard())
                return
            
            if data == "refresh_usdt":
                rate_data = get_usdt_vnd_rate()
                text = ("💱 *TỶ GIÁ USDT/VND*\n━━━━━━━━━━━━━━━━\n\n"
                        f"🇺🇸 *1 USDT* = `{fmt_vnd(rate_data['vnd'])}`\n"
                        f"🇻🇳 *1,000,000 VND* = `{1000000/rate_data['vnd']:.4f} USDT`\n\n"
                        f"⏱ *Cập nhật:* `{rate_data['update_time']}`\n"
                        f"📊 *Nguồn:* `{rate_data['source']}`\n\n"
                        f"🕐 {format_vn_time()}")
                keyboard = [[InlineKeyboardButton("🔄 Làm mới", callback_data="refresh_usdt")],
                            [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
                return

            # ===========================================
            # NÚT XUẤT MASTER DUY NHẤT - THÊM VÀO ĐÂY
            # ===========================================
            if data == "export_master":
                await safe_edit_message(
                    query,
                    "🔐 *XUẤT BÁO CÁO MASTER*\n\n"
                    "Dùng lệnh: `/export [mật khẩu]`\n\n"
                    "• `/export 123456` - File ZIP có mật khẩu (tự xóa sau 30s)\n"
                    "• `/export 0` - File CSV không mã hóa\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Về menu đầu tư", callback_data="back_to_invest")
                    ]])
                )
                return
                
            # ===========================================
            # NHÓM 4: XỬ LÝ XEM PORTFOLIO - CHỈ XEM CỦA CHỦ SỞ HỮU
            # ===========================================
            
            if data == "show_portfolio":
                logger.info("📊 Hiển thị portfolio")
                
                # Xác định user_id cần xem
                chat_type = query.message.chat.type
                current_user_id = query.from_user.id
                
                if chat_type == 'private':
                    target_user_id = current_user_id
                    logger.info(f"💬 Private: xem portfolio cá nhân {target_user_id}")
                else:
                    # Trong group, chỉ cho xem portfolio của chủ sở hữu nếu có quyền
                    owner_id = ctx.bot_data.get('group_owner_id', get_group_owner(chat_id))
                    is_admin = ctx.bot_data.get('is_admin', False)
                    is_owner = (current_user_id == owner_id)
                    
                    if not check_permission(chat_id, current_user_id, 'view'):
                        await safe_edit_message(query, "❌ Bạn không có quyền xem portfolio!")
                        return
                    
                    if is_admin or is_owner:
                        target_user_id = owner_id
                        logger.info(f"👥 Group: admin xem portfolio của owner {target_user_id}")
                    else:
                        target_user_id = current_user_id
                        logger.info(f"👥 Group: user xem portfolio cá nhân {target_user_id}")
                
                # Lấy dữ liệu portfolio
                portfolio_data = get_portfolio(target_user_id)
                
                if not portfolio_data:
                    msg = f"📭 Danh mục trống!\n\n🕐 {format_vn_time()}"
                    keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                
                # Lấy tất cả symbols để fetch giá
                symbols = list(set([row[0] for row in portfolio_data]))
                prices = get_prices_batch(symbols)
                
                # Tính toán tổng hợp theo từng coin
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
                
                # Lấy tên hiển thị
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (target_user_id,))
                user_info = c.fetchone()
                conn.close()
                
                display_name = user_info[0] if user_info and user_info[0] else (user_info[1] if user_info else f"User {target_user_id}")
                safe_display_name = escape_markdown(display_name)
                
                msg = f"📊 *DANH MỤC CỦA {safe_display_name}*\n━━━━━━━━━━━━━━━━\n\n"
                
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
                
                msg += "━━━━━━━━━━━━━━━━\n"
                msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
                msg += f"💰 GT: `{fmt_price(total_value)}`\n"
                msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)\n\n"
                msg += f"🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 5: XỬ LÝ XEM LỢI NHUẬN - CHỈ XEM CỦA CHỦ SỞ HỮU
            # ===========================================
            
            if data == "show_profit":
                logger.info("📈 Hiển thị lợi nhuận")
                
                # QUAN TRỌNG: Kiểm tra chat type
                chat_type = query.message.chat.type
                
                # Nếu là private chat, chỉ xem dữ liệu của chính mình
                if chat_type == 'private':
                    target_user_id = current_user_id
                    logger.info(f"💬 Private chat: xem lợi nhuận cá nhân {target_user_id}")
                else:
                    # Trong group, admin mới được xem dữ liệu chủ sở hữu
                    if not is_owner_user and not is_admin:
                        await safe_edit_message(query, "❌ Bạn không có quyền xem lợi nhuận!")
                        return
                    target_user_id = owner_id
                
                transactions = get_transaction_detail(target_user_id)
                
                if not transactions:
                    msg = f"📭 Danh mục trống!\n\n🕐 {format_vn_time()}"
                    keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                
                # Lấy tên hiển thị
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (owner_id,))
                user_info = c.fetchone()
                conn.close()
                
                display_name = user_info[0] if user_info and user_info[0] else (user_info[1] if user_info else f"User {owner_id}")
                safe_display_name = escape_markdown(display_name)
                
                msg = f"📈 *CHI TIẾT LỢI NHUẬN*\n━━━━━━━━━━━━━━━━\n\n"
                total_invest = 0
                total_value = 0
                
                for tx in transactions:
                    tx_id, symbol, amount, price, date, cost = tx
                    price_data = get_price(symbol)
                    
                    if price_data:
                        current = amount * price_data['p']
                        profit = current - cost
                        profit_percent = (profit / cost) * 100 if cost > 0 else 0
                        
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
                
                msg += "━━━━━━━━━━━━━━━━\n"
                msg += f"💵 Vốn: `{fmt_price(total_invest)}`\n"
                msg += f"💰 GT: `{fmt_price(total_value)}`\n"
                msg += f"{'✅' if total_profit>=0 else '❌'} Tổng LN: `{fmt_price(total_profit)}` ({total_profit_percent:+.2f}%)\n\n"
                msg += f"🕐 {format_vn_time()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 6: XỬ LÝ XEM THỐNG KÊ - CHỈ XEM CỦA CHỦ SỞ HỮU
            # ===========================================
            
            if data == "show_stats":
                logger.info("📊 Hiển thị thống kê")
                
                # QUAN TRỌNG: Kiểm tra chat type
                chat_type = query.message.chat.type
                
                # Nếu là private chat, chỉ xem dữ liệu của chính mình
                if chat_type == 'private':
                    target_user_id = current_user_id
                    logger.info(f"💬 Private chat: xem thống kê cá nhân {target_user_id}")
                else:
                    # Trong group, admin mới được xem dữ liệu chủ sở hữu
                    if not is_owner_user and not is_admin:
                        await safe_edit_message(query, "❌ Bạn không có quyền xem thống kê!")
                        return
                    target_user_id = owner_id
                
                stats = get_portfolio_stats(target_user_id)
                
                if not stats:
                    msg = f"📭 Danh mục trống!"
                    keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                
                # Lấy tên hiển thị
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username, first_name FROM users WHERE user_id = ?", (owner_id,))
                user_info = c.fetchone()
                conn.close()
                
                display_name = user_info[0] if user_info and user_info[0] else (user_info[1] if user_info else f"User {owner_id}")
                safe_display_name = escape_markdown(display_name)
                
                msg = (f"📊 *THỐNG KÊ DANH MỤC*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*TỔNG QUAN*\n"
                       f"• Vốn: `{fmt_price(stats['total_invest'])}`\n"
                       f"• Giá trị: `{fmt_price(stats['total_value'])}`\n"
                       f"• Lợi nhuận: `{fmt_price(stats['total_profit'])}`\n"
                       f"• Tỷ suất: `{stats['total_profit_percent']:+.2f}%`\n\n"
                       f"*📈 TOP COIN LỜI NHẤT*\n")
                
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
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 7: XỬ LÝ XEM GIÁ COIN
            # ===========================================
            
            if data.startswith("price_"):
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
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 8: XỬ LÝ SỬA GIAO DỊCH
            # ===========================================
            
            if data.startswith("edit_form_"):
                logger.info(f"📝 Form sửa giao dịch: {data}")
                tx_id_str = data.replace("edit_form_", "")
                
                if not tx_id_str.isdigit():
                    await safe_edit_message(query, "❌ ID không hợp lệ!")
                    return
                
                tx_id = int(tx_id_str)
                
                # Lấy thông tin giao dịch
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT symbol, amount, buy_price FROM portfolio WHERE id = ?''', (tx_id,))
                tx = c.fetchone()
                conn.close()
                
                if not tx:
                    await safe_edit_message(query, f"❌ Không tìm thấy giao dịch #{tx_id}")
                    return
                
                symbol, current_amount, current_price = tx
                
                msg = (f"✏️ *SỬA GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📊 SL hiện tại: `{current_amount:.4f}`\n"
                       f"💰 Giá hiện tại: `{fmt_price(current_price)}`\n\n"
                       f"*Nhập lệnh:*\n"
                       f"`/edit {tx_id} [số lượng mới] [giá mới]`\n\n"
                       f"*Ví dụ:*\n"
                       f"`/edit {tx_id} 0.5 45000`\n\n"
                       f"🕐 {format_vn_time_short()}")
                
                keyboard = [[InlineKeyboardButton("🔙 Quay lại", callback_data=f"edit_{tx_id}")]]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
                
            if data.startswith("edit_"):
                logger.info(f"✏️ Sửa giao dịch: {data}")
                
                # QUAN TRỌNG: Kiểm tra nếu là "edit_transactions" thì đã xử lý ở trên
                if data == "edit_transactions":
                    return
                
                tx_id_str = data.replace("edit_", "")
                
                if not tx_id_str.isdigit():
                    logger.error(f"❌ edit_ callback với ID không hợp lệ: {tx_id_str}")
                    await safe_edit_message(query, "❌ ID không hợp lệ!")
                    return
                
                tx_id = int(tx_id_str)
                
                # Lấy chi tiết giao dịch
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT id, symbol, amount, buy_price, buy_date, total_cost, user_id 
                            FROM portfolio WHERE id = ?''', (tx_id,))
                tx = c.fetchone()
                conn.close()
                
                if not tx:
                    await safe_edit_message(query, f"❌ Không tìm thấy giao dịch #{tx_id}")
                    return
                
                tx_id, symbol, amount, price, date, total, tx_owner_id = tx
                
                # Kiểm tra quyền xem/sửa - chỉ cho phép chủ sở hữu hoặc admin
                if tx_owner_id != target_user_id and not is_admin:
                    await safe_edit_message(query, "❌ Bạn không có quyền xem giao dịch này!")
                    return
                
                # Lấy giá hiện tại
                price_data = get_price(symbol)
                current_price = price_data['p'] if price_data else 0
                profit = (current_price - price) * amount if current_price else 0
                profit_percent = ((current_price - price) / price) * 100 if price and current_price else 0
                
                # Tạo message
                msg = (f"📝 *GIAO DỊCH #{tx_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📅 Ngày mua: {date}\n"
                       f"📊 Số lượng: `{amount:.4f}`\n"
                       f"💰 Giá mua: `{fmt_price(price)}`\n"
                       f"💵 Tổng vốn: `{fmt_price(total)}`\n"
                       f"📈 Giá hiện tại: `{fmt_price(current_price)}`\n"
                       f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_percent:+.2f}%)\n\n")
                
                # Thêm nút sửa/xóa nếu có quyền
                keyboard = []
                if tx_owner_id == owner_id or is_admin:
                    keyboard.append([
                        InlineKeyboardButton("✏️ Sửa", callback_data=f"edit_form_{tx_id}"),
                        InlineKeyboardButton("🗑 Xóa", callback_data=f"del_{tx_id}")
                    ])
                
                keyboard.append([InlineKeyboardButton("🔙 Về danh sách", callback_data="edit_transactions")])
                
                msg += f"🕐 {format_vn_time()}"
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
   
            if data.startswith("del_"):
                logger.info(f"🗑 Xóa giao dịch: {data}")
                tx_id_str = data.replace("del_", "")
                
                # Kiểm tra nếu là xóa danh mục (cat_) - đã xử lý ở nhóm 1
                if tx_id_str.startswith("cat_"):
                    return
                
                if not tx_id_str.isdigit():
                    await safe_edit_message(query, "❌ ID không hợp lệ!")
                    return
                
                tx_id = int(tx_id_str)
                
                # Kiểm tra giao dịch có tồn tại không
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT user_id, symbol, amount FROM portfolio WHERE id = ?''', (tx_id,))
                result = c.fetchone()
                conn.close()
                
                if not result:
                    await safe_edit_message(query, f"❌ Không tìm thấy giao dịch #{tx_id}")
                    return
                
                tx_owner_id, symbol, amount = result
                
                # Kiểm tra quyền xóa - chỉ cho phép chủ sở hữu hoặc admin
                can_delete = False
                can_delete = False
                if tx_owner_id == target_user_id:
                    can_delete = True
                elif is_admin and chat_type != 'private':  # Trong group mới được admin xóa
                    can_delete = True
                
                if not can_delete:
                    await safe_edit_message(query, "❌ Bạn không có quyền xóa giao dịch này!")
                    return
                
                # Hỏi xác nhận
                msg = (f"⚠️ *XÁC NHẬN XÓA*\n━━━━━━━━━━━━━━━━\n\n"
                       f"• Giao dịch: #{tx_id}\n"
                       f"• Coin: {symbol}\n"
                       f"• Số lượng: {amount:.4f}\n\n"
                       f"Bạn có chắc chắn muốn xóa?")
                
                keyboard = [[
                    InlineKeyboardButton("✅ Có", callback_data=f"confirm_del_{tx_id}"),
                    InlineKeyboardButton("❌ Không", callback_data=f"edit_{tx_id}")
                ]]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # ===========================================
            # NHÓM 9: XỬ LÝ MENU CHI TIÊU
            # ===========================================
            
            if data == "show_alerts":
                uid = query.from_user.id
                alerts = get_user_alerts(uid)
                
                if not alerts:
                    msg = f"📭 Bạn chưa có cảnh báo nào!\n\n🕐 {format_vn_time()}"
                    await safe_edit_message(query, msg)
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
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "show_top10":
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
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "show_buy":
                msg = ("➕ *MUA COIN*\n\nDùng lệnh: `/buy [coin] [sl] [giá]`\n\n"
                       "*Ví dụ:*\n• `/buy btc 0.5 40000`\n\n"
                       f"🕐 {format_vn_time_short()}")
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "show_sell":
                msg = ("➖ *BÁN COIN*\n\nDùng lệnh: `/sell [coin] [sl]`\n\n"
                       "*Ví dụ:*\n• `/sell btc 0.2`\n\n"
                       f"🕐 {format_vn_time_short()}")
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "admin_panel":
                uid = query.from_user.id
                group_id = query.message.chat.id
                
                msg = ("👑 *ADMIN PANEL*\n━━━━━━━━━━━━━━━━\n\n"
                       "• `/perm list` - Danh sách admin\n"
                       "• `/perm grant @user view` - Cấp quyền xem\n"
                       "• `/perm grant @user edit` - Cấp quyền sửa\n"
                       "• `/perm grant @user delete` - Cấp quyền xóa\n"
                       "• `/perm grant @user manage` - Cấp quyền QL\n"
                       "• `/perm revoke @user` - Thu hồi quyền\n\n"
                       f"🕐 {format_vn_time()}")
                
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "expense_income_menu":
                msg = ("💰 *MENU THU NHẬP*\n\n"
                       "• `tn [số tiền]` - Thêm thu nhập\n"
                       "• `tn 100 USD Lương` - Thêm 100 USD\n\n"
                       f"🕐 {format_vn_time_short()}")
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "expense_expense_menu":
                msg = ("💸 *MENU CHI TIÊU*\n\n"
                       "• `ct [mã] [số tiền]` - Thêm chi tiêu\n"
                       "• `ct 1 50000 VND Ăn trưa` - Ví dụ\n\n"
                       f"🕐 {format_vn_time_short()}")
                keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "expense_categories":
                categories = get_expense_categories(owner_id)
                
                if not categories:
                    msg = (f"📋 Chưa có danh mục nào!\n"
                           f"Tạo: `dm [tên] [budget]`\n\n"
                           f"🕐 {format_vn_time_short()}")
                    keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                
                msg = "📋 *DANH MỤC CHI TIÊU*\n━━━━━━━━━━━━━━━━\n\n"
                keyboard = []
                row = []
                
                for cat in categories:
                    cat_id, name, budget, created = cat
                    safe_name = escape_markdown(name)
                    msg += f"• *{cat_id}.* {safe_name} - {format_currency_simple(budget, 'VND')}\n"
                    
                    row.append(InlineKeyboardButton(f"🗑 {cat_id}", callback_data=f"del_cat_{cat_id}"))
                    if len(row) == 4:
                        keyboard.append(row)
                        row = []
                
                if row:
                    keyboard.append(row)
                
                keyboard.append([InlineKeyboardButton("➕ Thêm danh mục", callback_data="expense_expense_menu"),
                                 InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")])
                
                msg += f"\n🕐 {format_vn_time_short()}"
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            if data == "expense_today":
                try:
                    incomes_data = get_income_by_period(owner_id, 'day')
                    expenses_data = get_expenses_by_period(owner_id, 'day')
                    
                    msg = f"📅 *THU CHI HÔM NAY ({get_vn_time().strftime('%d/%m/%Y')})*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    if incomes_data['transactions']:
                        msg += "*💰 THU NHẬP:*\n"
                        for inc in incomes_data['transactions']:
                            id, amount, source, note, currency, date = inc
                            safe_source = escape_markdown(source)
                            safe_note = escape_markdown(note) if note else ""
                            
                            msg += f"• #{id}: {format_currency_simple(amount, currency)} - {safe_source}\n"
                            if safe_note:
                                msg += f"  📝 {safe_note}\n"
                        
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
                            safe_cat = escape_markdown(cat_name)
                            safe_note = escape_markdown(note) if note else ""
                            
                            msg += f"• #{id}: {format_currency_simple(amount, currency)} - {safe_cat}\n"
                            if safe_note:
                                msg += f"  📝 {safe_note}\n"
                        
                        msg += f"\n📊 *Tổng chi:*\n"
                        for currency, total in expenses_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                    else:
                        msg += "📭 Không có chi tiêu hôm nay."
                    
                    msg += f"\n\n🕐 {format_vn_time()}"
                    
                    if len(msg) > 4000:
                        await query.edit_message_text("📊 *Báo cáo quá dài, đang chia nhỏ...*")
                        chunks = [msg[i:i+3500] for i in range(0, len(msg), 3500)]
                        for i, chunk in enumerate(chunks, 1):
                            await query.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)
                    else:
                        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                        await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                        
                except Exception as e:
                    logger.error(f"Lỗi expense_today: {e}", exc_info=True)
                    await safe_edit_message(query, "❌ Có lỗi xảy ra khi xem hôm nay!")
                return
            
            if data == "expense_month":
                try:
                    incomes_data = get_income_by_period(owner_id, 'month')
                    expenses_data = get_expenses_by_period(owner_id, 'month')
                    
                    msg = f"📅 *THU CHI THÁNG {get_vn_time().strftime('%m/%Y')}*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    if incomes_data['transactions']:
                        msg += "*💰 THU NHẬP:*\n"
                        for inc in incomes_data['transactions'][:10]:
                            id, amount, source, note, currency, date = inc
                            safe_source = escape_markdown(source)
                            safe_note = escape_markdown(note) if note else ""
                            
                            msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {safe_source}\n"
                            if safe_note:
                                msg += f"  📝 {safe_note}\n"
                        
                        msg += f"\n📊 *Tổng thu:*\n"
                        for currency, total in incomes_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                        msg += f"  *Tổng số:* {incomes_data['total_count']} giao dịch\n\n"
                    else:
                        msg += "📭 Không có thu nhập.\n\n"
                    
                    if expenses_data['transactions']:
                        msg += "*💸 CHI TIÊU:*\n"
                        for exp in expenses_data['transactions'][:10]:
                            id, cat_name, amount, note, currency, date, budget = exp
                            safe_cat = escape_markdown(cat_name)
                            safe_note = escape_markdown(note) if note else ""
                            
                            msg += f"• #{id} {date}: {format_currency_simple(amount, currency)} - {safe_cat}\n"
                            if safe_note:
                                msg += f"  📝 {safe_note}\n"
                        
                        msg += f"\n📊 *Tổng chi:*\n"
                        for currency, total in expenses_data['summary'].items():
                            msg += f"  {format_currency_simple(total, currency)}\n"
                    else:
                        msg += "📭 Không có chi tiêu."
                    
                    msg += f"\n\n🕐 {format_vn_time()}"
                    
                    if len(msg) > 4000:
                        await query.edit_message_text("📊 *Báo cáo quá dài, đang chia nhỏ...*")
                        chunks = [msg[i:i+3500] for i in range(0, len(msg), 3500)]
                        for i, chunk in enumerate(chunks, 1):
                            await query.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)
                    else:
                        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                        await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                        
                except Exception as e:
                    logger.error(f"Lỗi expense_month: {e}", exc_info=True)
                    await safe_edit_message(query, "❌ Có lỗi xảy ra!")
                return
            
            if data == "expense_recent":
                try:
                    recent_incomes = get_recent_incomes(owner_id, 20)
                    recent_expenses = get_recent_expenses(owner_id, 20)
                    
                    if not recent_incomes and not recent_expenses:
                        msg = f"📭 Chưa có giao dịch nào!\n\n🕐 {format_vn_time_short()}"
                        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                        await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                        return
                    
                    msg = f"🔄 *20 GIAO DỊCH GẦN ĐÂY*\n━━━━━━━━━━━━━━━━\n\n"
                    
                    all_transactions = []
                    
                    for inc in recent_incomes:
                        id, amount, source, note, date, currency = inc
                        safe_source = escape_markdown(source)
                        safe_note = escape_markdown(note) if note else ""
                        desc = f"{format_currency_simple(amount, currency)} - {safe_source}"
                        all_transactions.append(('💰', id, date, desc, safe_note))
                    
                    for exp in recent_expenses:
                        id, cat_name, amount, note, date, currency = exp
                        safe_cat = escape_markdown(cat_name)
                        safe_note = escape_markdown(note) if note else ""
                        desc = f"{format_currency_simple(amount, currency)} - {safe_cat}"
                        all_transactions.append(('💸', id, date, desc, safe_note))
                    
                    all_transactions.sort(key=lambda x: x[2], reverse=True)
                    
                    for emoji, id, date, desc, note in all_transactions[:20]:
                        msg += f"{emoji} #{id} {date}: {desc}\n"
                        if note:
                            msg += f"   📝 {note}\n"
                    
                    msg += f"\n🕐 {format_vn_time_short()}"
                    
                    if len(msg) > 4000:
                        await query.edit_message_text("📊 *Danh sách quá dài, đang chia nhỏ...*")
                        chunks = [msg[i:i+3500] for i in range(0, len(msg), 3500)]
                        for i, chunk in enumerate(chunks, 1):
                            await query.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)
                    else:
                        keyboard = [[InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]]
                        await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                        
                except Exception as e:
                    logger.error(f"Lỗi expense_recent: {e}", exc_info=True)
                    await safe_edit_message(query, "❌ Có lỗi xảy ra!")
                return
            
            if data == "export_csv" or data == "expense_export":
                await export_csv_handler(update, ctx)
                return
    
            if data.startswith("balance_"):
                period = data.replace("balance_", "")
                
                balance_data = get_balance_summary(owner_id, period)
                
                if not balance_data:
                    msg = "❌ Không thể tính cân đối!"
                    await safe_edit_message(query, msg)
                    return
                
                balance_msg = format_balance_message(balance_data, "")
                
                keyboard = [
                    [InlineKeyboardButton("📅 Hôm nay", callback_data="balance_day"),
                     InlineKeyboardButton("📅 Tháng này", callback_data="balance_month")],
                    [InlineKeyboardButton("📅 Năm nay", callback_data="balance_year"),
                     InlineKeyboardButton("📊 Tất cả", callback_data="balance_all")],
                    [InlineKeyboardButton("🔙 Về menu", callback_data="back_to_expense")]
                ]
                
                await safe_edit_message(query, balance_msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return

            elif data == "export_secure":
                await query.edit_message_text(
                    "🔐 *XUẤT CSV CÓ MẬT KHẨU*\n\n"
                    "Dùng lệnh: `/export_secure [mật khẩu]`\n\n"
                    "*Ví dụ:*\n"
                    "• `/export_secure 123456`\n"
                    "• `/export_secure mysecretpass`\n\n"
                    "*Tính năng:*\n"
                    "• Mã hóa AES-256\n"
                    "• File ZIP có mật khẩu\n"
                    "• An toàn hơn CSV thường\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
                    ]])
                )
                return
            # ===========================================
            # XỬ LÝ XUẤT BÁO CÁO CHI TIÊU
            # ===========================================
            if data == "export_expense_menu":
                await safe_edit_message(
                    query,
                    "🔐 *XUẤT BÁO CÁO CHI TIÊU MASTER*\n\n"
                    "Dùng lệnh: `/export_expense [mật khẩu]`\n\n"
                    "*Ví dụ:*\n"
                    "• `/export_expense 123456` - File ZIP có mật khẩu (tự xóa sau 30s)\n"
                    "• `/export_expense 0` - File CSV không mã hóa\n\n"
                    "*Báo cáo bao gồm TẤT CẢ:*\n"
                    "📊 Danh sách thu nhập chi tiết\n"
                    "📊 Danh sách chi tiêu chi tiết\n"
                    "📋 Phân tích theo danh mục\n"
                    "⚖️ Cân đối theo loại tiền\n"
                    "📅 Phân tích theo tháng\n"
                    "💡 Đánh giá budget & khuyến nghị\n\n"
                    f"🕐 {format_vn_time_short()}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Về menu chi tiêu", callback_data="back_to_expense")
                    ]])
                )
                return

            # ===========================================
            # NHÓM 11: MENU CÀI ĐẶT - QUẢN LÝ PHÂN QUYỀN
            # ===========================================
            
            elif data == "settings_members":
                await query.edit_message_text("🔄 Đang tải danh sách thành viên...")
                
                try:
                    # Lấy danh sách thành viên từ Telegram
                    admins = await ctx.bot.get_chat_administrators(chat_id)
                    
                    # Tạo message và keyboard
                    msg_lines = ["👥 *DANH SÁCH THÀNH VIÊN*", "━━━━━━━━━━━━━━━━\n"]
                    keyboard = []
                    row = []
                    
                    human_count = 0
                    bot_count = 0
                    
                    for admin in admins:
                        user = admin.user
                        
                        # Lấy quyền hiện tại từ DB (nếu là người)
                        perm = None
                        if not user.is_bot:
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                                        FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, user.id))
                            perm = c.fetchone()
                            conn.close()
                        
                        # Xác định icon và tên hiển thị
                        if user.is_bot:
                            icon = "🤖"  # Bot
                            bot_count += 1
                        elif admin.status == 'creator':
                            icon = "👑"  # Chủ sở hữu
                            human_count += 1
                        elif perm:
                            icon = "🔰"  # Đã được cấp quyền
                            human_count += 1
                        else:
                            icon = "👤"  # Thành viên thường
                            human_count += 1
                        
                        display_name = f"@{user.username}" if user.username else user.first_name or "No name"
                        msg_lines.append(f"{icon} {display_name} (`{user.id}`)")
                        
                        # Chỉ thêm nút quản lý nếu KHÔNG phải bot và user hiện tại có quyền manage
                        if not user.is_bot and check_permission(chat_id, query.from_user.id, 'manage'):
                            btn_text = f"⚙️ {user.first_name[:10] if user.first_name else 'User'}"
                            if user.first_name and len(user.first_name) > 10:
                                btn_text = f"⚙️ {user.first_name[:8]}..."
                            
                            row.append(InlineKeyboardButton(btn_text, callback_data=f"perm_user_{user.id}"))
                            
                            if len(row) == 2:
                                keyboard.append(row)
                                row = []
                    
                    # Thêm hàng cuối cùng nếu còn
                    if row:
                        keyboard.append(row)
                    
                    # Thêm thống kê
                    msg_lines.append("")
                    msg_lines.append(f"📊 *Tổng số:* {len(admins)} thành viên")
                    msg_lines.append(f"   • 👤 Người: {human_count}")
                    msg_lines.append(f"   • 🤖 Bot: {bot_count}")
                    
                    # Nút quay lại
                    keyboard.append([InlineKeyboardButton("🔙 Về cài đặt", callback_data="back_to_settings")])
                    
                    msg = "\n".join(msg_lines) + f"\n\n🕐 {format_vn_time_short()}"
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    return
                    
                except Exception as e:
                    logger.error(f"❌ Lỗi settings_members: {e}")
                    await safe_edit_message(query, "❌ Không thể tải danh sách thành viên!")
                    return
            
            elif data == "settings_permissions":
                msg = (
                    "🔐 *HƯỚNG DẪN PHÂN QUYỀN*\n"
                    "━━━━━━━━━━━━━━━━\n\n"
                    "👁 *XEM*\n"
                    "• Xem giá coin, portfolio, lợi nhuận\n\n"
                    "✏️ *SỬA*\n"
                    "• Thêm/sửa giao dịch (bao gồm quyền XEM)\n\n"
                    "🗑 *XOÁ*\n"
                    "• Xóa giao dịch (bao gồm quyền XEM)\n\n"
                    "🔐 *QUẢN LÝ*\n"
                    "• Cấp/thu hồi quyền cho người khác\n"
                    "• Bao gồm TẤT CẢ quyền trên\n\n"
                    "⚡ *Cách thực hiện:*\n"
                    "1️⃣ Chọn *QUẢN LÝ THÀNH VIÊN*\n"
                    "2️⃣ Chọn người cần cấp quyền\n"
                    "3️⃣ Tick vào các quyền muốn cấp\n"
                    "4️⃣ Nhấn *LƯU THAY ĐỔI*"
                )
                
                keyboard = [
                    [InlineKeyboardButton("👥 QUẢN LÝ THÀNH VIÊN", callback_data="settings_members")],
                    [InlineKeyboardButton("🔙 Về cài đặt", callback_data="back_to_settings")]
                ]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            elif data == "settings_list":
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''
                    SELECT p.user_id, p.can_view_all, p.can_edit_all, p.can_delete_all, p.can_manage_perms,
                           u.username, u.first_name 
                    FROM permissions p 
                    LEFT JOIN users u ON p.user_id = u.user_id 
                    WHERE p.group_id = ?
                    ORDER BY p.created_at
                ''', (chat_id,))
                permissions = c.fetchall()
                conn.close()
                
                if not permissions:
                    msg = "📋 *DANH SÁCH QUYỀN*\n━━━━━━━━━━━━━━━━\n\nChưa có ai được cấp quyền đặc biệt."
                else:
                    msg_lines = ["📋 *DANH SÁCH QUYỀN HIỆN TẠI*", "━━━━━━━━━━━━━━━━\n"]
                    
                    for p in permissions:
                        user_id, view, edit, delete, manage, username, first_name = p
                        name = f"@{username}" if username else first_name or f"User {user_id}"
                        
                        perms = []
                        if view: perms.append("👁")
                        if edit: perms.append("✏️")
                        if delete: perms.append("🗑")
                        if manage: perms.append("🔐")
                        
                        perms_display = ' '.join(perms) if perms else '❌'
                        msg_lines.append(f"• {name}: {perms_display}")
                    
                    msg = "\n".join(msg_lines)
                
                msg += f"\n\n🕐 {format_vn_time_short()}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về cài đặt", callback_data="back_to_settings")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            elif data == "settings_sync":
                await query.edit_message_text("🔄 Đang đồng bộ danh sách admin từ Telegram...")
                
                try:
                    admins = await ctx.bot.get_chat_administrators(chat_id)
                    synced = 0
                    updated = 0
                    
                    for admin in admins:
                        if admin.user and not admin.user.is_bot:
                            await update_user_info_async(admin.user)
                            
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute('''SELECT id FROM permissions WHERE group_id = ? AND user_id = ?''', 
                                     (chat_id, admin.user.id))
                            exists = c.fetchone()
                            
                            if not exists:
                                # Tự động cấp quyền cho admin Telegram
                                perms = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 0}
                                if admin.status == 'creator':
                                    perms = {'view': 1, 'edit': 1, 'delete': 1, 'manage': 1}
                                
                                c.execute('''
                                    INSERT INTO permissions 
                                    (group_id, user_id, granted_by, is_approved, role, 
                                     can_view_all, can_edit_all, can_delete_all, can_manage_perms,
                                     created_at, approved_at) 
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (chat_id, admin.user.id, chat_id, 1, 'staff',
                                      perms['view'], perms['edit'], perms['delete'], perms['manage'],
                                      get_vn_time().strftime("%Y-%m-%d %H:%M:%S"),
                                      get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
                                synced += 1
                            else:
                                updated += 1
                            
                            conn.commit()
                            conn.close()
                    
                    msg = (
                        f"✅ *ĐỒNG BỘ THÀNH CÔNG*\n"
                        f"━━━━━━━━━━━━━━━━\n\n"
                        f"📊 *Kết quả:*\n"
                        f"• Tổng số admin: {len(admins)}\n"
                        f"• Đã cấp quyền mới: {synced}\n"
                        f"• Đã cập nhật: {updated}\n\n"
                        f"🕐 {format_vn_time()}"
                    )
                except Exception as e:
                    logger.error(f"❌ Lỗi đồng bộ: {e}")
                    msg = f"❌ Lỗi: {str(e)[:100]}"
                
                keyboard = [[InlineKeyboardButton("🔙 Về cài đặt", callback_data="back_to_settings")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return

            elif data.startswith("lang_"):
                lang = data.replace("lang_", "")
                user_id = query.from_user.id
                current_lang = get_lang(user_id)
                
                # Lưu ngôn ngữ mới
                if lang == 'vi':
                    LANGUAGE[user_id] = 'VI'
                    success_msg = "✅ Đã chuyển sang Tiếng Việt!"
                elif lang == 'zh':
                    LANGUAGE[user_id] = 'ZH'
                    success_msg = "✅ 已切换到中文！"
                else:
                    await safe_edit_message(query, "❌ Ngôn ngữ không hợp lệ!")
                    return
                
                # QUAN TRỌNG: Chỉ cập nhật message hiện tại, không tạo mới
                # Hiển thị thông báo thành công và nút quay lại
                keyboard = [[InlineKeyboardButton("🔙 Quay lại cài đặt", callback_data="back_to_settings")]]
                await safe_edit_message(query, success_msg, reply_markup=InlineKeyboardMarkup(keyboard))
                
                # Không gửi message mới, chỉ cập nhật message cũ
                return
                
            elif data == "lang_menu":
                user_id = query.from_user.id
                current_lang = get_lang(user_id)
                
                # Xác định ngôn ngữ hiện tại để hiển thị
                if current_lang == 'VI':
                    current_display = "🇻🇳 Tiếng Việt"
                else:
                    current_display = "🇨🇳 中文"
                
                keyboard = [
                    [InlineKeyboardButton("🇻🇳 Tiếng Việt", callback_data="lang_vi")],
                    [InlineKeyboardButton("🇨🇳 中文", callback_data="lang_zh")],
                    [InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_settings")]
                ]
                
                msg = (f"🌐 *CHỌN NGÔN NGỮ*\n"
                       f"━━━━━━━━━━━━━━━━\n\n"
                       f"Ngôn ngữ hiện tại: {current_display}\n\n"
                       f"Vui lòng chọn ngôn ngữ mới:")
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
        
            elif data == "back_to_settings":
                lang = get_lang(query.from_user.id)
                
                if lang == 'ZH':
                    keyboard = [
                        [InlineKeyboardButton("👥 成员管理", callback_data="settings_members")],
                        [InlineKeyboardButton("🔐 权限说明", callback_data="settings_permissions")],
                        [InlineKeyboardButton("📋 权限列表", callback_data="settings_list")],
                        [InlineKeyboardButton("🔄 同步管理员", callback_data="settings_sync")],
                        [InlineKeyboardButton("🌐 语言", callback_data="lang_menu")],
                        [InlineKeyboardButton("🔙 主菜单", callback_data="back_to_main")]
                    ]
                    msg = ("⚙️ *群组设置*\n"
                           "━━━━━━━━━━━━━━━━\n\n"
                           "请选择管理功能:\n\n"
                           f"🕐 {format_vn_time()}")
                else:
                    keyboard = [
                        [InlineKeyboardButton("👥 QUẢN LÝ THÀNH VIÊN", callback_data="settings_members")],
                        [InlineKeyboardButton("🔐 HƯỚNG DẪN QUYỀN", callback_data="settings_permissions")],
                        [InlineKeyboardButton("📋 DANH SÁCH QUYỀN", callback_data="settings_list")],
                        [InlineKeyboardButton("🔄 ĐỒNG BỘ ADMIN", callback_data="settings_sync")],
                        [InlineKeyboardButton("🌐 NGÔN NGỮ", callback_data="lang_menu")],
                        [InlineKeyboardButton("🔙 VỀ MENU CHÍNH", callback_data="back_to_main")]
                    ]
                    msg = ("⚙️ *CÀI ĐẶT NHÓM*\n"
                           "━━━━━━━━━━━━━━━━\n\n"
                           "Chọn chức năng quản lý:\n\n"
                           f"🕐 {format_vn_time()}")
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            elif data.startswith("perm_user_"):
                target_id = int(data.replace("perm_user_", ""))
                
                # Không cho tự quản lý
                if target_id == query.from_user.id:
                    await safe_edit_message(query, "❌ Bạn không thể tự phân quyền cho chính mình!")
                    return
                
                # Lấy thông tin user
                try:
                    chat = await ctx.bot.get_chat(target_id)
                    name = f"@{chat.username}" if chat.username else chat.first_name
                except:
                    name = f"User {target_id}"
                
                # Lấy quyền hiện tại
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                            FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, target_id))
                current = c.fetchone()
                conn.close()
                
                view = current[0] if current else 0
                edit = current[1] if current else 0
                delete = current[2] if current else 0
                manage = current[3] if current else 0
                
                # Lưu tạm vào bot_data
                ctx.bot_data[f"temp_perm_{target_id}"] = (view, edit, delete, manage)
                
                # Hiển thị giao diện phân quyền
                msg = (
                    f"🔐 *PHÂN QUYỀN CHO {name}*\n"
                    f"━━━━━━━━━━━━━━━━\n\n"
                    f"User ID: `{target_id}`\n\n"
                    f"*Quyền hiện tại:*\n"
                    f"{'✅' if view else '⬜'} 👁 XEM\n"
                    f"{'✅' if edit else '⬜'} ✏️ SỬA\n"
                    f"{'✅' if delete else '⬜'} 🗑 XÓA\n"
                    f"{'✅' if manage else '⬜'} 🔐 QUẢN LÝ\n\n"
                    f"*Chọn để thay đổi:*"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton(f"{'✅' if view else '⬜'} 👁", callback_data=f"perm_toggle_{target_id}_view"),
                        InlineKeyboardButton(f"{'✅' if edit else '⬜'} ✏️", callback_data=f"perm_toggle_{target_id}_edit"),
                        InlineKeyboardButton(f"{'✅' if delete else '⬜'} 🗑", callback_data=f"perm_toggle_{target_id}_delete"),
                        InlineKeyboardButton(f"{'✅' if manage else '⬜'} 🔐", callback_data=f"perm_toggle_{target_id}_manage")
                    ],
                    [
                        InlineKeyboardButton("👑 FULL", callback_data=f"perm_set_{target_id}_full"),
                        InlineKeyboardButton("❌ XÓA HẾT", callback_data=f"perm_set_{target_id}_none")
                    ],
                    [InlineKeyboardButton("💾 LƯU", callback_data=f"perm_save_{target_id}")],
                    [InlineKeyboardButton("🔙 Quay lại", callback_data="settings_members")]
                ]
                
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            elif data.startswith("perm_toggle_"):
                parts = data.split("_")
                target_id = int(parts[2])
                perm_type = parts[3]
                
                # Lấy quyền tạm thời
                key = f"temp_perm_{target_id}"
                temp = ctx.bot_data.get(key)
                
                if temp:
                    view, edit, delete, manage = temp
                else:
                    # Fallback: lấy từ DB
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute('''SELECT can_view_all, can_edit_all, can_delete_all, can_manage_perms 
                                FROM permissions WHERE group_id = ? AND user_id = ?''', (chat_id, target_id))
                    current = c.fetchone()
                    conn.close()
                    view = current[0] if current else 0
                    edit = current[1] if current else 0
                    delete = current[2] if current else 0
                    manage = current[3] if current else 0
                
                # Toggle
                if perm_type == 'view':
                    view = 1 - view
                elif perm_type == 'edit':
                    edit = 1 - edit
                elif perm_type == 'delete':
                    delete = 1 - delete
                elif perm_type == 'manage':
                    manage = 1 - manage
                
                # Lưu lại
                ctx.bot_data[key] = (view, edit, delete, manage)
                
                # Cập nhật message
                await update_perm_message(query, ctx, target_id, view, edit, delete, manage)
                return
            
            elif data.startswith("perm_set_"):
                parts = data.split("_")
                target_id = int(parts[2])
                preset = parts[3]
                
                if preset == 'full':
                    view = edit = delete = manage = 1
                elif preset == 'none':
                    view = edit = delete = manage = 0
                
                ctx.bot_data[f"temp_perm_{target_id}"] = (view, edit, delete, manage)
                await update_perm_message(query, ctx, target_id, view, edit, delete, manage)
                return
            
            elif data.startswith("perm_save_"):
                target_id = int(data.replace("perm_save_", ""))
                key = f"temp_perm_{target_id}"
                temp = ctx.bot_data.get(key)
                
                if not temp:
                    await safe_edit_message(query, "ℹ️ Không có thay đổi nào để lưu!")
                    return
                
                view, edit, delete, manage = temp
                
                # Lưu vào database
                permissions = {'view': view, 'edit': edit, 'delete': delete, 'manage': manage}
                
                if grant_permission(chat_id, target_id, query.from_user.id, permissions):
                    # Xóa temp
                    if key in ctx.bot_data:
                        del ctx.bot_data[key]
                    
                    # Lấy tên user
                    try:
                        chat = await ctx.bot.get_chat(target_id)
                        name = f"@{chat.username}" if chat.username else chat.first_name
                    except:
                        name = f"User {target_id}"
                    
                    # Tạo message thông báo
                    perms = []
                    if view: perms.append("👁 Xem")
                    if edit: perms.append("✏️ Sửa")
                    if delete: perms.append("🗑 Xóa")
                    if manage: perms.append("🔐 Quản lý")
                    
                    msg = (
                        f"✅ *ĐÃ LƯU QUYỀN CHO {name}*\n"
                        f"━━━━━━━━━━━━━━━━\n\n"
                        f"Quyền được cấp: {', '.join(perms) if perms else '❌ Không có'}\n\n"
                        f"🕐 {format_vn_time()}"
                    )
                    
                    keyboard = [[InlineKeyboardButton("🔙 Quay lại danh sách", callback_data="settings_members")]]
                    await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                    
                    # Thông báo cho user được cấp quyền
                    try:
                        await ctx.bot.send_message(
                            target_id,
                            f"🔔 *THÔNG BÁO QUYỀN TRONG NHÓM*\n\n"
                            f"Bạn đã được cấp quyền trong nhóm:\n"
                            f"• {query.message.chat.title}\n"
                            f"• Quyền: {', '.join(perms) if perms else '❌ Không có'}\n\n"
                            f"🕐 {format_vn_time()}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except:
                        pass
                else:
                    await safe_edit_message(query, "❌ Lỗi khi lưu quyền!")
                
                return

            # ===========================================
            # XỬ LÝ CALLBACK LIÊN QUAN ĐẾN SELL HISTORY
            # ===========================================
            
            if data.startswith("sell_detail_"):
                sell_id = int(data.replace("sell_detail_", ""))
                sell = get_sell_detail(sell_id, target_user_id)
                
                if not sell:
                    await safe_edit_message(query, f"❌ Không tìm thấy lệnh bán #{sell_id}")
                    return
                
                id, user, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_pct, sell_date, created = sell
                
                msg = (f"📝 *LỆNH BÁN #{sell_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📅 Ngày bán: {sell_date}\n"
                       f"📊 Số lượng: `{amount:.4f}`\n"
                       f"💰 Giá bán: `{fmt_price(sell_price)}`\n"
                       f"💵 Giá vốn: `{fmt_price(buy_price)}`\n"
                       f"💎 Giá trị bán: `{fmt_price(total_sold)}`\n"
                       f"{'✅' if profit>=0 else '❌'} Lợi nhuận: `{fmt_price(profit)}` ({profit_pct:+.2f}%)\n\n"
                       f"🕐 {format_vn_time()}")
                
                keyboard = [[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_invest")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return

            if data.startswith("del_sell_"):
                sell_id = int(data.replace("del_sell_", ""))
                
                keyboard = [[
                    InlineKeyboardButton("✅ Xác nhận", callback_data=f"confirm_del_sell_{sell_id}"),
                    InlineKeyboardButton("❌ Hủy", callback_data="cancel_del_sell")
                ]]
                
                await safe_edit_message(query, f"⚠️ *Xác nhận xóa lệnh bán #{sell_id}?*", 
                                       reply_markup=InlineKeyboardMarkup(keyboard))
                return

            if data.startswith("confirm_del_sell_"):
                sell_id = int(data.replace("confirm_del_sell_", ""))
                
                if delete_sell_history(sell_id, target_user_id):
                    await safe_edit_message(query, f"✅ *Đã xóa lệnh bán #{sell_id}*")
                else:
                    await safe_edit_message(query, f"❌ Không thể xóa lệnh bán #{sell_id}")
                return

            if data == "cancel_del_sell":
                await safe_edit_message(query, "❌ Đã hủy xóa.")
                return

            if data.startswith("edit_sell_"):
                sell_id = int(data.replace("edit_sell_", ""))
                sell = get_sell_detail(sell_id, target_user_id)
                
                if not sell:
                    await safe_edit_message(query, f"❌ Không tìm thấy lệnh bán #{sell_id}")
                    return
                
                id, user, symbol, amount, sell_price, buy_price, total_sold, total_cost, profit, profit_pct, sell_date, created = sell
                
                msg = (f"✏️ *SỬA LỆNH BÁN #{sell_id}*\n━━━━━━━━━━━━━━━━\n\n"
                       f"*{symbol}*\n"
                       f"📊 SL hiện tại: `{amount:.4f}`\n"
                       f"💰 Giá hiện tại: `{fmt_price(sell_price)}`\n\n"
                       f"*Nhập lệnh:*\n"
                       f"`/editsell {sell_id} [số lượng mới] [giá mới]`\n\n"
                       f"*Ví dụ:*\n"
                       f"`/editsell {sell_id} 0.3 50000`\n\n"
                       f"🕐 {format_vn_time_short()}")
                
                keyboard = [[InlineKeyboardButton("🔙 Quay lại", callback_data="back_to_invest")]]
                await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))
                return
                
            # ===========================================
            # XỬ LÝ CALLBACK KHÔNG XÁC ĐỊNH
            # ===========================================
            
            logger.warning(f"⚠️ Callback không xác định: {data}")
            await safe_edit_message(query, "❌ Chức năng chưa được hỗ trợ!")
            
        except Exception as e:
            logger.error(f"❌ LỖI CALLBACK: {e}", exc_info=True)
            logger.error(f"   • Data gây lỗi: {data}")
            logger.error(f"   • User: {query.from_user.id}")
            
            try:
                await query.edit_message_text(
                    "❌ Có lỗi xảy ra, vui lòng thử lại sau.", 
                    parse_mode=None
                )
            except:
                try:
                    await query.message.reply_text("❌ Có lỗi xảy ra.")
                except:
                    pass

    async def handle_sell_confirmation(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Xử lý xác nhận bán coin"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "cancel_sell":
            await query.edit_message_text(
                "❌ Đã hủy lệnh bán.", 
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Về menu", callback_data="back_to_invest")
                ]])
            )
            return
        
        if query.data.startswith("confirm_sell_"):
            # Parse dữ liệu
            parts = query.data.replace("confirm_sell_", "").split("_")
            
            if len(parts) < 3:
                await query.edit_message_text("❌ Dữ liệu không hợp lệ!")
                return
                
            symbol = parts[0]
            try:
                sell_amount = float(parts[1])
                sell_price = float(parts[2])
            except ValueError:
                await query.edit_message_text("❌ Số liệu không hợp lệ!")
                return
            
            current_user_id = query.from_user.id
            chat_type = query.message.chat.type
            chat_id = query.message.chat.id
            
            # Xác định target user
            if chat_type == 'private':
                target_user_id = current_user_id
            else:
                owner_id = get_group_owner(chat_id)
                is_admin = check_permission(chat_id, current_user_id, 'edit')
                target_user_id = owner_id if is_admin else current_user_id
            
            # Lấy portfolio
            portfolio_data = get_portfolio(target_user_id)
            portfolio = []
            for row in portfolio_data:
                portfolio.append({
                    'symbol': row[0], 
                    'amount': row[1], 
                    'buy_price': row[2], 
                    'buy_date': row[3], 
                    'total_cost': row[4]
                })
            
            # Lấy giá hiện tại
            price_data = get_price(symbol)
            current_price = price_data['p'] if price_data else 0
            
            await query.edit_message_text("🔄 Đang xử lý lệnh bán...")
            
            # Thực hiện bán
            await execute_sell(
                update, ctx, target_user_id, symbol, 
                sell_amount, sell_price, current_price, 
                portfolio, current_user_id
            )
                    
    # ==================== WEBHOOK SETUP ====================
    async def setup_webhook():
        try:
            if not render_config.render_url:
                logger.warning("⚠️ Không có RENDER_EXTERNAL_URL, dùng polling")
                return False
            
            webhook_url = f"{render_config.render_url}/webhook"
            
            await app.bot.delete_webhook(drop_pending_updates=True)
            
            await app.bot.set_webhook(url=webhook_url, allowed_updates=['message', 'callback_query'], drop_pending_updates=True, max_connections=render_config.get_worker_count())
            
            webhook_info = await app.bot.get_webhook_info()
            logger.info(f"✅ Webhook set: {webhook_url}")
            logger.info(f"📊 Pending updates: {webhook_info.pending_update_count}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi setup webhook: {e}")
            return False

    @webhook_app.route('/webhook', methods=['POST'])
    def webhook():
        try:
            update = Update.de_json(request.get_json(force=True), app.bot)
            asyncio.run_coroutine_threadsafe(app.process_update(update), app.loop)
            return 'OK', 200
        except Exception as e:
            logger.error(f"❌ Webhook error: {e}")
            return 'Error', 500

    @webhook_app.route('/health', methods=['GET'])
    def health():
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
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
        port = int(os.environ.get('PORT', 10000))
        logger.info(f"🌐 Starting webhook server on port {port}")
        webhook_app.run(host='0.0.0.0', port=port, threaded=True)

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
        try:
            port = int(os.environ.get('PORT', 10000))
            server = HTTPServer(('0.0.0.0', port), EnhancedHealthHandler)
            logger.info(f"✅ Health server on port {port}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"❌ Health server error: {e}")
            time.sleep(10)

    # ==================== MULTI-GROUP MANAGEMENT SYSTEM ====================

    # Danh sách tính năng có thể bật/tắt
    FEATURE_CATALOG = {
        "kick_mute":      {"name": "Kick/Mute thành viên",   "group": "🛡️ Quản lý nhóm"},
        "welcome":        {"name": "Chào mừng thành viên",   "group": "🛡️ Quản lý nhóm"},
        "anti_spam":      {"name": "Chống spam",             "group": "🛡️ Quản lý nhóm"},
        "filter_kw":      {"name": "Lọc từ khóa",            "group": "🛡️ Quản lý nhóm"},
        "del_msg":        {"name": "Xóa tin nhắn",           "group": "🛡️ Quản lý nhóm"},
        "pin_msg":        {"name": "Ghim tin nhắn",          "group": "🛡️ Quản lý nhóm"},
        "poll":           {"name": "Tạo poll",               "group": "🛡️ Quản lý nhóm"},
        "crypto_view":    {"name": "Xem giá coin",           "group": "💰 Crypto"},
        "crypto_buy":     {"name": "Mua coin",               "group": "💰 Crypto"},
        "crypto_sell":    {"name": "Bán coin",               "group": "💰 Crypto"},
        "crypto_alert":   {"name": "Cảnh báo giá",           "group": "💰 Crypto"},
        "crypto_port":    {"name": "Xem portfolio",          "group": "💰 Crypto"},
        "crypto_profit":  {"name": "Tính lợi nhuận",         "group": "💰 Crypto"},
        "crypto_export":  {"name": "Xuất báo cáo crypto",    "group": "💰 Crypto"},
        "expense_add":    {"name": "Ghi thu/chi",            "group": "💸 Thu chi"},
        "expense_cat":    {"name": "Quản lý danh mục",       "group": "💸 Thu chi"},
        "expense_budget": {"name": "Đặt ngân sách",          "group": "💸 Thu chi"},
        "expense_view":   {"name": "Xem cân đối",            "group": "💸 Thu chi"},
        "expense_report": {"name": "Báo cáo thu chi",        "group": "💸 Thu chi"},
        "expense_export": {"name": "Xuất Excel/PDF",         "group": "💸 Thu chi"},
        "broadcast_recv": {"name": "Nhận TB từ nhóm tổng",  "group": "📢 Thông báo"},
        "broadcast_send": {"name": "Gửi TB nội bộ",         "group": "📢 Thông báo"},
        "alert_market":   {"name": "Cảnh báo biến động TT", "group": "📢 Thông báo"},
        "report_sched":   {"name": "Báo cáo định kỳ",       "group": "📢 Thông báo"},
    }

    AUTONOMY_PRESETS = {
        0: [],
        1: ["crypto_view", "crypto_buy", "crypto_sell", "crypto_port",
            "expense_add", "expense_view", "broadcast_recv"],
        2: ["crypto_view", "crypto_buy", "crypto_sell", "crypto_port", "crypto_alert",
            "crypto_profit", "expense_add", "expense_cat", "expense_view", "expense_report",
            "kick_mute", "welcome", "anti_spam", "del_msg",
            "broadcast_recv", "broadcast_send", "report_sched"],
        3: list(FEATURE_CATALOG.keys()),
    }

    # ── DB Helpers ──────────────────────────────────────────────────

    def mg_set_master(group_id, group_name, set_by):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO master_groups (group_id, group_name, set_by, created_at)
                         VALUES (?, ?, ?, ?)''',
                      (group_id, group_name, set_by, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ mg_set_master: {e}"); return False

    def mg_is_master(group_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT group_id FROM master_groups WHERE group_id = ?", (group_id,))
            r = c.fetchone(); conn.close(); return r is not None
        except: return False

    def mg_get_master_of_child(child_group_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT master_group_id FROM group_hierarchy WHERE child_group_id = ?", (child_group_id,))
            r = c.fetchone(); conn.close()
            return r[0] if r else None
        except: return None

    def mg_add_child(master_id, child_id, child_name, level, added_by):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_hierarchy
                         (master_group_id, child_group_id, child_group_name, autonomy_level, added_by, created_at)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (master_id, child_id, child_name, level, added_by,
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
            mg_apply_preset(child_id, level, added_by)
            return True
        except Exception as e:
            logger.error(f"❌ mg_add_child: {e}"); return False

    def mg_remove_child(master_id, child_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM group_hierarchy WHERE master_group_id=? AND child_group_id=?",
                      (master_id, child_id))
            conn.commit(); conn.close(); return True
        except Exception as e:
            logger.error(f"❌ mg_remove_child: {e}"); return False

    def mg_get_children(master_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT child_group_id, child_group_name, autonomy_level, created_at
                         FROM group_hierarchy WHERE master_group_id=? ORDER BY autonomy_level DESC''', (master_id,))
            rows = c.fetchall(); conn.close(); return rows
        except: return []

    def mg_set_feature(group_id, feature_key, is_enabled, set_by):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO group_features (group_id, feature_key, is_enabled, set_by, updated_at)
                         VALUES (?, ?, ?, ?, ?)''',
                      (group_id, feature_key, 1 if is_enabled else 0, set_by,
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close(); return True
        except Exception as e:
            logger.error(f"❌ mg_set_feature: {e}"); return False

    def mg_has_feature(group_id, feature_key):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT is_enabled FROM group_features WHERE group_id=? AND feature_key=?",
                      (group_id, feature_key))
            r = c.fetchone(); conn.close()
            return r is not None and r[0] == 1
        except: return False

    def mg_get_features(group_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT feature_key, is_enabled FROM group_features WHERE group_id=?", (group_id,))
            rows = c.fetchall(); conn.close()
            return {row[0]: bool(row[1]) for row in rows}
        except: return {}

    def mg_apply_preset(group_id, level, set_by):
        enabled = AUTONOMY_PRESETS.get(level, [])
        for key in FEATURE_CATALOG:
            mg_set_feature(group_id, key, key in enabled, set_by)
        logger.info(f"✅ Preset Lv{level} → group {group_id}: {len(enabled)} features ON")

    def mg_cross_ban(master_id, banned_user_id, banned_by, reason=""):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO cross_bans
                         (master_group_id, banned_user_id, banned_by, reason, banned_at, is_active)
                         VALUES (?, ?, ?, ?, ?, 1)''',
                      (master_id, banned_user_id, banned_by, reason,
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close(); return True
        except Exception as e:
            logger.error(f"❌ mg_cross_ban: {e}"); return False

    def mg_cross_unban(master_id, banned_user_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE cross_bans SET is_active=0 WHERE master_group_id=? AND banned_user_id=?",
                      (master_id, banned_user_id))
            conn.commit(); conn.close(); return True
        except Exception as e:
            logger.error(f"❌ mg_cross_unban: {e}"); return False

    def mg_is_cross_banned(group_id, user_id):
        master_id = mg_get_master_of_child(group_id)
        if not master_id:
            master_id = group_id if mg_is_master(group_id) else None
        if not master_id:
            return False
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT id FROM cross_bans WHERE master_group_id=? AND banned_user_id=? AND is_active=1''',
                      (master_id, user_id))
            r = c.fetchone(); conn.close(); return r is not None
        except: return False

    def mg_get_ban_list(master_id):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT banned_user_id, banned_by, reason, banned_at FROM cross_bans
                         WHERE master_group_id=? AND is_active=1 ORDER BY banned_at DESC''', (master_id,))
            rows = c.fetchall(); conn.close(); return rows
        except: return []

    async def mg_broadcast(context, master_id, message, sent_by, target_ids=None, photo_id=None, caption=None):
        """Gửi broadcast text hoặc ảnh+caption đến các nhóm con"""
        children = mg_get_children(master_id)
        targets = [(cid, cname) for cid, cname, _, _ in children
                   if (target_ids is None or cid in target_ids) and mg_has_feature(cid, "broadcast_recv")]
        success = fail = 0
        header = "📢 *THÔNG BÁO TỪ NHÓM TỔNG*\n━━━━━━━━━━━━━━━━\n\n"
        footer = f"\n\n🕐 {format_vn_time()}"
        for child_id, child_name in targets:
            try:
                if photo_id:
                    # Gửi ảnh kèm caption
                    cap = header + (caption or "") + footer
                    await context.bot.send_photo(
                        chat_id=child_id,
                        photo=photo_id,
                        caption=cap,
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    full_msg = header + message + footer
                    await context.bot.send_message(
                        chat_id=child_id,
                        text=full_msg,
                        parse_mode=ParseMode.MARKDOWN
                    )
                success += 1
            except Exception as e:
                fail += 1
                logger.error(f"❌ Broadcast failed → {child_name}: {e}")
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO broadcasts (master_group_id, message, sent_by, target_groups, sent_at, success_count, fail_count)
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (master_id, caption or message, sent_by, ",".join(str(t[0]) for t in targets),
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S"), success, fail))
            conn.commit(); conn.close()
        except Exception as e:
            logger.error(f"❌ Lưu broadcast history: {e}")
        return success, fail

    # ── Commands ────────────────────────────────────────────────────

    async def mg_setmaster_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setmaster — Đặt nhóm hiện tại làm nhóm tổng (chỉ owner bot)"""
        user_id = update.effective_user.id
        chat = update.effective_chat
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ owner bot mới dùng lệnh này!"); return
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm!"); return
        # Kiểm tra nếu nhóm đang là nhóm CON của master khác
        existing_master = mg_get_master_of_child(chat.id)
        if existing_master:
            await update.message.reply_text(
                f"❌ Nhóm này đang là nhóm CON của master {existing_master}!\n"
                "Dùng /removechild ở nhóm tổng trước."
            ); return
        # Nếu đã là master rồi thì thông báo
        if mg_is_master(chat.id):
            await update.message.reply_text(
                "⚠️ Nhóm này đã là nhóm tổng rồi!\n"
                "Dùng /masterinfo để xem thông tin."
            ); return
        group_name = chat.title or f"Group {chat.id}"
        if mg_set_master(chat.id, group_name, user_id):
            await update.message.reply_text(
                f"✅ *ĐÃ ĐẶT NHÓM TỔNG*\n━━━━━━━━━━━━━━━━\n\n"
                f"🏢 Nhóm: *{escape_markdown(group_name)}*\n"
                f"🆔 ID: `{chat.id}`\n\n"
                f"*Các lệnh tiếp theo:*\n"
                f"• `/addchild [id] [level]` — Thêm nhóm con\n"
                f"• `/masterinfo` — Xem thông tin hệ thống\n"
                f"• `/broadcast [msg]` — Gửi thông báo\n\n"
                f"🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi khi đặt nhóm tổng!")

    async def mg_masterinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/masterinfo — Xem thông tin nhóm tổng và danh sách nhóm con"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        master_id = chat_id if mg_is_master(chat_id) else mg_get_master_of_child(chat_id)
        if not master_id and not is_owner(user_id):
            await update.message.reply_text("❌ Nhóm này chưa thuộc hệ thống nào!"); return
        if not master_id:
            master_id = chat_id
        children = mg_get_children(master_id)
        level_labels = {0: "🔒 Bị động", 1: "🔓 Cơ bản", 2: "🔓 Nâng cao", 3: "🔓 Đầy đủ"}
        msg = f"🏢 *NHÓM TỔNG — HỆ THỐNG*\n━━━━━━━━━━━━━━━━\n\n"
        msg += f"🆔 Master ID: `{master_id}`\n"
        msg += f"👥 Nhóm con: *{len(children)}*\n"
        msg += f"🚫 Đang ban: *{len(mg_get_ban_list(master_id))}* users\n\n"
        if children:
            msg += "*📋 DANH SÁCH NHÓM CON:*\n"
            for cid, cname, level, created_at in children:
                msg += f"├ {cname.replace(chr(96),chr(39))} ({cid})\n"
                msg += f"│  └ {level_labels.get(level, f'Lv{level}')} — {created_at[:10]}\n"
        else:
            msg += "⚠️ Chưa có nhóm con nào!\n"
        msg += f"\n🕐 {format_vn_time()}"
        keyboard = [
            [InlineKeyboardButton("➕ Thêm nhóm con", callback_data="mg_add_child_guide"),
             InlineKeyboardButton("🚫 Ban list", callback_data=f"mg_banlist_{master_id}")],
            [InlineKeyboardButton("📢 Broadcast", callback_data=f"mg_broadcast_panel_{master_id}"),
             InlineKeyboardButton("🎛️ Tính năng", callback_data=f"mg_features_{master_id}")],
        ]
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN,
                                         reply_markup=InlineKeyboardMarkup(keyboard))

    async def mg_addchild_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/addchild [group_id] [level 0-3] [tên] — Thêm nhóm con"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ owner bot mới dùng lệnh này!"); return
        if not mg_is_master(chat_id):
            await update.message.reply_text(
                "❌ Nhóm này chưa phải nhóm tổng\\!\nDùng `/setmaster` trước\\.",
                parse_mode=ParseMode.MARKDOWN); return
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "📖 *Cách dùng:*\n`/addchild [group\\_id] [level] [tên nhóm]`\n\n"
                "*Ví dụ:*\n`/addchild \\-1001234567890 2 Crypto Group`\n\n"
                "*Cấp độ:*\n`0` Bị động  `1` Cơ bản  `2` Nâng cao  `3` Đầy đủ",
                parse_mode=ParseMode.MARKDOWN); return
        try:
            child_id = int(args[0])
            level = int(args[1])
            if not 0 <= level <= 3: raise ValueError
            child_name = " ".join(args[2:]) if len(args) > 2 else f"Group {child_id}"
        except ValueError:
            await update.message.reply_text("❌ Sai tham số! group\\_id phải là số, level từ 0\\-3",
                                             parse_mode=ParseMode.MARKDOWN); return
        if mg_add_child(chat_id, child_id, child_name, level, user_id):
            level_labels = {0: "🔒 Bị động", 1: "🔓 Cơ bản", 2: "🔓 Nâng cao", 3: "🔓 Đầy đủ"}
            await update.message.reply_text(
                f"✅ *ĐÃ THÊM NHÓM CON*\n━━━━━━━━━━━━━━━━\n\n"
                f"📌 Tên: *{escape_markdown(child_name)}*\n"
                f"🆔 ID: `{child_id}`\n"
                f"🎯 Cấp độ: {level_labels.get(level)}\n"
                f"🎛️ Tính năng bật: *{len(AUTONOMY_PRESETS.get(level,[]))}*/{len(FEATURE_CATALOG)}\n\n"
                f"Dùng `/features {child_id}` để tùy chỉnh thêm\\.\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi thêm nhóm con!")

    async def mg_removechild_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/removechild [group_id] — Xóa nhóm con"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ owner bot mới dùng lệnh này!"); return
        if not mg_is_master(chat_id):
            await update.message.reply_text("❌ Nhóm này chưa phải nhóm tổng!"); return
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/removechild [group_id]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        try:
            child_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ group_id phải là số!"); return
        if mg_remove_child(chat_id, child_id):
            await update.message.reply_text(
                f"✅ Đã xóa nhóm `{child_id}` khỏi hệ thống\\!\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi xóa nhóm con!")

    async def mg_features_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/features [group_id] — Bật/tắt tính năng của nhóm"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        target_id = int(context.args[0]) if context.args else chat_id
        master_id = mg_get_master_of_child(target_id)
        if not is_owner(user_id) and not mg_is_master(chat_id) and chat_id != master_id:
            await update.message.reply_text("❌ Không có quyền!"); return
        await _mg_send_features_panel(update, target_id, user_id)

    async def _mg_send_features_panel(update_or_query, target_group_id, user_id):
        features = mg_get_features(target_group_id)
        categories = {}
        for key, meta in FEATURE_CATALOG.items():
            cat = meta["group"]
            categories.setdefault(cat, []).append((key, meta["name"]))
        keyboard = []
        for cat, items in categories.items():
            keyboard.append([InlineKeyboardButton(f"── {cat} ──", callback_data="mg_noop")])
            row = []
            for key, name in items:
                icon = "✅" if features.get(key, False) else "⬜"
                row.append(InlineKeyboardButton(f"{icon} {name}",
                            callback_data=f"mg_toggle_{target_group_id}_{key}"))
                if len(row) == 2:
                    keyboard.append(row); row = []
            if row: keyboard.append(row)
        keyboard.append([
            InlineKeyboardButton("🔒 Preset Lv0", callback_data=f"mg_preset_{target_group_id}_0"),
            InlineKeyboardButton("🔓 Preset Lv1", callback_data=f"mg_preset_{target_group_id}_1"),
        ])
        keyboard.append([
            InlineKeyboardButton("🔓 Preset Lv2", callback_data=f"mg_preset_{target_group_id}_2"),
            InlineKeyboardButton("🔓 Preset Lv3", callback_data=f"mg_preset_{target_group_id}_3"),
        ])
        enabled_count = sum(1 for v in features.values() if v)
        msg = (f"🎛️ *QUẢN LÝ TÍNH NĂNG*\n━━━━━━━━━━━━━━━━\n\n"
               f"Nhóm: `{target_group_id}`\n"
               f"Đang bật: *{enabled_count}*/{len(FEATURE_CATALOG)}\n\n"
               f"Nhấn để bật/tắt:")
        markup = InlineKeyboardMarkup(keyboard)
        if hasattr(update_or_query, 'message'):
            await update_or_query.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        else:
            await safe_edit_message(update_or_query, msg, reply_markup=markup)

    async def mg_crossban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/crossban [user_id] [lý do] — Ban user khỏi toàn hệ thống"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ owner bot mới dùng lệnh này!"); return
        if not mg_is_master(chat_id):
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm tổng!"); return
        if not context.args:
            await update.message.reply_text(
                "📖 *Cách dùng:*\n`/crossban [user_id] [lý do]`\n\n"
                f"🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN); return
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ user_id phải là số!"); return
        if target_id == OWNER_ID:
            await update.message.reply_text("❌ Không thể ban owner bot!"); return
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Không có lý do"
        if mg_cross_ban(chat_id, target_id, user_id, reason):
            children = mg_get_children(chat_id)
            kicked = 0
            # Kick khỏi nhóm tổng
            try:
                await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
                kicked += 1
                logger.info(f"🚫 Kicked {target_id} from master {chat_id}")
            except Exception as e:
                logger.warning(f"⚠️ Không kick được {target_id} khỏi nhóm tổng: {e}")
            # Kick khỏi tất cả nhóm con
            for child_id, _, _, _ in children:
                try:
                    await context.bot.ban_chat_member(chat_id=child_id, user_id=target_id)
                    kicked += 1
                except Exception as e:
                    logger.warning(f"⚠️ Không kick được {target_id} khỏi {child_id}: {e}")
            total_groups = len(children) + 1  # +1 nhóm tổng
            await update.message.reply_text(
                f"🚫 *ĐÃ BAN XUYÊN NHÓM*\n━━━━━━━━━━━━━━━━\n\n"
                f"👤 User ID: `{target_id}`\n"
                f"📝 Lý do: {escape_markdown(reason)}\n"
                f"👢 Kicked: *{kicked}*/{total_groups} nhóm (tổng + con)\n\n"
                f"Gỡ ban: `/crossunban {target_id}`\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi ban user!")

    async def mg_crossunban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/crossunban [user_id] — Gỡ ban user"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if not is_owner(user_id):
            await update.message.reply_text("❌ Chỉ owner bot mới dùng lệnh này!"); return
        if not mg_is_master(chat_id):
            await update.message.reply_text("❌ Lệnh này chỉ dùng trong nhóm tổng!"); return
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/crossunban [user_id]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ user_id phải là số!"); return
        if mg_cross_unban(chat_id, target_id):
            children = mg_get_children(chat_id)
            unbanned = 0
            for child_id, _, _, _ in children:
                try:
                    await context.bot.unban_chat_member(chat_id=child_id, user_id=target_id)
                    unbanned += 1
                except Exception as e:
                    logger.warning(f"⚠️ Không unban được {target_id} khỏi {child_id}: {e}")
            await update.message.reply_text(
                f"✅ *ĐÃ GỠ BAN XUYÊN NHÓM*\n━━━━━━━━━━━━━━━━\n\n"
                f"👤 User ID: `{target_id}`\n"
                f"🔓 Unban ở: *{unbanned}*/{len(children)} nhóm\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("❌ Lỗi gỡ ban!")

    async def mg_banlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/banlist — Xem danh sách bị ban"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if not is_owner(user_id) and not mg_is_master(chat_id):
            await update.message.reply_text("❌ Không có quyền!"); return
        master_id = chat_id if mg_is_master(chat_id) else mg_get_master_of_child(chat_id)
        if not master_id:
            await update.message.reply_text("❌ Nhóm này không thuộc hệ thống nào!"); return
        bans = mg_get_ban_list(master_id)
        if not bans:
            await update.message.reply_text(
                f"✅ *DANH SÁCH BAN — TRỐNG*\n\nKhông có user nào bị ban\\.\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN); return
        msg = f"🚫 *DANH SÁCH BAN XUYÊN NHÓM*\n━━━━━━━━━━━━━━━━\n\nTổng: *{len(bans)}* user\n\n"
        for i, (bid, _, reason, bat) in enumerate(bans[:20], 1):
            msg += f"{i}\\. `{bid}` — {escape_markdown(reason or 'Không có lý do')} _{bat[:10]}_\n"
        if len(bans) > 20:
            msg += f"\n_\\.\\.\\. và {len(bans)-20} user khác_"
        msg += f"\n\n🕐 {format_vn_time()}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    # Lưu trạng thái broadcast đang soạn (chat_id → {msg, targets})
    _broadcast_drafts = {}

    async def mg_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/broadcast [nội dung] — Mở panel chọn nhóm để gửi thông báo"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        # Kiểm tra quyền: owner bot HOẶC admin/owner của nhóm con có master
        master_id = None
        if is_owner(user_id):
            if mg_is_master(chat_id):
                master_id = chat_id
            else:
                master_id = mg_get_master_of_child(chat_id)
                if not master_id:
                    await update.message.reply_text("❌ Nhóm này chưa thuộc hệ thống nào!"); return
        else:
            # Admin/owner nhóm con
            master_id = mg_get_master_of_child(chat_id)
            if not master_id:
                await update.message.reply_text("❌ Nhóm này chưa thuộc hệ thống nào!"); return
            if not check_permission(chat_id, user_id, 'manage'):
                await update.message.reply_text("❌ Bạn cần quyền manage để gửi broadcast!"); return

        # Kiểm tra có reply ảnh không
        photo_id = None
        caption_text = None
        if update.message.reply_to_message and update.message.reply_to_message.photo:
            # Reply vào ảnh → lấy file_id ảnh lớn nhất
            photo_id = update.message.reply_to_message.photo[-1].file_id
            caption_text = " ".join(context.args) if context.args else (update.message.reply_to_message.caption or "")
        elif update.message.photo:
            # Gửi ảnh trực tiếp kèm /broadcast làm caption
            photo_id = update.message.photo[-1].file_id
            caption_text = " ".join(context.args) if context.args else ""
        
        if not context.args and not photo_id:
            await update.message.reply_text(
                "📖 *Cách dùng:*\n"
                "`/broadcast [nội dung]` — Gửi text\n"
                "Reply ảnh + `/broadcast [caption]` — Gửi ảnh\n\n"
                "*Ví dụ:*\n`/broadcast Họp nhóm lúc 8pm!`\n\n"
                "Sau đó chọn nhóm muốn gửi.",
                parse_mode=ParseMode.MARKDOWN); return

        message_text = " ".join(context.args) if context.args else ""
        children = mg_get_children(master_id)

        if not children:
            await update.message.reply_text(
                "⚠️ Chưa có nhóm con!\nDùng `/addchild [id] [level]` để thêm.",
                parse_mode=ParseMode.MARKDOWN); return

        # Lưu draft kèm photo nếu có
        draft_key = f"{chat_id}_{user_id}"
        _broadcast_drafts[draft_key] = {
            'msg': message_text,
            'master_id': master_id,
            'targets': set(),
            'photo_id': photo_id,
            'caption': caption_text,
        }

        # Preview
        preview = caption_text or message_text
        preview_short = preview[:60] + "..." if len(preview) > 60 else preview
        type_icon = "🖼" if photo_id else "📝"

        # Tạo panel chọn nhóm
        await _mg_send_broadcast_panel(update.message, draft_key, children, f"{type_icon} {preview_short}")

    async def _mg_send_broadcast_panel(message_or_query, draft_key, children, message_text, edit=False):
        """Hiển thị panel chọn nhóm để broadcast"""
        draft = _broadcast_drafts.get(draft_key, {})
        selected = draft.get('targets', set())

        keyboard = []
        for cid, cname, level, _ in children:
            is_recv = mg_has_feature(cid, "broadcast_recv")
            if not is_recv:
                continue  # Bỏ qua nhóm không bật nhận broadcast
            icon = "✅" if cid in selected else "⬜"
            short_name = cname[:20] + "..." if len(cname) > 20 else cname
            keyboard.append([InlineKeyboardButton(
                f"{icon} {short_name}",
                callback_data=f"mg_bc_toggle_{draft_key}_{cid}"
            )])

        if not keyboard:
            txt = "⚠️ Không có nhóm con nào bật tính năng nhận broadcast!\nDùng /features để bật."
            if edit:
                await message_or_query.edit_text(txt)
            else:
                await message_or_query.reply_text(txt)
            return

        # Nút chọn tất cả / bỏ tất cả
        all_ids = {cid for cid, _, _, _ in children if mg_has_feature(cid, "broadcast_recv")}
        all_selected = all_ids == selected
        keyboard.append([
            InlineKeyboardButton(
                "☑️ Chọn tất cả" if not all_selected else "🔲 Bỏ tất cả",
                callback_data=f"mg_bc_all_{draft_key}_{'deselect' if all_selected else 'select'}"
            )
        ])
        keyboard.append([
            InlineKeyboardButton(
                f"📢 GỬI ({len(selected)} nhóm)" if selected else "📢 GỬI TẤT CẢ",
                callback_data=f"mg_bc_send_{draft_key}"
            ),
            InlineKeyboardButton("❌ Hủy", callback_data=f"mg_bc_cancel_{draft_key}")
        ])

        preview = message_text[:80] + "..." if len(message_text) > 80 else message_text
        msg = (
            f"📢 *CHỌN NHÓM ĐỂ GỬI THÔNG BÁO*\n━━━━━━━━━━━━━━━━\n\n"
            f"📝 Nội dung: _{escape_markdown(preview)}_\n\n"
            f"*Chọn nhóm muốn gửi* (✅ = đã chọn):\n"
            f"Đã chọn: *{len(selected)}*/{len(all_ids)} nhóm"
        )
        markup = InlineKeyboardMarkup(keyboard)
        if edit:
            await safe_edit_message(message_or_query, msg, reply_markup=markup)
        else:
            await message_or_query.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)

    async def handle_mg_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Xử lý tất cả callback mg_* của multi-group system"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        data = query.data

        if data == "mg_noop":
            return

        elif data.startswith("mg_toggle_") and not data.startswith("mg_toggle_group_"):
            # mg_toggle_{group_id}_{feature_key}  (group_id có thể âm)
            import re as _re
            raw = data[len("mg_toggle_"):]
            m = _re.match(r'^(-?\d+)_(.+)$', raw)
            try:
                if not m: raise ValueError("parse failed")
                group_id = int(m.group(1))
                feature_key = m.group(2)
            except (ValueError, IndexError):
                await query.answer("❌ Lỗi dữ liệu!", show_alert=True); return
            if not is_owner(user_id):
                await query.answer("❌ Không có quyền!", show_alert=True); return
            current = mg_has_feature(group_id, feature_key)
            mg_set_feature(group_id, feature_key, not current, user_id)
            await _mg_send_features_panel(query, group_id, user_id)

        elif data.startswith("mg_preset_"):
            import re as _re
            raw = data[len("mg_preset_"):]
            m = _re.match(r'^(-?\d+)_(\d+)$', raw)
            try:
                if not m: raise ValueError("parse failed")
                group_id = int(m.group(1)); level = int(m.group(2))
            except (ValueError, IndexError):
                await query.answer("❌ Lỗi!", show_alert=True); return
            if not is_owner(user_id):
                await query.answer("❌ Không có quyền!", show_alert=True); return
            mg_apply_preset(group_id, level, user_id)
            await query.answer(f"✅ Đã áp dụng Preset Level {level}!", show_alert=True)
            await _mg_send_features_panel(query, group_id, user_id)

        elif data.startswith("mg_banlist_"):
            master_id = int(data[len("mg_banlist_"):])
            bans = mg_get_ban_list(master_id)
            if not bans:
                await query.answer("✅ Không có ai bị ban!", show_alert=True); return
            msg = f"🚫 *BAN LIST* \\({len(bans)} users\\)\n━━━━━━━━━━━━━━━━\n\n"
            for bid, _, reason, _ in bans[:10]:
                msg += f"• `{bid}` — {escape_markdown(reason or '-')}\n"
            await safe_edit_message(query, msg)

        elif data.startswith("mg_broadcast_panel_"):
            master_id = int(data[len("mg_broadcast_panel_"):])
            children = mg_get_children(master_id)
            recv = sum(1 for cid, _, _, _ in children if mg_has_feature(cid, "broadcast_recv"))
            msg = (f"📢 *BROADCAST PANEL*\n━━━━━━━━━━━━━━━━\n\n"
                   f"Nhóm con: *{len(children)}*\n"
                   f"Có thể nhận: *{recv}*\n\n"
                   f"Dùng lệnh trong nhóm tổng:\n`/broadcast [nội dung]`")
            await safe_edit_message(query, msg)

        elif data.startswith("mg_features_"):
            master_id = int(data[len("mg_features_"):])
            children = mg_get_children(master_id)
            if not children:
                await query.answer("⚠️ Chưa có nhóm con!", show_alert=True); return
            keyboard = [[InlineKeyboardButton(f"⚙️ {cname}", callback_data=f"mg_toggle_group_{cid}")]
                        for cid, cname, _, _ in children]
            keyboard.append([InlineKeyboardButton("🔙 Quay lại", callback_data="mg_noop")])
            await safe_edit_message(query, "🎛️ *CHỌN NHÓM ĐỂ CẤU HÌNH:*",
                                    reply_markup=InlineKeyboardMarkup(keyboard))

        elif data.startswith("mg_toggle_group_"):
            try:
                group_id = int(data[len("mg_toggle_group_"):])
            except ValueError:
                await query.answer("❌ Lỗi group_id!", show_alert=True); return
            await _mg_send_features_panel(query, group_id, user_id)

        elif data == "mg_add_child_guide":
            await safe_edit_message(query,
                "📖 *THÊM NHÓM CON*\n━━━━━━━━━━━━━━━━\n\n"
                "Gõ lệnh trong nhóm tổng:\n\n"
                "`/addchild [group_id] [level] [tên]`\n\n"
                "*Level:* 0=Bị động  1=Cơ bản  2=Nâng cao  3=Đầy đủ\n\n"
                "*Lấy group_id:* Forward tin nhắn từ nhóm con vào @userinfobot")

        # ── Broadcast panel callbacks ────────────────────────────────────
        elif data.startswith("mg_bc_toggle_"):
            # mg_bc_toggle_{chat_id}_{user_id}_{group_id}
            import re as _re
            m = _re.match(r"^mg_bc_toggle_(-?\d+)_(-?\d+)_(-?\d+)$", data)
            if not m:
                await query.answer("❌ Lỗi dữ liệu!", show_alert=True); return
            draft_key = f"{m.group(1)}_{m.group(2)}"
            target_cid = int(m.group(3))
            draft = _broadcast_drafts.get(draft_key)
            if not draft:
                await query.answer("❌ Phiên đã hết hạn! Gõ lại /broadcast", show_alert=True); return
            if target_cid in draft['targets']:
                draft['targets'].discard(target_cid)
            else:
                draft['targets'].add(target_cid)
            master_id = draft['master_id']
            children = mg_get_children(master_id)
            await _mg_send_broadcast_panel(query, draft_key, children, draft['msg'], edit=True)

        elif data.startswith("mg_bc_all_"):
            import re as _re
            m = _re.match(r"^mg_bc_all_(-?\d+)_(-?\d+)_(select|deselect)$", data)
            if not m:
                await query.answer("❌ Lỗi!", show_alert=True); return
            draft_key = f"{m.group(1)}_{m.group(2)}"
            action = m.group(3)
            draft = _broadcast_drafts.get(draft_key)
            if not draft:
                await query.answer("❌ Phiên đã hết hạn!", show_alert=True); return
            master_id = draft['master_id']
            children = mg_get_children(master_id)
            if action == 'select':
                draft['targets'] = {cid for cid, _, _, _ in children if mg_has_feature(cid, "broadcast_recv")}
            else:
                draft['targets'] = set()
            await _mg_send_broadcast_panel(query, draft_key, children, draft['msg'], edit=True)

        elif data.startswith("mg_bc_send_"):
            import re as _re
            m = _re.match(r"^mg_bc_send_(-?\d+)_(-?\d+)$", data)
            if not m:
                await query.answer("❌ Lỗi!", show_alert=True); return
            draft_key = f"{m.group(1)}_{m.group(2)}"
            draft = _broadcast_drafts.get(draft_key)
            if not draft:
                await query.answer("❌ Phiên đã hết hạn! Gõ lại /broadcast", show_alert=True); return
            master_id = draft['master_id']
            message_text = draft['msg']
            photo_id = draft.get('photo_id')
            caption = draft.get('caption')
            # Nếu chưa chọn nhóm nào → gửi tất cả
            target_ids = draft['targets'] if draft['targets'] else None
            children = mg_get_children(master_id)
            target_count = len(target_ids) if target_ids else sum(
                1 for cid, _, _, _ in children if mg_has_feature(cid, "broadcast_recv"))
            type_icon = "🖼" if photo_id else "📝"
            await safe_edit_message(query, f"📤 *Đang gửi {type_icon} đến {target_count} nhóm...*")
            success, fail = await mg_broadcast(context, master_id, message_text, user_id, target_ids, photo_id=photo_id, caption=caption)
            _broadcast_drafts.pop(draft_key, None)
            await safe_edit_message(query,
                f"📢 *KẾT QUẢ BROADCAST*\n━━━━━━━━━━━━━━━━\n\n"
                f"✅ Thành công: *{success}* nhóm\n"
                f"❌ Thất bại: *{fail}* nhóm\n\n"
                f"📝 _{escape_markdown(message_text[:80])}_\n\n"
                f"🕐 {format_vn_time()}")

        elif data.startswith("mg_bc_cancel_"):
            import re as _re
            m = _re.match(r"^mg_bc_cancel_(-?\d+)_(-?\d+)$", data)
            if m:
                draft_key = f"{m.group(1)}_{m.group(2)}"
                _broadcast_drafts.pop(draft_key, None)
            await safe_edit_message(query, "❌ Đã hủy broadcast.")

    # ==================== MODERATION: DATABASE TABLES ====================

    def mod_init_tables():
        """Khởi tạo tất cả bảng cho hệ thống moderation"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            # Cảnh cáo (Warns)
            c.execute('''CREATE TABLE IF NOT EXISTS mod_warns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER, user_id INTEGER,
                reason TEXT, warned_by INTEGER,
                created_at TEXT
            )''')
            # Cấu hình warn: max warn, action khi đạt max
            c.execute('''CREATE TABLE IF NOT EXISTS mod_warn_config (
                group_id INTEGER PRIMARY KEY,
                max_warns INTEGER DEFAULT 3,
                action TEXT DEFAULT 'mute',
                mute_duration INTEGER DEFAULT 3600
            )''')
            # Chào mừng tùy chỉnh
            c.execute('''CREATE TABLE IF NOT EXISTS mod_welcome (
                group_id INTEGER PRIMARY KEY,
                message TEXT,
                enabled INTEGER DEFAULT 1,
                set_by INTEGER, updated_at TEXT
            )''')
            # Nội quy nhóm
            c.execute('''CREATE TABLE IF NOT EXISTS mod_rules (
                group_id INTEGER PRIMARY KEY,
                rules TEXT,
                set_by INTEGER, updated_at TEXT
            )''')
            # Lọc từ khóa
            c.execute('''CREATE TABLE IF NOT EXISTS mod_filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER, keyword TEXT,
                action TEXT DEFAULT 'delete',
                reply TEXT,
                added_by INTEGER, created_at TEXT,
                UNIQUE(group_id, keyword)
            )''')
            # Lệnh tùy chỉnh
            c.execute('''CREATE TABLE IF NOT EXISTS mod_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER, command TEXT,
                response TEXT,
                added_by INTEGER, created_at TEXT,
                UNIQUE(group_id, command)
            )''')
            # Anti-flood config
            c.execute('''CREATE TABLE IF NOT EXISTS mod_flood_config (
                group_id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 0,
                max_msgs INTEGER DEFAULT 5,
                interval_sec INTEGER DEFAULT 5,
                action TEXT DEFAULT 'mute'
            )''')
            # Anti-flood tracking (RAM only, không lưu DB)
            # CAPTCHA config
            c.execute('''CREATE TABLE IF NOT EXISTS mod_captcha_config (
                group_id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 0,
                captcha_type TEXT DEFAULT 'button',
                timeout_sec INTEGER DEFAULT 60
            )''')
            # CAPTCHA pending
            c.execute('''CREATE TABLE IF NOT EXISTS mod_captcha_pending (
                group_id INTEGER, user_id INTEGER,
                answer TEXT, expires_at TEXT,
                message_id INTEGER,
                PRIMARY KEY (group_id, user_id)
            )''')
            # Admin logs
            c.execute('''CREATE TABLE IF NOT EXISTS mod_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER, action_by INTEGER,
                target_user INTEGER, action TEXT,
                reason TEXT, extra TEXT,
                created_at TEXT
            )''')
            # Report
            c.execute('''CREATE TABLE IF NOT EXISTS mod_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER, reporter_id INTEGER,
                target_user INTEGER, message_id INTEGER,
                reason TEXT, status TEXT DEFAULT 'pending',
                created_at TEXT
            )''')
            # Federation
            c.execute('''CREATE TABLE IF NOT EXISTS mod_federations (
                fed_id TEXT PRIMARY KEY,
                fed_name TEXT,
                owner_id INTEGER,
                created_at TEXT
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS mod_fed_members (
                fed_id TEXT, group_id INTEGER,
                joined_at TEXT,
                PRIMARY KEY (fed_id, group_id)
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS mod_fed_bans (
                fed_id TEXT, user_id INTEGER,
                reason TEXT, banned_by INTEGER,
                banned_at TEXT,
                PRIMARY KEY (fed_id, user_id)
            )''')

            conn.commit()
            conn.close()
            logger.info("✅ Moderation tables initialized")
        except Exception as e:
            logger.error(f"❌ mod_init_tables: {e}")

    # Flood tracking trong RAM
    _flood_tracker = {}  # {(group_id, user_id): [timestamps]}

    # ==================== MODERATION: HELPERS ====================

    def mod_log(group_id, action_by, target_user, action, reason="", extra=""):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''INSERT INTO mod_logs (group_id, action_by, target_user, action, reason, extra, created_at)
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (group_id, action_by, target_user, action, reason, extra,
                       get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit(); conn.close()
        except Exception as e:
            logger.error(f"❌ mod_log: {e}")

    def mod_is_admin(group_id, user_id):
        """Kiểm tra user có quyền manage trong nhóm không"""
        return is_owner(user_id) or check_permission(group_id, user_id, 'manage')

    async def mod_check_admin(update: Update) -> bool:
        """Trả về True nếu user là admin, ngược lại reply lỗi"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if mod_is_admin(chat_id, user_id):
            return True
        await update.message.reply_text("❌ Bạn cần quyền admin để dùng lệnh này!")
        return False

    async def mod_get_target(update: Update, context):
        """Lấy target user từ reply hoặc args"""
        if update.message.reply_to_message:
            u = update.message.reply_to_message.from_user
            return u.id, u.first_name
        if context.args:
            try:
                uid = int(context.args[0])
                return uid, str(uid)
            except ValueError:
                username = context.args[0].replace('@', '')
                uid = get_user_id_by_username(username)
                if uid:
                    return uid, username
        return None, None

    # ==================== BAN / MUTE / KICK / WARN ====================

    async def mod_ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/ban [reply/user_id] [lý do] — Cấm thành viên vĩnh viễn"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, target_name = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Reply vào tin nhắn người cần ban hoặc dùng /ban [user_id]")
            return
        reason = " ".join(context.args[1:]) if context.args and len(context.args) > 1 else \
                 " ".join(context.args) if context.args and not update.message.reply_to_message else "Không có lý do"
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
            mod_log(chat_id, user_id, target_id, "ban", reason)
            await update.message.reply_text(
                f"🚫 *ĐÃ BAN*\n━━━━━━━━━━━━━━━━\n\n"
                f"👤 User: `{target_id}`\n"
                f"📝 Lý do: {reason}\n"
                f"👮 Admin: {update.effective_user.first_name}\n"
                f"🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi ban: {e}")

    async def mod_unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/unban [user_id] — Gỡ ban"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, target_name = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Dùng /unban [user_id]"); return
        try:
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id)
            mod_log(chat_id, user_id, target_id, "unban")
            await update.message.reply_text(f"✅ Đã gỡ ban `{target_id}`", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi unban: {e}")

    async def mod_kick_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/kick [reply/user_id] [lý do] — Đuổi thành viên (có thể quay lại)"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, target_name = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Reply vào tin nhắn người cần kick"); return
        reason = " ".join(context.args[1:]) if context.args and len(context.args) > 1 else "Không có lý do"
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id)
            mod_log(chat_id, user_id, target_id, "kick", reason)
            await update.message.reply_text(
                f"👢 *ĐÃ KICK*\n\n👤 User: `{target_id}`\n📝 Lý do: {reason}\n🕐 {format_vn_time()}",
                parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi kick: {e}")

    async def mod_mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/mute [reply/user_id] [thời gian: 1h/30m/2d] [lý do] — Tắt tiếng"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, target_name = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text(
                "❌ Cách dùng: `/mute [reply/user_id] [thời gian] [lý do]`\n"
                "Ví dụ: `/mute 30m Spam` hoặc reply + `/mute 2h`",
                parse_mode=ParseMode.MARKDOWN); return
        # Parse thời gian
        duration_sec = None
        duration_str = "vĩnh viễn"
        args = context.args if not update.message.reply_to_message else context.args
        time_arg = None
        for arg in (args or []):
            import re as _re
            m = _re.match(r'^(\d+)(s|m|h|d)$', arg.lower())
            if m:
                val, unit = int(m.group(1)), m.group(2)
                multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
                duration_sec = val * multipliers[unit]
                duration_str = f"{val}{'giây' if unit=='s' else 'phút' if unit=='m' else 'giờ' if unit=='h' else 'ngày'}"
                time_arg = arg
                break
        reason_parts = [a for a in (args or []) if a != time_arg and a != str(target_id)]
        reason = " ".join(reason_parts) or "Không có lý do"
        from telegram import ChatPermissions
        until_date = None
        if duration_sec:
            until_date = datetime.utcnow() + timedelta(seconds=duration_sec)
        try:
            await context.bot.restrict_chat_member(
                chat_id=chat_id, user_id=target_id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until_date
            )
            mod_log(chat_id, user_id, target_id, "mute", reason, duration_str)
            await update.message.reply_text(
                f"🔇 *ĐÃ TẮT TIẾNG*\n━━━━━━━━━━━━━━━━\n\n"
                f"👤 User: `{target_id}`\n"
                f"⏱ Thời gian: {duration_str}\n"
                f"📝 Lý do: {reason}\n"
                f"🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi mute: {e}")

    async def mod_unmute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/unmute [reply/user_id] — Bật tiếng lại"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, _ = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Reply vào tin nhắn người cần unmute"); return
        from telegram import ChatPermissions
        try:
            await context.bot.restrict_chat_member(
                chat_id=chat_id, user_id=target_id,
                permissions=ChatPermissions(
                    can_send_messages=True, can_send_media_messages=True,
                    can_send_other_messages=True, can_add_web_page_previews=True
                )
            )
            mod_log(chat_id, user_id, target_id, "unmute")
            await update.message.reply_text(f"🔊 Đã bật tiếng `{target_id}`", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await update.message.reply_text(f"❌ Lỗi unmute: {e}")

    # ==================== HỆ THỐNG CẢNH CÁO (WARN) ====================

    async def mod_warn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/warn [reply/user_id] [lý do] — Cảnh cáo thành viên"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        target_id, target_name = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Reply vào tin nhắn người cần warn"); return
        reason = " ".join(context.args[1:]) if context.args and len(context.args) > 1 else \
                 " ".join(context.args) if context.args and not update.message.reply_to_message else "Không có lý do"
        # Lấy config warn
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT max_warns, action, mute_duration FROM mod_warn_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone() or (3, 'mute', 3600)
        max_warns, action, mute_dur = cfg
        # Thêm warn
        c.execute('''INSERT INTO mod_warns (group_id, user_id, reason, warned_by, created_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (chat_id, target_id, reason, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        # Đếm tổng warn
        c.execute("SELECT COUNT(*) FROM mod_warns WHERE group_id=? AND user_id=?", (chat_id, target_id))
        total_warns = c.fetchone()[0]
        conn.commit(); conn.close()
        mod_log(chat_id, user_id, target_id, "warn", reason, f"{total_warns}/{max_warns}")
        msg = (f"⚠️ *CẢNH CÁO*\n━━━━━━━━━━━━━━━━\n\n"
               f"👤 User: `{target_id}`\n"
               f"📝 Lý do: {reason}\n"
               f"🔢 Cảnh cáo: *{total_warns}*/{max_warns}\n")
        # Kiểm tra đạt max warn
        if total_warns >= max_warns:
            msg += f"\n🚨 *ĐẠT GIỚI HẠN! Đang thực hiện: {action.upper()}*\n"
            try:
                if action == 'ban':
                    await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
                    mod_log(chat_id, user_id, target_id, "auto_ban", f"Đạt {max_warns} warns")
                elif action == 'kick':
                    await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
                    await context.bot.unban_chat_member(chat_id=chat_id, user_id=target_id)
                    mod_log(chat_id, user_id, target_id, "auto_kick", f"Đạt {max_warns} warns")
                elif action == 'mute':
                    from telegram import ChatPermissions
                    until = datetime.utcnow() + timedelta(seconds=mute_dur)
                    await context.bot.restrict_chat_member(
                        chat_id=chat_id, user_id=target_id,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=until)
                    mod_log(chat_id, user_id, target_id, "auto_mute", f"Đạt {max_warns} warns")
            except Exception as e:
                msg += f"\n❌ Lỗi thực hiện action: {e}\n"
            # Reset warns sau khi xử lý
            conn2 = sqlite3.connect(DB_PATH)
            c2 = conn2.cursor()
            c2.execute("DELETE FROM mod_warns WHERE group_id=? AND user_id=?", (chat_id, target_id))
            conn2.commit(); conn2.close()
        msg += f"\n🕐 {format_vn_time()}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def mod_unwarn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/unwarn [reply/user_id] — Xóa 1 cảnh cáo"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        target_id, _ = await mod_get_target(update, context)
        if not target_id:
            await update.message.reply_text("❌ Reply vào tin nhắn người cần unwarn"); return
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''DELETE FROM mod_warns WHERE id = (
                     SELECT id FROM mod_warns WHERE group_id=? AND user_id=? ORDER BY created_at DESC LIMIT 1)''',
                  (chat_id, target_id))
        c.execute("SELECT COUNT(*) FROM mod_warns WHERE group_id=? AND user_id=?", (chat_id, target_id))
        remaining = c.fetchone()[0]
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ Đã xóa 1 cảnh cáo của `{target_id}`\nCòn lại: *{remaining}* cảnh cáo",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_warns_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/warns [reply/user_id] — Xem số cảnh cáo"""
        chat_id = update.effective_chat.id
        target_id, _ = await mod_get_target(update, context)
        if not target_id:
            target_id = update.effective_user.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT max_warns FROM mod_warn_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone()
        max_warns = cfg[0] if cfg else 3
        c.execute("SELECT reason, created_at FROM mod_warns WHERE group_id=? AND user_id=? ORDER BY created_at DESC",
                  (chat_id, target_id))
        warns = c.fetchall(); conn.close()
        if not warns:
            await update.message.reply_text(f"✅ User `{target_id}` chưa có cảnh cáo nào.",
                                             parse_mode=ParseMode.MARKDOWN); return
        msg = f"⚠️ *CẢNH CÁO CỦA* `{target_id}`\n━━━━━━━━━━━━━━━━\n\n"
        msg += f"Tổng: *{len(warns)}*/{max_warns}\n\n"
        for i, (reason, at) in enumerate(warns, 1):
            msg += f"{i}. {reason} _{at[:10]}_\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def mod_setwarn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setwarn [max] [action: ban/kick/mute] — Cấu hình hệ thống warn"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "📖 Cách dùng: `/setwarn [số] [ban/kick/mute]`\n"
                "Ví dụ: `/setwarn 3 mute`", parse_mode=ParseMode.MARKDOWN); return
        try:
            max_w = int(context.args[0])
            action = context.args[1].lower()
            if action not in ['ban', 'kick', 'mute']:
                raise ValueError
        except:
            await update.message.reply_text("❌ Sai cú pháp! action phải là: ban, kick, hoặc mute"); return
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_warn_config (group_id, max_warns, action)
                     VALUES (?, ?, ?)''', (chat_id, max_w, action))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ Đã cấu hình warns:\n🔢 Tối đa: *{max_w}* cảnh cáo\n⚡ Hành động: *{action.upper()}*",
            parse_mode=ParseMode.MARKDOWN)

    # ==================== CAPTCHA ====================

    async def mod_captcha_join(update: Update, context: ContextTypes.DEFAULT_TYPE, new_member):
        """Gửi CAPTCHA cho thành viên mới"""
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT enabled, captcha_type, timeout_sec FROM mod_captcha_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone(); conn.close()
        if not cfg or not cfg[0]:
            return  # CAPTCHA tắt
        captcha_type, timeout = cfg[1], cfg[2]
        import random, math
        if captcha_type == 'button':
            # CAPTCHA nút bấm đơn giản
            keyboard = [[InlineKeyboardButton(
                "✅ Tôi không phải bot - Nhấn để xác nhận",
                callback_data=f"mod_captcha_{chat_id}_{new_member.id}"
            )]]
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"👋 Chào mừng {new_member.first_name}!\n\n"
                     f"⚠️ Vui lòng nhấn nút bên dưới trong *{timeout}* giây để xác nhận bạn không phải bot.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            # Tắt tiếng user cho đến khi xác nhận
            from telegram import ChatPermissions
            try:
                await context.bot.restrict_chat_member(
                    chat_id=chat_id, user_id=new_member.id,
                    permissions=ChatPermissions(can_send_messages=False)
                )
            except: pass
            # Lưu pending
            expires = (get_vn_time() + timedelta(seconds=timeout)).strftime("%Y-%m-%d %H:%M:%S")
            conn2 = sqlite3.connect(DB_PATH)
            c2 = conn2.cursor()
            c2.execute('''INSERT OR REPLACE INTO mod_captcha_pending (group_id, user_id, answer, expires_at, message_id)
                          VALUES (?, ?, ?, ?, ?)''', (chat_id, new_member.id, "confirmed", expires, msg.message_id))
            conn2.commit(); conn2.close()
            # Schedule timeout kick
            context.application.create_task(
                _mod_captcha_timeout(context, chat_id, new_member.id, msg.message_id, timeout)
            )
        elif captcha_type == 'math':
            a, b = random.randint(1, 10), random.randint(1, 10)
            answer = str(a + b)
            buttons = [str(a + b), str(a * b), str(abs(a - b)), str(a + b + 1)]
            random.shuffle(buttons)
            keyboard = [[InlineKeyboardButton(
                btn, callback_data=f"mod_captcha_{chat_id}_{new_member.id}_{btn}"
            ) for btn in buttons[:2]],
            [InlineKeyboardButton(
                btn, callback_data=f"mod_captcha_{chat_id}_{new_member.id}_{btn}"
            ) for btn in buttons[2:]]]
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔢 CAPTCHA cho {new_member.first_name}\n\n"
                     f"❓ Bao nhiêu là *{a} + {b}* = ?\n"
                     f"⏱ Trả lời trong {timeout} giây",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            from telegram import ChatPermissions
            try:
                await context.bot.restrict_chat_member(
                    chat_id=chat_id, user_id=new_member.id,
                    permissions=ChatPermissions(can_send_messages=False)
                )
            except: pass
            expires = (get_vn_time() + timedelta(seconds=timeout)).strftime("%Y-%m-%d %H:%M:%S")
            conn2 = sqlite3.connect(DB_PATH)
            c2 = conn2.cursor()
            c2.execute('''INSERT OR REPLACE INTO mod_captcha_pending (group_id, user_id, answer, expires_at, message_id)
                          VALUES (?, ?, ?, ?, ?)''', (chat_id, new_member.id, answer, expires, msg.message_id))
            conn2.commit(); conn2.close()
            context.application.create_task(
                _mod_captcha_timeout(context, chat_id, new_member.id, msg.message_id, timeout)
            )

    async def _mod_captcha_timeout(context, chat_id, user_id, msg_id, timeout):
        """Kick user nếu không xác nhận CAPTCHA trong timeout"""
        await asyncio.sleep(timeout)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT user_id FROM mod_captcha_pending WHERE group_id=? AND user_id=?", (chat_id, user_id))
        still_pending = c.fetchone(); conn.close()
        if still_pending:
            try:
                await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                await context.bot.send_message(chat_id=chat_id,
                    text=f"⏱ User `{user_id}` đã bị kick do không xác nhận CAPTCHA.",
                    parse_mode=ParseMode.MARKDOWN)
            except: pass
            conn2 = sqlite3.connect(DB_PATH)
            c2 = conn2.cursor()
            c2.execute("DELETE FROM mod_captcha_pending WHERE group_id=? AND user_id=?", (chat_id, user_id))
            conn2.commit(); conn2.close()

    async def mod_setcaptcha_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setcaptcha [on/off] [button/math] — Cấu hình CAPTCHA"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args:
            await update.message.reply_text(
                "📖 Cách dùng:\n`/setcaptcha on button` — Bật CAPTCHA nút bấm\n"
                "`/setcaptcha on math` — Bật CAPTCHA toán học\n"
                "`/setcaptcha off` — Tắt CAPTCHA", parse_mode=ParseMode.MARKDOWN); return
        enabled = 1 if context.args[0].lower() == 'on' else 0
        ctype = context.args[1].lower() if len(context.args) > 1 else 'button'
        if ctype not in ['button', 'math']:
            ctype = 'button'
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_captcha_config (group_id, enabled, captcha_type)
                     VALUES (?, ?, ?)''', (chat_id, enabled, ctype))
        conn.commit(); conn.close()
        status = "BẬT" if enabled else "TẮT"
        await update.message.reply_text(
            f"✅ CAPTCHA *{status}*" + (f" — Loại: *{ctype}*" if enabled else ""),
            parse_mode=ParseMode.MARKDOWN)

    # ==================== ANTI-FLOOD ====================

    async def mod_check_flood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Kiểm tra flood, return True nếu bị flood"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT enabled, max_msgs, interval_sec, action FROM mod_flood_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone(); conn.close()
        if not cfg or not cfg[0]:
            return False
        _, max_msgs, interval_sec, action = cfg
        key = (chat_id, user_id)
        now = time.time()
        if key not in _flood_tracker:
            _flood_tracker[key] = []
        # Xóa timestamps cũ
        _flood_tracker[key] = [t for t in _flood_tracker[key] if now - t < interval_sec]
        _flood_tracker[key].append(now)
        if len(_flood_tracker[key]) >= max_msgs:
            _flood_tracker[key] = []
            try:
                if action == 'ban':
                    await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                    mod_log(chat_id, 0, user_id, "auto_ban_flood")
                elif action == 'kick':
                    await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                    await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
                    mod_log(chat_id, 0, user_id, "auto_kick_flood")
                else:  # mute
                    from telegram import ChatPermissions
                    until = datetime.utcnow() + timedelta(seconds=300)
                    await context.bot.restrict_chat_member(
                        chat_id=chat_id, user_id=user_id,
                        permissions=ChatPermissions(can_send_messages=False),
                        until_date=until)
                    mod_log(chat_id, 0, user_id, "auto_mute_flood")
                await update.message.reply_text(
                    f"🌊 `{user_id}` bị {action} do gửi tin quá nhanh!",
                    parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"❌ Flood action error: {e}")
            return True
        return False

    async def mod_setflood_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setflood [on/off] [max_msgs] [interval_sec] [action] — Cấu hình anti-flood"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args:
            await update.message.reply_text(
                "📖 Cách dùng:\n`/setflood on 5 5 mute` — Bật, max 5 tin/5 giây → mute\n"
                "`/setflood off` — Tắt", parse_mode=ParseMode.MARKDOWN); return
        enabled = 1 if context.args[0].lower() == 'on' else 0
        max_m = int(context.args[1]) if len(context.args) > 1 else 5
        interval = int(context.args[2]) if len(context.args) > 2 else 5
        action = context.args[3].lower() if len(context.args) > 3 else 'mute'
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_flood_config (group_id, enabled, max_msgs, interval_sec, action)
                     VALUES (?, ?, ?, ?, ?)''', (chat_id, enabled, max_m, interval, action))
        conn.commit(); conn.close()
        if enabled:
            await update.message.reply_text(
                f"✅ Anti-flood *BẬT*\n📨 Max: *{max_m}* tin/{interval}s → *{action}*",
                parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("✅ Anti-flood *TẮT*", parse_mode=ParseMode.MARKDOWN)

    # ==================== CHÀO MỪNG ====================

    async def mod_setwelcome_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setwelcome [nội dung] — Đặt tin nhắn chào mừng tùy chỉnh
        Biến: {name} {id} {group} {count}"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text(
                "📖 Cách dùng: `/setwelcome [nội dung]`\n\n"
                "*Biến có thể dùng:*\n"
                "`{name}` — Tên thành viên\n"
                "`{id}` — ID thành viên\n"
                "`{group}` — Tên nhóm\n"
                "`{count}` — Số thành viên\n\n"
                "Ví dụ: `/setwelcome Chào {name}! Bạn là thành viên thứ {count} 🎉`",
                parse_mode=ParseMode.MARKDOWN); return
        msg_text = " ".join(context.args)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_welcome (group_id, message, enabled, set_by, updated_at)
                     VALUES (?, ?, 1, ?, ?)''',
                  (chat_id, msg_text, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ *Đã đặt tin chào mừng!*\n\n*Preview:*\n{msg_text.replace('{name}','Nguyễn Văn A').replace('{id}','123456').replace('{group}', update.effective_chat.title or 'Nhóm').replace('{count}','100')}",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_welcome_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/welcomeoff — Tắt chào mừng"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE mod_welcome SET enabled=0 WHERE group_id=?", (chat_id,))
        conn.commit(); conn.close()
        await update.message.reply_text("✅ Đã tắt tin chào mừng.")

    async def mod_send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE, member):
        """Gửi tin chào mừng tùy chỉnh"""
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT message, enabled FROM mod_welcome WHERE group_id=?", (chat_id,))
        row = c.fetchone(); conn.close()
        if not row or not row[1]:
            return  # Không có hoặc tắt
        try:
            chat = update.effective_chat
            count = await context.bot.get_chat_member_count(chat_id)
            msg = row[0].replace('{name}', member.first_name or "Bạn") \
                        .replace('{id}', str(member.id)) \
                        .replace('{group}', chat.title or "nhóm") \
                        .replace('{count}', str(count))
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logger.error(f"❌ mod_send_welcome: {e}")

    # ==================== NỘI QUY ====================

    async def mod_setrules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setrules [nội quy] — Đặt nội quy nhóm"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/setrules [nội dung nội quy]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        rules_text = " ".join(context.args)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_rules (group_id, rules, set_by, updated_at)
                     VALUES (?, ?, ?, ?)''',
                  (chat_id, rules_text, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text("✅ Đã cập nhật nội quy nhóm!\nDùng /rules để xem.")

    async def mod_rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/rules — Xem nội quy nhóm"""
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT rules FROM mod_rules WHERE group_id=?", (chat_id,))
        row = c.fetchone(); conn.close()
        if not row:
            await update.message.reply_text("❌ Nhóm này chưa có nội quy!\nAdmin dùng /setrules để đặt."); return
        await update.message.reply_text(
            f"📋 *NỘI QUY NHÓM*\n━━━━━━━━━━━━━━━━\n\n{row[0]}\n\n🕐 {format_vn_time()}",
            parse_mode=ParseMode.MARKDOWN)

    # ==================== LỌC TỪ KHÓA (FILTERS) ====================

    async def mod_filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/filter [từ khóa] [reply/nội dung] — Thêm bộ lọc tự động"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text(
                "📖 Cách dùng:\n"
                "`/filter [từ khóa] [nội dung trả lời]`\n"
                "Hoặc reply vào tin nhắn: `/filter [từ khóa]`\n\n"
                "Ví dụ: `/filter xin chào Chào mừng bạn!`\n"
                "Để xóa: `/unfilter [từ khóa]`",
                parse_mode=ParseMode.MARKDOWN); return
        keyword = context.args[0].lower()
        if update.message.reply_to_message:
            reply_text = update.message.reply_to_message.text or ""
        else:
            reply_text = " ".join(context.args[1:]) if len(context.args) > 1 else ""
        action = "delete" if not reply_text else "reply"
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_filters (group_id, keyword, action, reply, added_by, created_at)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (chat_id, keyword, action, reply_text, user_id,
                   get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ *Đã thêm filter:*\n🔍 Từ khóa: `{keyword}`\n"
            f"⚡ Hành động: {'Trả lời' if action=='reply' else 'Xóa tin nhắn'}"
            + (f"\n💬 Nội dung: {reply_text}" if reply_text else ""),
            parse_mode=ParseMode.MARKDOWN)

    async def mod_unfilter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/unfilter [từ khóa] — Xóa bộ lọc"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/unfilter [từ khóa]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        keyword = context.args[0].lower()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM mod_filters WHERE group_id=? AND keyword=?", (chat_id, keyword))
        deleted = c.rowcount; conn.commit(); conn.close()
        if deleted:
            await update.message.reply_text(f"✅ Đã xóa filter `{keyword}`", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"❌ Không tìm thấy filter `{keyword}`", parse_mode=ParseMode.MARKDOWN)

    async def mod_filters_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/filters — Xem danh sách bộ lọc"""
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT keyword, action, reply FROM mod_filters WHERE group_id=? ORDER BY keyword", (chat_id,))
        rows = c.fetchall(); conn.close()
        if not rows:
            await update.message.reply_text("❌ Nhóm này chưa có filter nào!"); return
        msg = f"🔍 *DANH SÁCH FILTER* ({len(rows)} filter)\n━━━━━━━━━━━━━━━━\n\n"
        for kw, action, reply in rows:
            msg += f"• `{kw}` → {'💬 '+reply[:30] if reply else '🗑 Xóa tin'}\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def mod_check_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Kiểm tra tin nhắn có chứa từ khóa filter không, return True nếu đã xử lý"""
        if not update.message or not update.message.text:
            return False
        chat_id = update.effective_chat.id
        text = update.message.text.lower()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT keyword, action, reply FROM mod_filters WHERE group_id=?", (chat_id,))
        filters = c.fetchall(); conn.close()
        for keyword, action, reply in filters:
            if keyword in text:
                try:
                    if action == 'delete':
                        await update.message.delete()
                    elif action == 'reply' and reply:
                        await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
                except: pass
                return True
        return False

    # ==================== LỆNH TÙY CHỈNH (CUSTOM COMMANDS) ====================

    async def mod_addcmd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/addcmd [lệnh] [nội dung] — Thêm lệnh tùy chỉnh"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "📖 Cách dùng: `/addcmd [lệnh] [nội dung]`\n"
                "Ví dụ: `/addcmd fb Link FB: facebook.com/nhom`",
                parse_mode=ParseMode.MARKDOWN); return
        cmd = context.args[0].lower().replace('/', '')
        response = " ".join(context.args[1:])
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO mod_commands (group_id, command, response, added_by, created_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (chat_id, cmd, response, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ Đã thêm lệnh `/{cmd}`\nThử ngay: `/{cmd}`",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_delcmd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/delcmd [lệnh] — Xóa lệnh tùy chỉnh"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/delcmd [lệnh]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        cmd = context.args[0].lower().replace('/', '')
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM mod_commands WHERE group_id=? AND command=?", (chat_id, cmd))
        deleted = c.rowcount; conn.commit(); conn.close()
        if deleted:
            await update.message.reply_text(f"✅ Đã xóa lệnh `/{cmd}`", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"❌ Không tìm thấy lệnh `/{cmd}`", parse_mode=ParseMode.MARKDOWN)

    async def mod_cmds_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/cmds — Xem danh sách lệnh tùy chỉnh"""
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT command, response FROM mod_commands WHERE group_id=? ORDER BY command", (chat_id,))
        rows = c.fetchall(); conn.close()
        if not rows:
            await update.message.reply_text("❌ Nhóm này chưa có lệnh tùy chỉnh nào!"); return
        msg = f"⚡ *LỆNH TÙY CHỈNH* ({len(rows)} lệnh)\n━━━━━━━━━━━━━━━━\n\n"
        for cmd, resp in rows:
            msg += f"• `/{cmd}` — {resp[:40]}{'...' if len(resp)>40 else ''}\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def mod_check_custom_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Kiểm tra lệnh tùy chỉnh trong tin nhắn"""
        if not update.message or not update.message.text:
            return False
        chat_id = update.effective_chat.id
        text = update.message.text.strip()
        if not text.startswith('/'):
            return False
        cmd = text.split()[0][1:].split('@')[0].lower()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT response FROM mod_commands WHERE group_id=? AND command=?", (chat_id, cmd))
        row = c.fetchone(); conn.close()
        if row:
            await update.message.reply_text(row[0], parse_mode=ParseMode.MARKDOWN)
            return True
        return False

    # ==================== PURGE ====================

    async def mod_purge_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/purge — Reply vào tin nhắn đầu tiên cần xóa → xóa từ đó đến hiện tại"""
        if not await mod_check_admin(update): return
        if not update.message.reply_to_message:
            await update.message.reply_text("❌ Reply vào tin nhắn đầu tiên muốn xóa!"); return
        chat_id = update.effective_chat.id
        start_id = update.message.reply_to_message.message_id
        end_id = update.message.message_id
        deleted = 0
        failed = 0
        for msg_id in range(start_id, end_id + 1):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                deleted += 1
            except:
                failed += 1
        mod_log(chat_id, update.effective_user.id, 0, "purge", f"Xóa {deleted} tin nhắn")
        notice = await context.bot.send_message(
            chat_id=chat_id,
            text=f"🧹 Đã xóa *{deleted}* tin nhắn.\n🕐 {format_vn_time()}",
            parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(5)
        try:
            await notice.delete()
        except: pass

    async def mod_spurge_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/spurge — Xóa không để lại thông báo"""
        if not await mod_check_admin(update): return
        if not update.message.reply_to_message:
            await update.message.reply_text("❌ Reply vào tin nhắn đầu tiên muốn xóa!"); return
        chat_id = update.effective_chat.id
        start_id = update.message.reply_to_message.message_id
        end_id = update.message.message_id
        for msg_id in range(start_id, end_id + 1):
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except: pass
        mod_log(chat_id, update.effective_user.id, 0, "spurge")

    # ==================== NHẬT KÝ ADMIN (LOGS) ====================

    async def mod_logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/adminlogs — Xem nhật ký hành động admin"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        limit = int(context.args[0]) if context.args else 20
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''SELECT action_by, target_user, action, reason, created_at
                     FROM mod_logs WHERE group_id=? ORDER BY created_at DESC LIMIT ?''',
                  (chat_id, limit))
        rows = c.fetchall(); conn.close()
        if not rows:
            await update.message.reply_text("📋 Chưa có hành động nào được ghi lại."); return
        msg = f"📋 *NHẬT KÝ ADMIN* (mới nhất {len(rows)})\n━━━━━━━━━━━━━━━━\n\n"
        action_icons = {
            'ban': '🚫', 'unban': '✅', 'kick': '👢', 'mute': '🔇',
            'unmute': '🔊', 'warn': '⚠️', 'purge': '🧹',
            'auto_ban_flood': '🌊🚫', 'auto_mute_flood': '🌊🔇'
        }
        for action_by, target, action, reason, at in rows:
            icon = action_icons.get(action, '📌')
            msg += f"{icon} `{action_by}` → `{target}` | *{action}*"
            if reason:
                msg += f" — {reason[:30]}"
            msg += f" _{at[11:16]}_\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    # ==================== BÁO CÁO VI PHẠM (REPORT) ====================

    async def mod_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/report [lý do] — Báo cáo tin nhắn (reply vào tin cần báo cáo)"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        if not update.message.reply_to_message:
            await update.message.reply_text("❌ Reply vào tin nhắn muốn báo cáo!"); return
        target = update.message.reply_to_message.from_user
        target_msg_id = update.message.reply_to_message.message_id
        reason = " ".join(context.args) if context.args else "Không có lý do"
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO mod_reports (group_id, reporter_id, target_user, message_id, reason, created_at)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (chat_id, user_id, target.id, target_msg_id, reason,
                   get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        report_id = c.lastrowid; conn.commit(); conn.close()
        # Xác nhận với người báo cáo
        await update.message.reply_text(
            f"✅ *Đã ghi nhận báo cáo #{report_id}*\nAdmin sẽ xem xét sớm nhất.",
            parse_mode=ParseMode.MARKDOWN)
        # Thông báo cho admin (cố gắng ping admin trong nhóm)
        try:
            keyboard = [[
                InlineKeyboardButton("🚫 Ban", callback_data=f"mod_rpt_ban_{target.id}_{report_id}"),
                InlineKeyboardButton("🔇 Mute", callback_data=f"mod_rpt_mute_{target.id}_{report_id}"),
                InlineKeyboardButton("✅ Bỏ qua", callback_data=f"mod_rpt_ignore_{report_id}"),
            ]]
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🚨 *BÁO CÁO VI PHẠM #{report_id}*\n━━━━━━━━━━━━━━━━\n\n"
                     f"👤 Người báo cáo: `{user_id}`\n"
                     f"🎯 Bị báo cáo: `{target.id}` ({target.first_name})\n"
                     f"📝 Lý do: {reason}\n\n"
                     f"⚠️ @admin Vui lòng xử lý!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard),
                reply_to_message_id=target_msg_id
            )
        except Exception as e:
            logger.error(f"❌ Report notify error: {e}")

    # ==================== FEDERATION (LIÊN MINH) ====================

    async def mod_newfed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/newfed [tên] — Tạo liên minh chống spam mới"""
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/newfed [tên liên minh]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        import uuid
        fed_name = " ".join(context.args)
        fed_id = str(uuid.uuid4())[:8].upper()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT INTO mod_federations (fed_id, fed_name, owner_id, created_at)
                     VALUES (?, ?, ?, ?)''',
                  (fed_id, fed_name, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"🏛 *LIÊN MINH MỚI*\n━━━━━━━━━━━━━━━━\n\n"
            f"🏷 Tên: *{fed_name}*\n"
            f"🆔 ID: `{fed_id}`\n\n"
            f"Thêm nhóm vào liên minh: `/joinfed {fed_id}`\n"
            f"Ban user toàn liên minh: `/fban {fed_id} [user_id]`",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_joinfed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/joinfed [fed_id] — Thêm nhóm vào liên minh"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/joinfed [fed_id]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        fed_id = context.args[0].upper()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT fed_name FROM mod_federations WHERE fed_id=?", (fed_id,))
        fed = c.fetchone()
        if not fed:
            conn.close()
            await update.message.reply_text(f"❌ Không tìm thấy liên minh `{fed_id}`",
                                             parse_mode=ParseMode.MARKDOWN); return
        c.execute('''INSERT OR REPLACE INTO mod_fed_members (fed_id, group_id, joined_at)
                     VALUES (?, ?, ?)''',
                  (fed_id, chat_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
        await update.message.reply_text(
            f"✅ Đã tham gia liên minh *{fed[0]}* (`{fed_id}`)\n"
            f"User bị fban trong liên minh sẽ tự động bị ban tại nhóm này.",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_leavefed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/leavefed — Rời liên minh"""
        if not await mod_check_admin(update): return
        chat_id = update.effective_chat.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM mod_fed_members WHERE group_id=?", (chat_id,))
        conn.commit(); conn.close()
        await update.message.reply_text("✅ Đã rời khỏi liên minh.")

    async def mod_fban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/fban [fed_id] [user_id] [lý do] — Ban user khỏi toàn liên minh"""
        user_id = update.effective_user.id
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "📖 Cách dùng: `/fban [fed_id] [user_id] [lý do]`",
                parse_mode=ParseMode.MARKDOWN); return
        fed_id = context.args[0].upper()
        try:
            target_id = int(context.args[1])
        except:
            await update.message.reply_text("❌ user_id phải là số!"); return
        # Kiểm tra quyền: phải là owner của fed
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT owner_id, fed_name FROM mod_federations WHERE fed_id=?", (fed_id,))
        fed = c.fetchone()
        if not fed:
            conn.close()
            await update.message.reply_text(f"❌ Không tìm thấy liên minh `{fed_id}`",
                                             parse_mode=ParseMode.MARKDOWN); return
        if fed[0] != user_id and not is_owner(user_id):
            conn.close()
            await update.message.reply_text("❌ Chỉ owner liên minh mới fban được!"); return
        reason = " ".join(context.args[2:]) or "Không có lý do"
        c.execute('''INSERT OR REPLACE INTO mod_fed_bans (fed_id, user_id, reason, banned_by, banned_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (fed_id, target_id, reason, user_id, get_vn_time().strftime("%Y-%m-%d %H:%M:%S")))
        # Lấy tất cả nhóm trong liên minh
        c.execute("SELECT group_id FROM mod_fed_members WHERE fed_id=?", (fed_id,))
        groups = c.fetchall(); conn.commit(); conn.close()
        banned = 0
        for (gid,) in groups:
            try:
                await context.bot.ban_chat_member(chat_id=gid, user_id=target_id)
                banned += 1
            except: pass
        await update.message.reply_text(
            f"🏛 *FBAN THỰC HIỆN*\n━━━━━━━━━━━━━━━━\n\n"
            f"👤 User: `{target_id}`\n"
            f"🏷 Liên minh: *{fed[1]}*\n"
            f"📝 Lý do: {reason}\n"
            f"🚫 Banned ở: *{banned}*/{len(groups)} nhóm\n"
            f"🕐 {format_vn_time()}", parse_mode=ParseMode.MARKDOWN)

    async def mod_funban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/funban [fed_id] [user_id] — Gỡ fban"""
        user_id = update.effective_user.id
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("📖 Cách dùng: `/funban [fed_id] [user_id]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        fed_id = context.args[0].upper()
        try:
            target_id = int(context.args[1])
        except:
            await update.message.reply_text("❌ user_id phải là số!"); return
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM mod_fed_bans WHERE fed_id=? AND user_id=?", (fed_id, target_id))
        c.execute("SELECT group_id FROM mod_fed_members WHERE fed_id=?", (fed_id,))
        groups = c.fetchall(); conn.commit(); conn.close()
        unbanned = 0
        for (gid,) in groups:
            try:
                await context.bot.unban_chat_member(chat_id=gid, user_id=target_id)
                unbanned += 1
            except: pass
        await update.message.reply_text(
            f"✅ Đã gỡ fban `{target_id}` ở *{unbanned}* nhóm.",
            parse_mode=ParseMode.MARKDOWN)

    async def mod_fedinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/fedinfo [fed_id] — Xem thông tin liên minh"""
        if not context.args:
            await update.message.reply_text("📖 Cách dùng: `/fedinfo [fed_id]`",
                                             parse_mode=ParseMode.MARKDOWN); return
        fed_id = context.args[0].upper()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT fed_name, owner_id, created_at FROM mod_federations WHERE fed_id=?", (fed_id,))
        fed = c.fetchone()
        if not fed:
            conn.close()
            await update.message.reply_text(f"❌ Không tìm thấy liên minh `{fed_id}`",
                                             parse_mode=ParseMode.MARKDOWN); return
        c.execute("SELECT COUNT(*) FROM mod_fed_members WHERE fed_id=?", (fed_id,))
        group_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM mod_fed_bans WHERE fed_id=?", (fed_id,))
        ban_count = c.fetchone()[0]
        conn.close()
        await update.message.reply_text(
            f"🏛 *THÔNG TIN LIÊN MINH*\n━━━━━━━━━━━━━━━━\n\n"
            f"🏷 Tên: *{fed[0]}*\n"
            f"🆔 ID: `{fed_id}`\n"
            f"👑 Owner: `{fed[1]}`\n"
            f"🏘 Số nhóm: *{group_count}*\n"
            f"🚫 Đang fban: *{ban_count}* users\n"
            f"📅 Tạo: {fed[2][:10]}",
            parse_mode=ParseMode.MARKDOWN)

    # ==================== CALLBACK HANDLER CHO MODERATION ====================


    # ==================== MODERATION MENU (INLINE UI) ====================

    async def mod_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await mod_check_admin(update): return
        await _mod_send_main_menu(update.message, update.effective_chat.id)

    async def _mod_send_main_menu(msg_or_query, chat_id, edit=False):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT enabled, captcha_type FROM mod_captcha_config WHERE group_id=?", (chat_id,))
        cap = c.fetchone() or (0, 'button')
        c.execute("SELECT enabled, max_msgs, interval_sec, action FROM mod_flood_config WHERE group_id=?", (chat_id,))
        flood = c.fetchone() or (0, 5, 5, 'mute')
        c.execute("SELECT enabled FROM mod_welcome WHERE group_id=?", (chat_id,))
        welcome = c.fetchone()
        c.execute("SELECT rules FROM mod_rules WHERE group_id=?", (chat_id,))
        rules_row = c.fetchone()
        c.execute("SELECT COUNT(*) FROM mod_filters WHERE group_id=?", (chat_id,))
        filter_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM mod_commands WHERE group_id=?", (chat_id,))
        cmd_count = c.fetchone()[0]
        c.execute("SELECT max_warns, action FROM mod_warn_config WHERE group_id=?", (chat_id,))
        warn_cfg = c.fetchone() or (3, 'mute')
        c.execute("SELECT fed_id FROM mod_fed_members WHERE group_id=?", (chat_id,))
        fed = c.fetchone()
        conn.close()
        cap_on = cap[0] == 1
        flood_on = flood[0] == 1
        welcome_on = welcome and welcome[0] == 1
        rules_set = rules_row is not None
        keyboard = [
            [InlineKeyboardButton(f"{'🟢' if cap_on else '🔴'} CAPTCHA ({cap[1]})", callback_data=f"mod_menu_captcha_{chat_id}"),
             InlineKeyboardButton(f"{'🟢' if flood_on else '🔴'} Anti-flood", callback_data=f"mod_menu_flood_{chat_id}")],
            [InlineKeyboardButton(f"{'🟢' if welcome_on else '🔴'} Chao mung", callback_data=f"mod_menu_welcome_{chat_id}"),
             InlineKeyboardButton(f"{'🟢' if rules_set else '⚪'} Noi quy", callback_data=f"mod_menu_rules_{chat_id}")],
            [InlineKeyboardButton(f"⚠️ Warns (max:{warn_cfg[0]}→{warn_cfg[1]})", callback_data=f"mod_menu_warn_{chat_id}")],
            [InlineKeyboardButton(f"🔍 Filters ({filter_count})", callback_data=f"mod_menu_filters_{chat_id}"),
             InlineKeyboardButton(f"⚡ Lenh TC ({cmd_count})", callback_data=f"mod_menu_cmds_{chat_id}")],
            [InlineKeyboardButton("👮 Hanh dong thanh vien", callback_data=f"mod_menu_actions_{chat_id}"),
             InlineKeyboardButton("📋 Nhat ky", callback_data=f"mod_menu_logs_{chat_id}")],
            [InlineKeyboardButton(f"🏛️ Lien minh {'✅' if fed else '❌'}", callback_data=f"mod_menu_fed_{chat_id}"),
             InlineKeyboardButton("🔄 Lam moi", callback_data=f"mod_menu_refresh_{chat_id}")],
        ]
        text = (
            "🛡️ *BANG DIEU KHIEN NHOM*\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "🟢 Bat  🔴 Tat  ⚪ Chua cai\n\n"
            "Nhan vao de cau hinh tung tinh nang.\n"
            + "🕐 " + format_vn_time()
        )
        markup = InlineKeyboardMarkup(keyboard)
        if edit:
            await safe_edit_message(msg_or_query, text, reply_markup=markup)
        else:
            await msg_or_query.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)

    async def _mod_panel_captcha(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT enabled, captcha_type, timeout_sec FROM mod_captcha_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone() or (0, 'button', 60)
        conn.close()
        enabled, ctype, timeout = cfg
        keyboard = [
            [InlineKeyboardButton(f"{'✅' if enabled else '⬜'} Bat CAPTCHA", callback_data=f"mod_cap_toggle_{chat_id}")],
            [InlineKeyboardButton(f"{'🔵' if ctype=='button' else '⬜'} Nut bam", callback_data=f"mod_cap_type_{chat_id}_button"),
             InlineKeyboardButton(f"{'🔵' if ctype=='math' else '⬜'} Toan hoc", callback_data=f"mod_cap_type_{chat_id}_math")],
            [InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")],
        ]
        await safe_edit_message(query,
            f"🤖 *CAPTCHA*\nTrang thai: {'🟢 BAT' if enabled else '🔴 TAT'}\nLoai: *{ctype}* | Timeout: *{timeout}s*",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_flood(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT enabled, max_msgs, interval_sec, action FROM mod_flood_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone() or (0, 5, 5, 'mute')
        conn.close()
        enabled, max_m, interval, action = cfg
        keyboard = [
            [InlineKeyboardButton(f"{'✅' if enabled else '⬜'} Bat Anti-flood", callback_data=f"mod_flood_toggle_{chat_id}")],
            [InlineKeyboardButton("➖", callback_data=f"mod_flood_max_{chat_id}_dec"),
             InlineKeyboardButton(f"Max: {max_m} tin", callback_data="mod_noop"),
             InlineKeyboardButton("➕", callback_data=f"mod_flood_max_{chat_id}_inc")],
            [InlineKeyboardButton("➖", callback_data=f"mod_flood_int_{chat_id}_dec"),
             InlineKeyboardButton(f"Trong: {interval}s", callback_data="mod_noop"),
             InlineKeyboardButton("➕", callback_data=f"mod_flood_int_{chat_id}_inc")],
            [InlineKeyboardButton(f"{'🔵' if action=='mute' else '⬜'} Mute", callback_data=f"mod_flood_act_{chat_id}_mute"),
             InlineKeyboardButton(f"{'🔵' if action=='kick' else '⬜'} Kick", callback_data=f"mod_flood_act_{chat_id}_kick"),
             InlineKeyboardButton(f"{'🔵' if action=='ban' else '⬜'} Ban", callback_data=f"mod_flood_act_{chat_id}_ban")],
            [InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")],
        ]
        await safe_edit_message(query,
            f"🌊 *ANTI-FLOOD*\nTrang thai: {'🟢 BAT' if enabled else '🔴 TAT'}\nGioi han: *{max_m}* tin/{interval}s → *{action.upper()}*",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_warn(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT max_warns, action FROM mod_warn_config WHERE group_id=?", (chat_id,))
        cfg = c.fetchone() or (3, 'mute')
        conn.close()
        max_w, action = cfg
        keyboard = [
            [InlineKeyboardButton("➖", callback_data=f"mod_warn_max_{chat_id}_dec"),
             InlineKeyboardButton(f"Toi da: {max_w} lan", callback_data="mod_noop"),
             InlineKeyboardButton("➕", callback_data=f"mod_warn_max_{chat_id}_inc")],
            [InlineKeyboardButton(f"{'🔵' if action=='mute' else '⬜'} Mute", callback_data=f"mod_warn_act_{chat_id}_mute"),
             InlineKeyboardButton(f"{'🔵' if action=='kick' else '⬜'} Kick", callback_data=f"mod_warn_act_{chat_id}_kick"),
             InlineKeyboardButton(f"{'🔵' if action=='ban' else '⬜'} Ban", callback_data=f"mod_warn_act_{chat_id}_ban")],
            [InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")],
        ]
        await safe_edit_message(query,
            f"⚠️ *CANH CAO*\nKhi dat *{max_w}* lan → *{action.upper()}*\nDung ➕/➖ de dieu chinh.",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_welcome(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT message, enabled FROM mod_welcome WHERE group_id=?", (chat_id,))
        row = c.fetchone()
        conn.close()
        enabled = row and row[1] == 1
        msg_text = (row[0][:60] + "...") if row and row[0] and len(row[0]) > 60 else (row[0] if row else "Chua dat")
        keyboard = [
            [InlineKeyboardButton(f"{'✅' if enabled else '⬜'} Bat chao mung", callback_data=f"mod_welcome_toggle_{chat_id}")],
            [InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")],
        ]
        await safe_edit_message(query,
            f"👋 *CHAO MUNG*\nTrang thai: {'🟢 BAT' if enabled else '🔴 TAT'}\n\nNoi dung: _{msg_text}_\n\nBien: {{name}} {{id}} {{group}} {{count}}\nDat: /setwelcome [noi dung]",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_rules(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT rules FROM mod_rules WHERE group_id=?", (chat_id,))
        row = c.fetchone()
        conn.close()
        rules_text = (row[0][:100] + "...") if row and len(row[0]) > 100 else (row[0] if row else "Chua dat")
        keyboard = [[InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")]]
        await safe_edit_message(query,
            f"📋 *NOI QUY*\n_{rules_text}_\n\nDat: /setrules [noi dung]",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_actions(query, chat_id):
        keyboard = [
            [InlineKeyboardButton("🚫 Ban", callback_data=f"mod_act_guide_ban_{chat_id}"),
             InlineKeyboardButton("✅ Unban", callback_data=f"mod_act_guide_unban_{chat_id}"),
             InlineKeyboardButton("👢 Kick", callback_data=f"mod_act_guide_kick_{chat_id}")],
            [InlineKeyboardButton("🔇 Mute", callback_data=f"mod_act_guide_mute_{chat_id}"),
             InlineKeyboardButton("🔊 Unmute", callback_data=f"mod_act_guide_unmute_{chat_id}"),
             InlineKeyboardButton("⚠️ Warn", callback_data=f"mod_act_guide_warn_{chat_id}")],
            [InlineKeyboardButton("🧹 Purge: reply + /purge", callback_data="mod_noop")],
            [InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")],
        ]
        await safe_edit_message(query,
            "👮 *HANH DONG THANH VIEN*\nChon hanh dong de xem huong dan.\nHoac reply tin nhan + go lenh truc tiep.",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_filters(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, keyword, action, reply FROM mod_filters WHERE group_id=? ORDER BY keyword LIMIT 20", (chat_id,))
        rows = c.fetchall()
        conn.close()
        keyboard = []
        for fid, kw, action, reply in rows:
            icon = "💬" if reply else "🗑"
            keyboard.append([
                InlineKeyboardButton(f"{icon} {kw[:22]}", callback_data="mod_noop"),
                InlineKeyboardButton("❌ Xoa", callback_data=f"mod_filter_del_{chat_id}_{fid}"),
            ])
        keyboard.append([InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")])
        await safe_edit_message(query,
            f"🔍 *FILTERS* ({len(rows)})\nNhan ❌ de xoa. Them: /filter [tu khoa] [tra loi]",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_cmds(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, command, response FROM mod_commands WHERE group_id=? ORDER BY command LIMIT 20", (chat_id,))
        rows = c.fetchall()
        conn.close()
        keyboard = []
        for rid, cmd, resp in rows:
            keyboard.append([
                InlineKeyboardButton(f"/{cmd}", callback_data="mod_noop"),
                InlineKeyboardButton(f"{resp[:18]}", callback_data="mod_noop"),
                InlineKeyboardButton("❌", callback_data=f"mod_cmd_del_{chat_id}_{rid}"),
            ])
        keyboard.append([InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")])
        await safe_edit_message(query,
            f"⚡ *LENH TUY CHINH* ({len(rows)})\nNhan ❌ de xoa. Them: /addcmd [lenh] [noi dung]",
            reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_logs(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT action_by, target_user, action, reason, created_at FROM mod_logs WHERE group_id=? ORDER BY created_at DESC LIMIT 15", (chat_id,))
        rows = c.fetchall()
        conn.close()
        icons = {'ban':'🚫','unban':'✅','kick':'👢','mute':'🔇','unmute':'🔊','warn':'⚠️','purge':'🧹','auto_ban_flood':'🌊🚫','auto_mute_flood':'🌊🔇'}
        msg = f"📋 *NHAT KY* ({len(rows)})\n━━━━━━━━━━━━━━━━"
        if not rows:
            msg += "\nChua co hanh dong nao."
        for by, target, action, reason, at in rows:
            icon = icons.get(action, '📌')
            line = f"\n{icon} `{by}`→`{target}` *{action}*"
            if reason:
                line += f" _{reason[:15]}_"
            line += f" {at[11:16]}"
            msg += line
        keyboard = [[InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")]]
        await safe_edit_message(query, msg, reply_markup=InlineKeyboardMarkup(keyboard))

    async def _mod_panel_fed(query, chat_id):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT f.fed_id, f.fed_name, (SELECT COUNT(*) FROM mod_fed_bans WHERE fed_id=f.fed_id) FROM mod_federations f JOIN mod_fed_members m ON f.fed_id=m.fed_id WHERE m.group_id=?", (chat_id,))
        fed = c.fetchone()
        conn.close()
        keyboard = []
        if fed:
            keyboard.append([InlineKeyboardButton(f"🏛 {fed[1]} ({fed[2]} bans)", callback_data="mod_noop")])
            keyboard.append([InlineKeyboardButton("🚪 Roi lien minh", callback_data=f"mod_fed_leave_{chat_id}")])
        else:
            keyboard.append([InlineKeyboardButton("Tao: /newfed [ten]", callback_data="mod_noop")])
            keyboard.append([InlineKeyboardButton("Tham gia: /joinfed [id]", callback_data="mod_noop")])
        keyboard.append([InlineKeyboardButton("🔙 Quay lai", callback_data=f"mod_menu_main_{chat_id}")])
        status = f"✅ Trong: *{fed[1]}* | 🚫 {fed[2]} fbans" if fed else "❌ Chua tham gia lien minh nao"
        await safe_edit_message(query,
            f"🏛️ *LIEN MINH*\n{status}\n\nTao: /newfed [ten]\nTham gia: /joinfed [id]",
            reply_markup=InlineKeyboardMarkup(keyboard))


    async def handle_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Xử lý callbacks của moderation system"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        data = query.data
        chat_id = query.message.chat.id

        if data == "mod_noop":
            return

        # ── MENU NAVIGATION ──────────────────────────────────────────────
        if data.startswith("mod_menu_main_"):
            cid = int(data[len("mod_menu_main_"):])
            await _mod_send_main_menu(query, cid, edit=True); return

        if data.startswith("mod_menu_refresh_"):
            cid = int(data[len("mod_menu_refresh_"):])
            await _mod_send_main_menu(query, cid, edit=True); return

        if data.startswith("mod_menu_captcha_"):
            cid = int(data[len("mod_menu_captcha_"):])
            await _mod_panel_captcha(query, cid); return

        if data.startswith("mod_menu_flood_"):
            cid = int(data[len("mod_menu_flood_"):])
            await _mod_panel_flood(query, cid); return

        if data.startswith("mod_menu_warn_"):
            cid = int(data[len("mod_menu_warn_"):])
            await _mod_panel_warn(query, cid); return

        if data.startswith("mod_menu_welcome_"):
            cid = int(data[len("mod_menu_welcome_"):])
            await _mod_panel_welcome(query, cid); return

        if data.startswith("mod_menu_rules_"):
            cid = int(data[len("mod_menu_rules_"):])
            await _mod_panel_rules(query, cid); return

        if data.startswith("mod_menu_actions_"):
            cid = int(data[len("mod_menu_actions_"):])
            await _mod_panel_actions(query, cid); return

        if data.startswith("mod_menu_filters_"):
            cid = int(data[len("mod_menu_filters_"):])
            await _mod_panel_filters(query, cid); return

        if data.startswith("mod_menu_cmds_"):
            cid = int(data[len("mod_menu_cmds_"):])
            await _mod_panel_cmds(query, cid); return

        if data.startswith("mod_menu_logs_"):
            cid = int(data[len("mod_menu_logs_"):])
            await _mod_panel_logs(query, cid); return

        if data.startswith("mod_menu_fed_"):
            cid = int(data[len("mod_menu_fed_"):])
            await _mod_panel_fed(query, cid); return

        # ── CAPTCHA TOGGLE / TYPE ─────────────────────────────────────────
        if data.startswith("mod_cap_toggle_"):
            cid = int(data[len("mod_cap_toggle_"):])
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT enabled FROM mod_captcha_config WHERE group_id=?", (cid,))
            row = c.fetchone()
            new_val = 0 if (row and row[0]) else 1
            c.execute("INSERT OR REPLACE INTO mod_captcha_config (group_id, enabled, captcha_type, timeout_sec) VALUES (?, ?, COALESCE((SELECT captcha_type FROM mod_captcha_config WHERE group_id=?), 'button'), COALESCE((SELECT timeout_sec FROM mod_captcha_config WHERE group_id=?), 60))", (cid, new_val, cid, cid))
            conn.commit(); conn.close()
            await _mod_panel_captcha(query, cid); return

        if data.startswith("mod_cap_type_"):
            import re as _re
            m = _re.match(r"mod_cap_type_(-?\d+)_(button|math)", data)
            if m:
                cid, ctype = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO mod_captcha_config (group_id, enabled, captcha_type, timeout_sec) VALUES (?, COALESCE((SELECT enabled FROM mod_captcha_config WHERE group_id=?),0), ?, COALESCE((SELECT timeout_sec FROM mod_captcha_config WHERE group_id=?),60))", (cid, cid, ctype, cid))
                conn.commit(); conn.close()
                await _mod_panel_captcha(query, cid); return

        # ── FLOOD CONTROLS ───────────────────────────────────────────────
        if data.startswith("mod_flood_toggle_"):
            cid = int(data[len("mod_flood_toggle_"):])
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT enabled FROM mod_flood_config WHERE group_id=?", (cid,))
            row = c.fetchone()
            new_val = 0 if (row and row[0]) else 1
            c.execute("INSERT OR REPLACE INTO mod_flood_config (group_id, enabled, max_msgs, interval_sec, action) VALUES (?, ?, COALESCE((SELECT max_msgs FROM mod_flood_config WHERE group_id=?),5), COALESCE((SELECT interval_sec FROM mod_flood_config WHERE group_id=?),5), COALESCE((SELECT action FROM mod_flood_config WHERE group_id=?),'mute'))", (cid, new_val, cid, cid, cid))
            conn.commit(); conn.close()
            await _mod_panel_flood(query, cid); return

        if data.startswith("mod_flood_max_"):
            import re as _re
            m = _re.match(r"mod_flood_max_(-?\d+)_(inc|dec)", data)
            if m:
                cid, op = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT max_msgs FROM mod_flood_config WHERE group_id=?", (cid,))
                row = c.fetchone()
                cur = row[0] if row else 5
                new_val = max(2, cur + (1 if op == 'inc' else -1))
                c.execute("INSERT OR REPLACE INTO mod_flood_config (group_id, enabled, max_msgs, interval_sec, action) VALUES (?, COALESCE((SELECT enabled FROM mod_flood_config WHERE group_id=?),0), ?, COALESCE((SELECT interval_sec FROM mod_flood_config WHERE group_id=?),5), COALESCE((SELECT action FROM mod_flood_config WHERE group_id=?),'mute'))", (cid, cid, new_val, cid, cid))
                conn.commit(); conn.close()
                await _mod_panel_flood(query, cid); return

        if data.startswith("mod_flood_int_"):
            import re as _re
            m = _re.match(r"mod_flood_int_(-?\d+)_(inc|dec)", data)
            if m:
                cid, op = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT interval_sec FROM mod_flood_config WHERE group_id=?", (cid,))
                row = c.fetchone()
                cur = row[0] if row else 5
                new_val = max(2, cur + (1 if op == 'inc' else -1))
                c.execute("INSERT OR REPLACE INTO mod_flood_config (group_id, enabled, max_msgs, interval_sec, action) VALUES (?, COALESCE((SELECT enabled FROM mod_flood_config WHERE group_id=?),0), COALESCE((SELECT max_msgs FROM mod_flood_config WHERE group_id=?),5), ?, COALESCE((SELECT action FROM mod_flood_config WHERE group_id=?),'mute'))", (cid, cid, cid, new_val, cid))
                conn.commit(); conn.close()
                await _mod_panel_flood(query, cid); return

        if data.startswith("mod_flood_act_"):
            import re as _re
            m = _re.match(r"mod_flood_act_(-?\d+)_(mute|kick|ban)", data)
            if m:
                cid, action = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO mod_flood_config (group_id, enabled, max_msgs, interval_sec, action) VALUES (?, COALESCE((SELECT enabled FROM mod_flood_config WHERE group_id=?),0), COALESCE((SELECT max_msgs FROM mod_flood_config WHERE group_id=?),5), COALESCE((SELECT interval_sec FROM mod_flood_config WHERE group_id=?),5), ?)", (cid, cid, cid, cid, action))
                conn.commit(); conn.close()
                await _mod_panel_flood(query, cid); return

        # ── WARN CONTROLS ────────────────────────────────────────────────
        if data.startswith("mod_warn_max_"):
            import re as _re
            m = _re.match(r"mod_warn_max_(-?\d+)_(inc|dec)", data)
            if m:
                cid, op = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT max_warns FROM mod_warn_config WHERE group_id=?", (cid,))
                row = c.fetchone()
                cur = row[0] if row else 3
                new_val = max(1, cur + (1 if op == 'inc' else -1))
                c.execute("INSERT OR REPLACE INTO mod_warn_config (group_id, max_warns, action) VALUES (?, ?, COALESCE((SELECT action FROM mod_warn_config WHERE group_id=?),'mute'))", (cid, new_val, cid))
                conn.commit(); conn.close()
                await _mod_panel_warn(query, cid); return

        if data.startswith("mod_warn_act_"):
            import re as _re
            m = _re.match(r"mod_warn_act_(-?\d+)_(mute|kick|ban)", data)
            if m:
                cid, action = int(m.group(1)), m.group(2)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO mod_warn_config (group_id, max_warns, action) VALUES (?, COALESCE((SELECT max_warns FROM mod_warn_config WHERE group_id=?),3), ?)", (cid, cid, action))
                conn.commit(); conn.close()
                await _mod_panel_warn(query, cid); return

        # ── WELCOME TOGGLE ───────────────────────────────────────────────
        if data.startswith("mod_welcome_toggle_"):
            cid = int(data[len("mod_welcome_toggle_"):])
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT enabled FROM mod_welcome WHERE group_id=?", (cid,))
            row = c.fetchone()
            new_val = 0 if (row and row[0]) else 1
            if row:
                c.execute("UPDATE mod_welcome SET enabled=? WHERE group_id=?", (new_val, cid))
            else:
                c.execute("INSERT INTO mod_welcome (group_id, message, enabled) VALUES (?, '', ?)", (cid, new_val))
            conn.commit(); conn.close()
            await _mod_panel_welcome(query, cid); return

        # ── DELETE FILTER ────────────────────────────────────────────────
        if data.startswith("mod_filter_del_"):
            import re as _re
            m = _re.match(r"mod_filter_del_(-?\d+)_(\d+)", data)
            if m:
                cid, fid = int(m.group(1)), int(m.group(2))
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("DELETE FROM mod_filters WHERE id=? AND group_id=?", (fid, cid))
                conn.commit(); conn.close()
                await _mod_panel_filters(query, cid); return

        # ── DELETE CUSTOM CMD ────────────────────────────────────────────
        if data.startswith("mod_cmd_del_"):
            import re as _re
            m = _re.match(r"mod_cmd_del_(-?\d+)_(\d+)", data)
            if m:
                cid, rid = int(m.group(1)), int(m.group(2))
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("DELETE FROM mod_commands WHERE id=? AND group_id=?", (rid, cid))
                conn.commit(); conn.close()
                await _mod_panel_cmds(query, cid); return

        # ── FED LEAVE ────────────────────────────────────────────────────
        if data.startswith("mod_fed_leave_"):
            cid = int(data[len("mod_fed_leave_"):])
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("DELETE FROM mod_fed_members WHERE group_id=?", (cid,))
            conn.commit(); conn.close()
            await query.answer("✅ Đã rời liên minh!", show_alert=True)
            await _mod_panel_fed(query, cid); return

        # ── ACTION GUIDES ────────────────────────────────────────────────
        if data.startswith("mod_act_guide_"):
            import re as _re
            m = _re.match(r"mod_act_guide_(ban|unban|kick|mute|unmute|warn)_(-?\d+)", data)
            if m:
                action, cid = m.group(1), m.group(2)
                guides = {
                    'ban': '🚫 *BAN*\nReply tin nhan → /ban [ly do]\nHoac: /ban [user_id] [ly do]',
                    'unban': '✅ *UNBAN*\n/unban [user_id]',
                    'kick': '👢 *KICK*\nReply tin nhan → /kick [ly do]',
                    'mute': '🔇 *MUTE*\nReply tin nhan → /mute [30m/2h/1d] [ly do]\nVi du: /mute 1h Spam',
                    'unmute': '🔊 *UNMUTE*\nReply tin nhan → /unmute',
                    'warn': '⚠️ *WARN*\nReply tin nhan → /warn [ly do]\nXem: /warns [user_id]',
                }
                await safe_edit_message(query, guides.get(action, "Không tìm thấy hướng dẫn"),
                                        reply_markup=InlineKeyboardMarkup(keyboard)); return

        # CAPTCHA confirm (button type)
        if data.startswith("mod_captcha_"):
            parts = data.split("_")
            # mod_captcha_{chat_id}_{user_id} hoặc mod_captcha_{chat_id}_{user_id}_{answer}
            try:
                cap_chat = int(parts[2])
                cap_user = int(parts[3])
                user_answer = parts[4] if len(parts) > 4 else "confirmed"
            except:
                return
            if user_id != cap_user:
                await query.answer("❌ CAPTCHA này không phải của bạn!", show_alert=True); return
            # Kiểm tra đáp án
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT answer, message_id FROM mod_captcha_pending WHERE group_id=? AND user_id=?",
                      (cap_chat, cap_user))
            pending = c.fetchone()
            if not pending:
                await query.answer("⏱ CAPTCHA đã hết hạn!", show_alert=True)
                conn.close(); return
            correct_answer, msg_id = pending
            if user_answer == correct_answer or correct_answer == "confirmed":
                # Đúng → bật tiếng lại
                from telegram import ChatPermissions
                try:
                    await context.bot.restrict_chat_member(
                        chat_id=cap_chat, user_id=cap_user,
                        permissions=ChatPermissions(
                            can_send_messages=True, can_send_media_messages=True,
                            can_send_other_messages=True, can_add_web_page_previews=True
                        )
                    )
                except: pass
                c.execute("DELETE FROM mod_captcha_pending WHERE group_id=? AND user_id=?", (cap_chat, cap_user))
                conn.commit(); conn.close()
                try:
                    await context.bot.delete_message(chat_id=cap_chat, message_id=msg_id)
                except: pass
                await context.bot.send_message(cap_chat,
                    f"✅ `{cap_user}` đã xác nhận CAPTCHA thành công!", parse_mode=ParseMode.MARKDOWN)
            else:
                # Sai → kick
                c.execute("DELETE FROM mod_captcha_pending WHERE group_id=? AND user_id=?", (cap_chat, cap_user))
                conn.commit(); conn.close()
                try:
                    await context.bot.ban_chat_member(chat_id=cap_chat, user_id=cap_user)
                    await context.bot.unban_chat_member(chat_id=cap_chat, user_id=cap_user)
                    await context.bot.delete_message(chat_id=cap_chat, message_id=msg_id)
                except: pass
                await query.answer("❌ Sai đáp án! Bạn đã bị kick.", show_alert=True)

        # Report action
        elif data.startswith("mod_rpt_"):
            if not mod_is_admin(chat_id, user_id):
                await query.answer("❌ Chỉ admin mới xử lý được!", show_alert=True); return
            parts = data.split("_")
            action = parts[2]  # ban/mute/ignore
            if action == "ignore":
                report_id = int(parts[3])
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE mod_reports SET status='ignored' WHERE id=?", (report_id,))
                conn.commit(); conn.close()
                await safe_edit_message(query, f"✅ Báo cáo #{report_id} đã được bỏ qua.")
            else:
                target_id = int(parts[3])
                report_id = int(parts[4])
                try:
                    if action == "ban":
                        await context.bot.ban_chat_member(chat_id=chat_id, user_id=target_id)
                        mod_log(chat_id, user_id, target_id, "ban", f"Từ báo cáo #{report_id}")
                        await safe_edit_message(query, f"🚫 Đã ban user `{target_id}` (báo cáo #{report_id})")
                    elif action == "mute":
                        from telegram import ChatPermissions
                        until = datetime.utcnow() + timedelta(hours=1)
                        await context.bot.restrict_chat_member(
                            chat_id=chat_id, user_id=target_id,
                            permissions=ChatPermissions(can_send_messages=False),
                            until_date=until)
                        mod_log(chat_id, user_id, target_id, "mute", f"Từ báo cáo #{report_id}", "1h")
                        await safe_edit_message(query, f"🔇 Đã mute 1h user `{target_id}` (báo cáo #{report_id})")
                    conn2 = sqlite3.connect(DB_PATH)
                    c2 = conn2.cursor()
                    c2.execute("UPDATE mod_reports SET status='resolved' WHERE id=?", (report_id,))
                    conn2.commit(); conn2.close()
                except Exception as e:
                    await query.answer(f"❌ Lỗi: {e}", show_alert=True)

    # ==================== TÍCH HỢP VÀO new_chat_members & handle_message ====================
    # Các hàm này sẽ được gọi từ handler hiện có

    async def mod_on_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE, member):
        """Gọi khi có thành viên mới: chạy CAPTCHA và welcome"""
        await mod_captcha_join(update, context, member)
        if not mg_is_cross_banned(update.effective_chat.id, member.id):
            await mod_send_welcome(update, context, member)

    async def mod_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Gọi từ handle_message: kiểm tra flood, filter, custom cmd
           Return True nếu tin nhắn đã được xử lý (không cần xử lý tiếp)"""
        if not update.message or not update.message.text:
            return False
        chat_type = update.effective_chat.type
        if chat_type not in ['group', 'supergroup']:
            return False
        # 1. Check flood
        if await mod_check_flood(update, context):
            return True
        # 2. Check filters
        if await mod_check_filters(update, context):
            return True
        # 3. Check custom commands
        if await mod_check_custom_command(update, context):
            return True
        return False

    # ==================== FED CHECK KHI USER JOIN ====================

    async def mod_check_fed_ban(context, chat_id, user_id) -> bool:
        """Kiểm tra user có bị fban không, return True nếu bị ban"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT fed_id FROM mod_fed_members WHERE group_id=?", (chat_id,))
            feds = c.fetchall()
            for (fed_id,) in feds:
                c.execute("SELECT reason FROM mod_fed_bans WHERE fed_id=? AND user_id=?", (fed_id, user_id))
                ban = c.fetchone()
                if ban:
                    conn.close()
                    await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
                    await context.bot.send_message(chat_id,
                        f"🚫 `{user_id}` bị tự động ban do nằm trong fban list (lý do: {ban[0]})",
                        parse_mode=ParseMode.MARKDOWN)
                    return True
            conn.close()
        except Exception as e:
            logger.error(f"❌ mod_check_fed_ban: {e}")
        return False



    # ==================== SMART STARTUP ====================
    def smart_startup():
        logger.info("🚀 SMART STARTUP")
        logger.info(f"📊 Render mode: {render_config.is_render}")
        logger.info(f"💾 Memory limit: {render_config.memory_limit}MB")
        logger.info(f"⚙️ CPU limit: {render_config.cpu_limit}")
        logger.info(f"🌐 Render URL: {render_config.render_url}")

        EXPORT_DIR = os.path.join(DATA_DIR, 'exports')
        os.makedirs(EXPORT_DIR, exist_ok=True)
        logger.info(f"📁 Export directory: {EXPORT_DIR}")
        
        test_file = os.path.join(EXPORT_DIR, 'test.txt')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            logger.info("✅ Export directory is writable")
        except Exception as e:
            logger.error(f"❌ Export directory not writable: {e}")
        
        if not init_database():
            logger.error("❌ KHÔNG THỂ KHỞI TẠO DATABASE")
            time.sleep(5)
            
        def fix_database_constraints():
            """Sửa các ràng buộc trong database"""
            conn = None
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                # Bật khóa ngoại
                c.execute("PRAGMA foreign_keys = ON")
                logger.info("✅ Đã bật FOREIGN_KEY support")
                
                # Kiểm tra bảng expenses có cột category_id không
                c.execute("PRAGMA table_info(expenses)")
                columns = [col[1] for col in c.fetchall()]
                
                if 'category_id' in columns:
                    logger.info("✅ Bảng expenses có cột category_id")
                else:
                    logger.warning("⚠️ Bảng expenses thiếu cột category_id - Đang thêm...")
                    try:
                        c.execute("ALTER TABLE expenses ADD COLUMN category_id INTEGER")
                        logger.info("✅ Đã thêm cột category_id")
                    except Exception as e:
                        logger.error(f"❌ Không thể thêm cột category_id: {e}")
                
                # Kiểm tra ràng buộc khóa ngoại
                c.execute("PRAGMA foreign_key_list(expenses)")
                fk_list = c.fetchall()
                
                has_fk = False
                for fk in fk_list:
                    if len(fk) >= 5 and fk[3] == 'category_id' and fk[2] == 'expense_categories':
                        has_fk = True
                        logger.info(f"✅ Có ràng buộc khóa ngoại: {fk}")
                        break
                
                if not has_fk:
                    logger.warning("⚠️ Thiếu ràng buộc khóa ngoại giữa expenses và expense_categories")
                    logger.warning("   Có thể gây lỗi khi xóa danh mục")
                
                # Xóa các chi tiêu có category_id không tồn tại
                c.execute('''
                    SELECT COUNT(*) FROM expenses 
                    WHERE category_id IS NOT NULL 
                    AND category_id NOT IN (SELECT id FROM expense_categories)
                ''')
                orphan_count = c.fetchone()[0]
                
                if orphan_count > 0:
                    logger.warning(f"⚠️ Phát hiện {orphan_count} chi tiêu orphan (không có danh mục)")
                    c.execute('''
                        DELETE FROM expenses 
                        WHERE category_id IS NOT NULL 
                        AND category_id NOT IN (SELECT id FROM expense_categories)
                    ''')
                    deleted = c.rowcount
                    logger.info(f"✅ Đã xóa {deleted} chi tiêu orphan")
                
                conn.commit()
                
                # Đếm số lượng để báo cáo
                c.execute("SELECT COUNT(*) FROM expense_categories")
                cat_count = c.fetchone()[0]
                
                c.execute("SELECT COUNT(*) FROM expenses")
                exp_count = c.fetchone()[0]
                
                logger.info(f"📊 Thống kê: {cat_count} danh mục, {exp_count} chi tiêu")
                logger.info("✅ Đã kiểm tra và sửa ràng buộc database")
                
            except Exception as e:
                logger.error(f"❌ Lỗi sửa database: {e}")
            finally:
                if conn:
                    conn.close()
            
        # ===== THÊM PHẦN KIỂM TRA TỔNG THỂ VÀO ĐÂY =====
        logger.info("🔍 KIỂM TRA TỔNG THỂ HỆ THỐNG...")
        
        # 1. Migrate dữ liệu admin cũ
        logger.info("🔄 Kiểm tra và migrate dữ liệu admin...")
        try:
            migrate_admin_data()
        except Exception as e:
            logger.error(f"❌ Lỗi migrate admin: {e}")
        
        # 2. Load group owners
        logger.info("🔄 Loading group owners...")
        load_group_owners()
        
        # 3. Kiểm tra dữ liệu trong database
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Đếm số lượng staff trong permissions
            c.execute("SELECT COUNT(*) FROM permissions WHERE role = 'staff'")
            staff_count = c.fetchone()[0]
            
            # Đếm số lượng admin cũ (nếu bảng còn tồn tại)
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='group_admins'")
            old_table_exists = c.fetchone()
            
            old_admin_count = 0
            if old_table_exists:
                c.execute("SELECT COUNT(*) FROM group_admins")
                old_admin_count = c.fetchone()[0]
            
            # Đếm số lượng group owners
            c.execute("SELECT COUNT(*) FROM group_owners")
            group_owners_count = c.fetchone()[0]
            
            # Đếm tổng số user
            c.execute("SELECT COUNT(*) FROM users")
            users_count = c.fetchone()[0]
            
            # Đếm tổng số giao dịch
            c.execute("SELECT COUNT(*) FROM portfolio")
            portfolio_count = c.fetchone()[0]
            
            # Đếm tổng số thu chi
            c.execute("SELECT COUNT(*) FROM incomes")
            income_count = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM expenses")
            expense_count = c.fetchone()[0]
            
            conn.close()
            
            # In báo cáo kiểm tra
            logger.info("📊 *BÁO CÁO KIỂM TRA DATABASE*")
            logger.info(f"   • Users: {users_count}")
            logger.info(f"   • Group Owners: {group_owners_count}")
            logger.info(f"   • Staff (permissions): {staff_count}")
            if old_table_exists:
                logger.info(f"   • Old admins (group_admins): {old_admin_count}")
            logger.info(f"   • Portfolio transactions: {portfolio_count}")
            logger.info(f"   • Income records: {income_count}")
            logger.info(f"   • Expense records: {expense_count}")
            
            # Cảnh báo nếu còn dữ liệu cũ
            if old_admin_count > 0:
                logger.warning(f"⚠️ Vẫn còn {old_admin_count} admin trong bảng cũ group_admins!")
                logger.warning("   Chạy migrate_admin_data() để chuyển sang bảng mới")
            
            # Kiểm tra consistency
            if staff_count == 0 and old_admin_count > 0:
                logger.warning("⚠️ Có admin cũ nhưng chưa có staff trong permissions!")
                logger.warning("   Đang thử migrate lại...")
                migrate_admin_data()  # Thử migrate lần nữa
                
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra database: {e}")
        
        # 4. Kiểm tra cache
        logger.info("🔄 Kiểm tra cache system...")
        logger.info(f"   • Price cache: {price_cache.get_stats()}")
        logger.info(f"   • USDT cache: {usdt_cache.get_stats()}")
        logger.info(f"   • Username cache: {len(username_cache.cache)} entries")
        
        # 5. Kiểm tra thư mục
        logger.info("🔄 Kiểm tra thư mục...")
        db_size = os.path.getsize(DB_PATH) / (1024 * 1024) if os.path.exists(DB_PATH) else 0
        logger.info(f"   • Database size: {db_size:.2f} MB")
        logger.info(f"   • Backup dir: {BACKUP_DIR} ({len(os.listdir(BACKUP_DIR)) if os.path.exists(BACKUP_DIR) else 0} files)")
        logger.info(f"   • Export dir: {EXPORT_DIR} ({len(os.listdir(EXPORT_DIR)) if os.path.exists(EXPORT_DIR) else 0} files)")
        
        # 6. Kiểm tra Render Disk
        if render_config.is_render:
            logger.info("🔄 Kiểm tra Render Disk...")
            if os.path.exists('/data'):
                # Kiểm tra dung lượng
                stat = os.statvfs('/data')
                free_space = stat.f_frsize * stat.f_bavail / (1024 * 1024 * 1024)  # GB
                total_space = stat.f_frsize * stat.f_blocks / (1024 * 1024 * 1024)  # GB
                logger.info(f"   • Render Disk mounted at /data")
                logger.info(f"   • Free space: {free_space:.2f} GB / {total_space:.2f} GB")
                
                # Kiểm tra database có trong disk không
                if DB_PATH.startswith('/data'):
                    logger.info(f"   ✅ Database is on Render Disk: {DB_PATH}")
                else:
                    logger.warning(f"⚠️ Database is NOT on Render Disk: {DB_PATH}")
            else:
                logger.warning("⚠️ Render Disk not mounted at /data")
        
        # ===== KẾT THÚC PHẦN KIỂM TRA =====

        # Các phần còn lại giữ nguyên
        try:
            migrate_database()
        except Exception as e:
            logger.error(f"❌ Lỗi migrate: {e}")
        
        optimize_database()
        
        if render_config.is_render and render_config.render_url:
            logger.info("🌐 Using webhook mode")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(setup_webhook())
            
            threading.Thread(target=run_webhook_server, daemon=True).start()
        else:
            logger.info("🔄 Using polling mode")
            threading.Thread(target=run_health_server, daemon=True).start()
        
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
            app.add_handler(CommandHandler("whoami", whoami_command))
            app.add_handler(CommandHandler("permgrant", quick_grant_command))
            app.add_handler(CommandHandler("getid", getid_command))
            app.add_handler(CommandHandler("syncusers", sync_users_command))
            app.add_handler(CommandHandler("view", view_portfolio_command))
            app.add_handler(CommandHandler("users", list_users_command))
            app.add_handler(CommandHandler("syncadmins", sync_admins_command))
            app.add_handler(CommandHandler("checkperm", check_perm_command))
            app.add_handler(CommandHandler("syncdata", sync_data_command))
            app.add_handler(CommandHandler("owner", owner_panel))
            app.add_handler(CommandHandler("debugperm", debug_perm_command))
            app.add_handler(CommandHandler("setupgroup", setup_group_command))
            app.add_handler(CommandHandler("groupinfo", group_info_command))
            app.add_handler(CommandHandler("addadmin", add_group_admin))
            app.add_handler(CommandHandler("hide", hide_keyboard))
            app.add_handler(CommandHandler("balance", balance_command))
            app.add_handler(CommandHandler("canhdoi", balance_command))
            app.add_handler(CommandHandler("thuchi", balance_command))
            app.add_handler(CommandHandler("addadmin", add_admin_command))
            app.add_handler(CommandHandler("listadmin", list_admin_command))
            app.add_handler(CommandHandler("removeadmin", remove_admin_command))
            app.add_handler(CommandHandler("xoadm", delete_category_command))
            app.add_handler(CommandHandler("xoacategory", delete_category_command))
            app.add_handler(CommandHandler("xoadanhmuc", delete_category_command))
            app.add_handler(CommandHandler("xoadanhmuc", delete_category_command))
            app.add_handler(CommandHandler("delcat", delete_category_command))
            app.add_handler(CommandHandler("editthu", edit_income_command))
            app.add_handler(CommandHandler("editchi", edit_expense_command))
            app.add_handler(CommandHandler("suathu", edit_income_command))
            app.add_handler(CommandHandler("suachi", edit_expense_command))
            app.add_handler(CommandHandler("grant", grant_command))
            app.add_handler(CommandHandler("myperm", myperm_command))
            app.add_handler(CommandHandler("export", export_master_command))
            app.add_handler(CommandHandler("lang", lang_command))
            app.add_handler(CommandHandler("export_secure", export_secure_command))
            app.add_handler(CommandHandler("export_expense", export_expense_command))
            app.add_handler(CommandHandler("sells", sells_command))
            app.add_handler(CommandHandler("delsell", delete_sell_command))
            app.add_handler(CommandHandler("editsell", edit_sell_command))
            app.add_handler(CommandHandler("sells", sells_command))
            app.add_handler(CommandHandler("addsell", addsell_command))
            app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_members))
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
            app.add_handler(CallbackQueryHandler(handle_sell_confirmation, pattern="^(confirm_sell_|cancel_sell)"))
            app.add_handler(CallbackQueryHandler(handle_callback))

            # === MULTI-GROUP SYSTEM HANDLERS ===
            app.add_handler(CommandHandler("setmaster", mg_setmaster_command))
            app.add_handler(CommandHandler("masterinfo", mg_masterinfo_command))

            # ── MODERATION COMMANDS ──────────────────────────
            app.add_handler(CommandHandler("mod", mod_menu_command))
            app.add_handler(CommandHandler("ban", mod_ban_command))
            app.add_handler(CommandHandler("unban", mod_unban_command))
            app.add_handler(CommandHandler("kick", mod_kick_command))
            app.add_handler(CommandHandler("mute", mod_mute_command))
            app.add_handler(CommandHandler("unmute", mod_unmute_command))
            app.add_handler(CommandHandler("warn", mod_warn_command))
            app.add_handler(CommandHandler("unwarn", mod_unwarn_command))
            app.add_handler(CommandHandler("warns", mod_warns_command))
            app.add_handler(CommandHandler("setwarn", mod_setwarn_command))
            app.add_handler(CommandHandler("setcaptcha", mod_setcaptcha_command))
            app.add_handler(CommandHandler("setflood", mod_setflood_command))
            app.add_handler(CommandHandler("setwelcome", mod_setwelcome_command))
            app.add_handler(CommandHandler("welcomeoff", mod_welcome_off_command))
            app.add_handler(CommandHandler("setrules", mod_setrules_command))
            app.add_handler(CommandHandler("rules", mod_rules_command))
            app.add_handler(CommandHandler("filter", mod_filter_command))
            app.add_handler(CommandHandler("unfilter", mod_unfilter_command))
            app.add_handler(CommandHandler("filters", mod_filters_list_command))
            app.add_handler(CommandHandler("addcmd", mod_addcmd_command))
            app.add_handler(CommandHandler("delcmd", mod_delcmd_command))
            app.add_handler(CommandHandler("cmds", mod_cmds_list_command))
            app.add_handler(CommandHandler("purge", mod_purge_command))
            app.add_handler(CommandHandler("spurge", mod_spurge_command))
            app.add_handler(CommandHandler("adminlogs", mod_logs_command))
            app.add_handler(CommandHandler("report", mod_report_command))
            app.add_handler(CommandHandler("newfed", mod_newfed_command))
            app.add_handler(CommandHandler("joinfed", mod_joinfed_command))
            app.add_handler(CommandHandler("leavefed", mod_leavefed_command))
            app.add_handler(CommandHandler("fban", mod_fban_command))
            app.add_handler(CommandHandler("funban", mod_funban_command))
            app.add_handler(CommandHandler("fedinfo", mod_fedinfo_command))
            # Callback handler cho moderation
            app.add_handler(CallbackQueryHandler(handle_mod_callback, pattern="^mod_"))
            app.add_handler(CommandHandler("addchild", mg_addchild_command))
            app.add_handler(CommandHandler("removechild", mg_removechild_command))
            app.add_handler(CommandHandler("features", mg_features_command))
            app.add_handler(CommandHandler("crossban", mg_crossban_command))
            app.add_handler(CommandHandler("crossunban", mg_crossunban_command))
            app.add_handler(CommandHandler("banlist", mg_banlist_command))
            app.add_handler(CommandHandler("broadcast", mg_broadcast_command))
        
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

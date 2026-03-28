import requests
import gspread
import time
import hmac
import hashlib
import json
import logging
import os
import asyncio
import threading
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

# === ЗАГРУЖАЕМ ПЕРЕМЕННЫЕ ИЗ .env ===
load_dotenv()
from database import db

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET", "")

# Платёжные реквизиты
OZON_PAY_URL = os.getenv("OZON_PAY_URL", "")
BYBIT_UID = os.getenv("BYBIT_UID", "")
BSC_ADDRESS = os.getenv("BSC_ADDRESS", "")
TRC20_ADDRESS = os.getenv("TRC20_ADDRESS", "")

# Проверяем, что все переменные загружены
if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не установлен в .env файле!")
if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID не установлен в .env файле!")

print(f"✅ Переменные окружения загружены")
print(f"   ADMIN_ID: {ADMIN_ID}")

# === ЛОГИРОВАНИЕ ===
from logging.handlers import RotatingFileHandler

# Ротация логов: макс 5 МБ на файл, хранить 3 последних файла
file_handler = RotatingFileHandler(
    'bot.log', encoding='utf-8',
    maxBytes=5*1024*1024,  # 5 MB
    backupCount=3
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# Глушим HTTP-спам от httpx и telegram (там утекает токен)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice
from telegram.error import BadRequest
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, PreCheckoutQueryHandler, ContextTypes, filters
from oauth2client.service_account import ServiceAccountCredentials


# === ХРАНИЛИЩЕ ДАННЫХ (с лимитом времени жизни) ===
class TimedDict(dict):
    """Словарь, который автоматически удаляет старые записи"""
    def __init__(self, max_age_seconds=86400):  # 24 часа по умолчанию
        super().__init__()
        self.max_age = max_age_seconds
        self.timestamps = {}
    
    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.timestamps[key] = time.time()
    
    def __getitem__(self, key):
        # Проверяем, не устарела ли запись
        if key in self.timestamps:
            age = time.time() - self.timestamps[key]
            if age > self.max_age:
                self.timestamps.pop(key, None)
                super().__delitem__(key)
                raise KeyError(f"Record {key} expired")
        return super().__getitem__(key)
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    
    def cleanup(self):
        """Удаляет все устаревшие записи"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in list(self.timestamps.items())
            if current_time - timestamp > self.max_age
        ]
        for key in expired_keys:
            self.timestamps.pop(key, None)
            try:
                dict.__delitem__(self, key)
            except KeyError:
                pass


ORDER_USER_MAP = TimedDict(max_age_seconds=86400)  # 24 часа
PAYMENT_MAP = TimedDict(max_age_seconds=3600)  # 1 час
ORDER_INFO_MAP = TimedDict(max_age_seconds=604800)  # 7 дней
ORDER_LOCK = {}  # Защита от дублей: {order_number: True}
AWAITING_SCREENSHOT = TimedDict(max_age_seconds=86400)  # user_id: order_number
AWAITING_EMAIL = TimedDict(max_age_seconds=86400)  # user_id: order_number (ожидание почты Apple ID)
AWAITING_CODE = {}  # admin_id: {"order_num": ..., "client_id": ...} (ожидание ввода кода админом)

# === АНТИСПАМ ===
USER_ORDER_TIMES = {}  # user_id: [timestamp1, timestamp2, ...] — время создания заказов
ORDER_COOLDOWN = 60  # Минимум 60 секунд между заказами
MAX_ORDERS_IN_PERIOD = 3  # Максимум заказов за период
ORDER_PERIOD = 1200  # Период в секундах (20 минут)


def check_spam(user_id: int) -> tuple[bool, str]:
    """Проверка на спам. Возвращает (можно_создать, сообщение_ошибки)"""
    now = time.time()
    
    # Очищаем старые записи
    if user_id in USER_ORDER_TIMES:
        USER_ORDER_TIMES[user_id] = [t for t in USER_ORDER_TIMES[user_id] if now - t < ORDER_PERIOD]
    
    # Проверка кулдауна (60 сек между заказами)
    if user_id in USER_ORDER_TIMES and USER_ORDER_TIMES[user_id]:
        last_order = max(USER_ORDER_TIMES[user_id])
        elapsed = now - last_order
        if elapsed < ORDER_COOLDOWN:
            wait = int(ORDER_COOLDOWN - elapsed)
            return False, f"⏳ Подождите {wait} сек. перед созданием нового заказа."
    
    # Проверка лимита (макс 3 заказа за 20 минут)
    if user_id in USER_ORDER_TIMES:
        if len(USER_ORDER_TIMES[user_id]) >= MAX_ORDERS_IN_PERIOD:
            oldest = min(USER_ORDER_TIMES[user_id])
            wait_mins = int((ORDER_PERIOD - (now - oldest)) / 60) + 1
            return False, f"❌ Лимит заказов. Попробуйте через {wait_mins} мин."
    
    return True, ""


def mark_order_created(user_id: int):
    """Отмечает время создания заказа"""
    now = time.time()
    if user_id not in USER_ORDER_TIMES:
        USER_ORDER_TIMES[user_id] = []
    USER_ORDER_TIMES[user_id].append(now)


# === GOOGLE SHEETS ===
_sheet_cache = {"sheet": None, "time": 0}
_SHEET_CACHE_TTL = 300  # 5 минут

def get_sheet():
    """Получает объект таблицы с кэшированием (5 мин)"""
    now = time.time()
    if _sheet_cache["sheet"] and now - _sheet_cache["time"] < _SHEET_CACHE_TTL:
        return _sheet_cache["sheet"]
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "service_account.json",
            scope
        )
        client = gspread.authorize(creds)
        sheet = client.open("popolnyaska_bot").sheet1
        _sheet_cache["sheet"] = sheet
        _sheet_cache["time"] = now
        logger.info("✅ Подключение к Google Sheets успешно")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        _sheet_cache["sheet"] = None
        _sheet_cache["time"] = 0
        return None


def _run_stats_update():
    """Запускает обновление статистики в фоновом потоке"""
    threading.Thread(target=update_stats_sheet, daemon=True).start()

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}


def update_stats_sheet():
    """Полностью обновляет лист 'Статистика' в Google Sheets"""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open("popolnyaska_bot")

        # Получаем лист "Статистика" или создаём
        try:
            stats_ws = spreadsheet.worksheet("Статистика")
        except gspread.exceptions.WorksheetNotFound:
            stats_ws = spreadsheet.add_worksheet(title="Статистика", rows=100, cols=6)

        # Читаем все заказы с основного листа
        main_sheet = spreadsheet.sheet1
        records = main_sheet.get_all_records()

        if not records:
            stats_ws.clear()
            stats_ws.update("A1", [["Нет данных для статистики"]])
            return

        today_str = datetime.now().strftime("%d.%m.%Y")

        # === ПОДСЧЁТЫ ===
        total = len(records)
        unique_users = len(set(str(r.get("User_ID", "")) for r in records if r.get("User_ID")))

        statuses = {}
        for r in records:
            s = r.get("Статус", "—")
            statuses[s] = statuses.get(s, 0) + 1

        completed_records = [r for r in records if r.get("Статус") == "Выполнен"]
        revenue = sum(int(r.get("Сумма RUB", 0) or 0) for r in completed_records)
        avg_check = int(revenue / len(completed_records)) if completed_records else 0
        paid_count = statuses.get("Оплачен", 0) + statuses.get("Выполнен", 0)
        conversion = int(paid_count / total * 100) if total > 0 else 0

        # Сегодня
        today_records = [r for r in records if str(r.get("Дата", "")).startswith(today_str)]
        today_orders = len(today_records)
        today_completed = [r for r in today_records if r.get("Статус") == "Выполнен"]
        today_revenue = sum(int(r.get("Сумма RUB", 0) or 0) for r in today_completed)
        today_users = len(set(str(r.get("User_ID", "")) for r in today_records if r.get("User_ID")))

        # По месяцам
        months_data = {}
        for r in records:
            date_str = str(r.get("Дата", ""))
            if not date_str:
                continue
            try:
                parts = date_str.split(" ")[0].split(".")
                month_key = (int(parts[2]), int(parts[1]))  # (year, month)
            except (IndexError, ValueError):
                continue
            if month_key not in months_data:
                months_data[month_key] = {"orders": 0, "users": set(), "revenue": 0, "paid": 0}
            months_data[month_key]["orders"] += 1
            months_data[month_key]["users"].add(str(r.get("User_ID", "")))
            if r.get("Статус") in ("Оплачен", "Выполнен"):
                months_data[month_key]["paid"] += 1
            if r.get("Статус") == "Выполнен":
                months_data[month_key]["revenue"] += int(r.get("Сумма RUB", 0) or 0)

        # По регионам
        regions_data = {}
        for r in records:
            reg = r.get("Регион", "—") or "—"
            if reg not in regions_data:
                regions_data[reg] = {"orders": 0, "users": set(), "revenue": 0, "paid": 0}
            regions_data[reg]["orders"] += 1
            regions_data[reg]["users"].add(str(r.get("User_ID", "")))
            if r.get("Статус") in ("Оплачен", "Выполнен"):
                regions_data[reg]["paid"] += 1
            if r.get("Статус") == "Выполнен":
                regions_data[reg]["revenue"] += int(r.get("Сумма RUB", 0) or 0)

        # По способам оплаты
        payment_methods = {}
        for r in records:
            pm = r.get("Способ оплаты", "") or ""
            if pm:
                payment_methods[pm] = payment_methods.get(pm, 0) + 1

        # === ФОРМИРУЕМ ТАБЛИЦУ ===
        rows = []

        # ЗАКАЗЫ
        rows.append(["═══ ЗАКАЗЫ ═══", ""])
        rows.append(["Всего заказов:", total])
        rows.append(["Уникальных клиентов:", unique_users])
        rows.append(["Ожидает оплаты:", statuses.get("Ожидает оплаты", 0)])
        rows.append(["Оплачено:", statuses.get("Оплачен", 0)])
        rows.append(["Выполнено:", statuses.get("Выполнен", 0)])
        rows.append(["Отменено:", statuses.get("Отменён", 0)])
        rows.append(["", ""])

        # ФИНАНСЫ
        rows.append(["═══ ФИНАНСЫ ═══", ""])
        rows.append(["Выручка (₽):", fmt(revenue)])
        rows.append(["Средний чек (₽):", fmt(avg_check)])
        rows.append(["Конверсия (%):", conversion])
        rows.append(["", ""])

        # СЕГОДНЯ
        rows.append(["═══ СЕГОДНЯ ═══", ""])
        rows.append(["Заказов сегодня:", today_orders])
        rows.append(["Выручка сегодня (₽):", fmt(today_revenue)])
        rows.append(["Клиентов сегодня:", today_users])
        rows.append(["", ""])

        # ПО МЕСЯЦАМ
        rows.append(["═══ ПО МЕСЯЦАМ ═══", "", "", "", ""])
        rows.append(["Месяц", "Заказов", "Клиентов", "Выручка (₽)", "Конверсия (%)"])
        for key in sorted(months_data.keys()):
            year, month = key
            m = months_data[key]
            m_conv = int(m["paid"] / m["orders"] * 100) if m["orders"] > 0 else 0
            month_name = f"{MONTH_NAMES.get(month, month)} {year}"
            rows.append([month_name, m["orders"], len(m["users"]), fmt(m["revenue"]), m_conv])
        rows.append(["", ""])

        # ПО РЕГИОНАМ
        rows.append(["═══ ПО РЕГИОНАМ ═══", "", "", "", ""])
        rows.append(["Регион", "Заказов", "Клиентов", "Выручка (₽)", "Конверсия (%)"])
        for reg_code in ["US", "AE", "TR", "KZ", "SA"]:
            if reg_code in regions_data:
                rd = regions_data[reg_code]
                r_conv = int(rd["paid"] / rd["orders"] * 100) if rd["orders"] > 0 else 0
                reg_name = REGION_DISPLAY.get(reg_code, reg_code)
                rows.append([reg_name, rd["orders"], len(rd["users"]), fmt(rd["revenue"]), r_conv])
        # Другие регионы если есть
        for reg_code, rd in regions_data.items():
            if reg_code not in ["US", "AE", "TR", "KZ", "SA"]:
                r_conv = int(rd["paid"] / rd["orders"] * 100) if rd["orders"] > 0 else 0
                rows.append([reg_code, rd["orders"], len(rd["users"]), fmt(rd["revenue"]), r_conv])
        rows.append(["", ""])

        # СПОСОБЫ ОПЛАТЫ
        rows.append(["═══ СПОСОБЫ ОПЛАТЫ ═══", ""])
        for pm_name in ["ЮMoney", "OZON банк", "Crypto"]:
            rows.append([f"{pm_name}:", payment_methods.get(pm_name, 0)])
        # Другие способы если есть
        for pm_name, count in payment_methods.items():
            if pm_name not in ["ЮMoney", "OZON банк", "Crypto"]:
                rows.append([f"{pm_name}:", count])

        # === ЗАПИСЫВАЕМ ===
        stats_ws.clear()
        stats_ws.update(f"A1:E{len(rows)}", rows, value_input_option="RAW")

        logger.info("✅ Лист 'Статистика' обновлён")

    except Exception as e:
        logger.error(f"⚠️ Ошибка обновления листа Статистика: {e}")


PRICES = {
    "apple_5000": 5000,
    "apple_10000": 10000,
    "apple_15000": 15000
}

# === ТАРИФЫ ГИФТ-КАРТ ПО РЕГИОНАМ (номинал, валюта, себестоимость в USDT) ===
GIFT_CARD_TARIFFS = {
    "TR": [
        (25, "TL", 0.71),
        (50, "TL", 1.42),
        (100, "TL", 2.83),
        (250, "TL", 6.84),
        (1000, "TL", 27.36),
    ],
    "US": [
        (5, "USD", 4.85),
        (10, "USD", 9.70),
        (25, "USD", 24.25),
        (50, "USD", 48.50),
        (100, "USD", 97.00),
        (200, "USD", 194.00),
        (300, "USD", 291.00),
        (500, "USD", 495.00),
    ],
    "AE": [
        (50, "AED", 13.34),
        (100, "AED", 26.64),
        (250, "AED", 66.62),
        (500, "AED", 133.30),
        (1000, "AED", 266.36),
        (1500, "AED", 399.54),
    ],
    "SA": [
        (50, "SAR", 13.21),
        (100, "SAR", 26.07),
        (200, "SAR", 52.43),
        (300, "SAR", 78.40),
        (500, "SAR", 130.83),
        (750, "SAR", 195.00),
        (1000, "SAR", 261.42),
        (1500, "SAR", 392.00),
        (2000, "SAR", 522.83),
        (2500, "SAR", 653.42),
    ],
}

REGION_DISPLAY = {
    "KZ": "🇰🇿 Казахстан",
    "TR": "🇹🇷 Турция",
    "US": "🇺🇸 США",
    "AE": "🇦🇪 ОАЭ",
    "SA": "🇸🇦 Саудовская Аравия",
}

REGION_COMMISSION = {
    "KZ": 1.15,
    "TR": 1.10,
    "US": 1.15,
    "AE": 1.15,
    "SA": 1.15,
}

ORDER_STATUSES = {
    "pending": "Ожидает оплаты",
    "paid": "Оплачен",
    "completed": "Выполнен",
    "cancelled": "Отменён"
}

FAQ_KEYBOARD = [
    [InlineKeyboardButton("🔹 Как работает сервис?", callback_data="faq_how")],
    [InlineKeyboardButton("🔹 Сколько времени занимает?", callback_data="faq_time")],
    [InlineKeyboardButton("🔹 Способы оплаты", callback_data="faq_payment")],
    [InlineKeyboardButton("🔹 Какая комиссия?", callback_data="faq_commission")],
    [InlineKeyboardButton("🔹 Что делать при проблемах?", callback_data="faq_problems")],
    [InlineKeyboardButton("🔹 Безопасно ли это?", callback_data="faq_safety")]
]


rate_cache = {"value": None, "time": 0}


def fmt(num):
    """Форматирует число с пробелами между тысячами: 25000 → 25 000"""
    return f"{int(num):,}".replace(",", " ")


def get_rate():
    """Получение курса KZT to RUB с обработкой ошибок"""
    if time.time() - rate_cache["time"] < 3600 and rate_cache["value"] is not None:
        return rate_cache["value"]

    try:
        data = requests.get(
            "https://www.cbr-xml-daily.ru/daily_json.js",
            timeout=10
        ).json()
        value = data["Valute"]["KZT"]["Value"]
        nominal = data["Valute"]["KZT"]["Nominal"]
        rate = value / nominal

        rate_cache["value"] = rate
        rate_cache["time"] = time.time()
        logger.info(f"Курс обновлён: {rate}")
        return rate
    except Exception as e:
        logger.error(f"Ошибка получения курса: {e}")
        if rate_cache["value"] is not None:
            return rate_cache["value"]
        return 0.185


def generate_order():
    """Генерация номера ордера с обработкой ошибок"""
    try:
        max_number = 1000

        # Проверяем Google Sheets
        current_sheet = get_sheet()
        if current_sheet:
            try:
                records = current_sheet.get_all_records()
                if records:
                    last = records[-1].get("Номер ордера", "ORD-1000")
                    try:
                        max_number = max(max_number, int(last.split("-")[1]))
                    except (ValueError, IndexError):
                        pass
            except Exception as e:
                logger.warning(f"Ошибка чтения Sheets для генерации ордера: {e}")

        # Проверяем локальную БД (на случай если в БД номер больше)
        try:
            conn = sqlite3.connect("orders.db")
            c = conn.cursor()
            c.execute("SELECT order_number FROM orders ORDER BY id DESC LIMIT 1")
            row = c.fetchone()
            conn.close()
            if row:
                try:
                    db_number = int(row[0].split("-")[1])
                    max_number = max(max_number, db_number)
                except (ValueError, IndexError):
                    pass
        except Exception as e:
            logger.warning(f"Ошибка чтения БД для генерации ордера: {e}")

        return f"ORD-{max_number + 1}"
    except Exception as e:
        logger.error(f"Ошибка при генерации ордера: {e}")
        return f"ORD-{int(time.time())}"


def generate_payment_id():
    """Генерация уникального ID платежа"""
    return str(int(time.time() * 1000))


def bybit_signature(timestamp, api_key, secret, recv_window, body_str):
    """Создание подписи для Bybit API"""
    param_str = f"{timestamp}{api_key}{recv_window}{body_str}"
    return hmac.new(secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()


def create_bybit_payment(order_number, amount_usdt, service_name, tariff_name):
    """Создаёт счёт через Bybit Pay API"""
    try:
        timestamp = str(int(time.time() * 1000))
        recv_window = "20000"
        
        body = {
            "accountId": BYBIT_API_KEY,
            "amount": str(amount_usdt),
            "currency": "USDT",
            "orderNo": order_number,
            "orderDescription": f"{service_name} ({tariff_name})",
            "callbackUrl": "",  # Будет настроен при деплое на VPS
            "env": {
                "terminalType": "APP"
            }
        }
        
        body_str = json.dumps(body)
        sign = bybit_signature(timestamp, BYBIT_API_KEY, BYBIT_API_SECRET, recv_window, body_str)
        
        headers = {
            "X-BAPI-API-KEY": BYBIT_API_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": sign,
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            "https://api.bybit.com/fiat/otc/pay/order/create",
            headers=headers,
            data=body_str,
            timeout=15
        )
        
        logger.info(f"Bybit Pay ответ: HTTP {response.status_code}, Body: {response.text[:500]}")
        
        if not response.text:
            logger.error(f"❌ Bybit Pay: пустой ответ (HTTP {response.status_code})")
            return None
        
        data = response.json()
        
        if data.get("ret_code") == 0 or data.get("retCode") == 0:
            result = data.get("result", {})
            payment_url = result.get("payUrl") or result.get("pay_url", "")
            logger.info(f"✅ Bybit Pay счёт создан для {order_number}: {payment_url}")
            return payment_url
        else:
            logger.error(f"❌ Ошибка Bybit Pay: {data}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Ошибка создания Bybit Pay счёта: {e}")
        return None


def get_usdt_rate():
    """Получает курс RUB/USDT"""
    try:
        response = requests.get(
            "https://api.bybit.com/v5/market/tickers?category=spot&symbol=USDTRUB",
            timeout=10
        )
        data = response.json()
        if data.get("retCode") == 0:
            price = float(data["result"]["list"][0]["lastPrice"])
            logger.info(f"Курс USDT/RUB: {price}")
            return price
    except Exception as e:
        logger.warning(f"Ошибка получения курса USDT: {e}")
    return 95.0  # Фоллбэк


def add_order_to_sheet(order_data):
    """Добавляет заказ в таблицу и в БД"""
    try:
        # === ДОБАВЛЯЕМ В GOOGLE SHEETS ===
        current_sheet = get_sheet()
        if current_sheet:
            try:
                current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
                current_sheet.append_row([
                    order_data["number"],
                    order_data["user_id"],
                    order_data["username"],
                    order_data.get("region", "KZ"),
                    order_data["tariff"],
                    order_data["rub"],
                    "",  # Способ оплаты - заполнится позже
                    current_date,
                    ORDER_STATUSES["pending"]
                ])
                logger.info(f"Заказ {order_data['number']} добавлен в Google Sheets")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка добавления в Google Sheets: {e}")
        
        # === ДОБАВЛЯЕМ В ЛОКАЛЬНУЮ БД (ГЛАВНОЕ!) ===
        user_id = db.add_user(
            telegram_id=order_data["user_id"],
            username=order_data["username"],
            first_name=order_data.get("first_name", "Клиент")
        )
        
        if not user_id:
            logger.error("Ошибка добавления пользователя в БД")
            return False
        
        order_id = db.add_order(
            order_number=order_data["number"],
            user_id=user_id,
            service=order_data["service"],
            tariff=order_data["tariff"],
            amount_kzt=order_data["kzt"],
            amount_rub=order_data["rub"],
            payment_id=None
        )
        
        if not order_id:
            logger.error("Ошибка добавления заказа в БД")
            return False
        
        logger.info(f"✅ Заказ {order_data['number']} добавлен в БД")
        
        # Обновляем лист статистики (в фоне)
        _run_stats_update()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении заказа: {e}")
        return False

def update_payment_method(order_number, payment_method):
    """Записывает способ оплаты в Google Sheets (колонка G)"""
    try:
        current_sheet = get_sheet()
        if current_sheet:
            cell = current_sheet.find(order_number)
            if cell:
                current_sheet.update_cell(cell.row, 7, payment_method)
                logger.info(f"✅ Способ оплаты {payment_method} записан для {order_number}")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка записи способа оплаты: {e}")

def update_order_status(order_number, new_status):
    """Обновляет статус заказа в БД и Google Sheets"""
    try:
        # === ОБНОВЛЯЕМ В БД (ГЛАВНОЕ!) ===
        success = db.update_order_status(order_number, new_status)
        
        if not success:
            logger.warning(f"Заказ {order_number} не найден в БД")
            return False
        
        # === ОБНОВЛЯЕМ В GOOGLE SHEETS (для совместимости) ===
        current_sheet = get_sheet()
        if current_sheet:
            try:
                cell = current_sheet.find(order_number)
                if cell:
                    current_sheet.update_cell(cell.row, 9, new_status)
                    logger.info(f"✅ Статус {order_number} обновлён в Google Sheets")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обновления Google Sheets: {e}")
        
        logger.info(f"✅ Статус {order_number} изменён на {new_status}")
        
        # Обновляем лист статистики (в фоне)
        _run_stats_update()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении статуса: {e}")
        return False


def cleanup_memory():
    """Очищает память от устаревших данных"""
    try:
        ORDER_USER_MAP.cleanup()
        PAYMENT_MAP.cleanup()
        ORDER_INFO_MAP.cleanup()
        
        logger.info("Память очищена")
    except Exception as e:
        logger.error(f"Ошибка при очистке памяти: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню"""
    try:
        reply_keyboard = ReplyKeyboardMarkup(
            [
                [KeyboardButton("🍏 Пополнить Apple ID")],
                [KeyboardButton("📋 Заказы"), KeyboardButton("❓ FAQ")],
                [KeyboardButton("⭐ Отзывы")]
            ],
            resize_keyboard=True
        )
        await update.message.reply_text(
            "Рад видеть тебя! Я готов помочь с пополнением твоего Apple ID\n\n"
            "Что для этого нужно?\n\n"
            "1️⃣ Нажми \"🍏 Пополнить Apple ID\"\n"
            "2️⃣ Выбери регион своего Apple ID и тариф\n"
            "   (для Казахстана можно ввести свою сумму)\n"
            "3️⃣ Выбери способ оплаты и оплати\n"
            "4️⃣ Отправь данные менеджеру и жди пополнения\n\n"
            "Всё проще, чем кажется! 😉",
            reply_markup=reply_keyboard
        )
        logger.info(f"Пользователь {update.message.from_user.id} запустил бот")
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")
        await update.message.reply_text("❌ Ошибка. Попробуйте позже.")


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ-панель"""
    try:
        if update.message.from_user.id != ADMIN_ID:
            await update.message.reply_text("❌ У вас нет доступа.")
            logger.warning(f"Попытка доступа к админ-панели от {update.message.from_user.id}")
            return
        
        keyboard = [
            [InlineKeyboardButton("📊 Общая статистика", callback_data="stats_general")],
            [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
            [InlineKeyboardButton("🔄 Изменить статус заказа", callback_data="admin_manage_orders")]
        ]
        await update.message.reply_text(
            "⚙️ Админ панель",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info(f"Админ {ADMIN_ID} открыл админ-панель")
    except Exception as e:
        logger.error(f"Ошибка в admin: {e}")


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех кнопок"""
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Ошибка при ответе на callback: {e}")
        return

    try:
        # === МОИ ЗАКАЗЫ ===
        if query.data == "my_orders":
            user_id = query.from_user.id
            
            try:
                current_sheet = get_sheet()
                if not current_sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                records = current_sheet.get_all_records()
            except Exception as e:
                logger.error(f"Ошибка получения записей из таблицы: {e}")
                await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                return
            
            # Ищем по ID (второй столбец)
            user_records = [r for r in records if str(r.get("User_ID", "")) == str(user_id)]
            
            if not user_records:
                await query.edit_message_text(
                    "📋 У вас пока нет заказов.\n\n"
                    "Нажми «🍏 Пополнить Apple ID» чтобы создать заказ."
                )
                logger.info(f"Пользователь {user_id} проверил заказы - нет заказов")
                return
            
            msg = "📋 Ваши заказы:\n\n"
            
            for record in user_records:
                order_num = record.get("Номер ордера", "N/A")
                tariff = record.get("Тариф", "N/A")
                rub_amt = record.get("Сумма RUB", "N/A")
                status = record.get("Статус", "Новый")
                region = record.get("Регион", "")
                region_display = REGION_DISPLAY.get(region, region) if region else "—"
                
                msg += (
                    f"🔹 {order_num}\n"
                    f"   Регион: {region_display}\n"
                    f"   Тариф: {tariff}\n"
                    f"   Сумма: {rub_amt} ₽\n"
                    f"   Статус: {status}\n\n"
                )
            
            await query.edit_message_text(msg)
            logger.info(f"Пользователь {user_id} просмотрел {len(user_records)} заказов")
            return
        
        # === НАЗАД В ГЛАВНОЕ МЕНЮ ===
        if query.data == "back_to_start" or query.data == "new_order":
            keyboard = [
                [InlineKeyboardButton("🍏 Пополнить Apple ID", callback_data="apple_topup")],
                [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
                [InlineKeyboardButton("❓ FAQ", callback_data="faq_menu")],
                [InlineKeyboardButton("⭐ Отзывы", callback_data="reviews")]
            ]
            await query.edit_message_text(
                "🍏 Главное меню\n\n"
                "Выбери действие или используй кнопки внизу:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === ОТЗЫВЫ ===
        if query.data == "reviews":
            keyboard = [
                [InlineKeyboardButton("📢 Отзывы в канале", url="https://t.me/popolnyaskaservice")],
                [InlineKeyboardButton("✍️ Оставить отзыв", url="https://t.me/poplnyaska_halper")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]
            ]
            await query.edit_message_text(
                "⭐ <b>Отзывы наших клиентов</b>\n\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Пополнил Apple ID за 15 минут, всё чётко! Рекомендую\"</i>\n"
                "— Клиент из Казахстана\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Заказал Gift Card США, код пришёл быстро. Сервис огонь 🔥\"</i>\n"
                "— Клиент из России\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Второй раз пользуюсь, всё работает без проблем\"</i>\n"
                "— Постоянный клиент\n\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "📢 Больше отзывов — в нашем канале\n"
                "✍️ Хотите оставить отзыв? Напишите менеджеру!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            return

        # === FAQ МЕНЮ ===
        if query.data == "faq_menu":
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\n"
                "Выберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        # === FAQ HANDLERS ===
        if query.data == "faq_how":
            await query.edit_message_text(
                "🔹 Как работает сервис?\n\n"
                "Мы помогаем пополнить Apple ID граждан РФ методом смены региона.\n\n"
                "🇰🇿 <b>Казахстан</b> — пополнение напрямую на ваш Apple ID. "
                "Вы отправляете почту, привязанную к Apple ID, а мы отправляем "
                "подарочный код для пополнения Apple ID.\n\n"
                "<b>🇺🇸 США, 🇦🇪 ОАЭ, 🇹🇷 Турция, 🇸🇦 Саудовская Аравия</b> — мы отправляем "
                "Gift Card (код) нужного номинала вам для активации через бот.\n\n"
                "📱 Как сменить регион Apple ID:\n"
                "1️⃣ Откройте «Настройки» → ваше имя → «Контент и покупки»\n"
                "2️⃣ Нажмите «Просмотреть» → «Страна/регион»\n"
                "3️⃣ Выберите нужную страну\n"
                "4️⃣ Примите условия и подтвердите\n"
                "5️⃣ Введите любой адрес выбранной страны (можно найти в интернете)\n\n"
                "Готово! Теперь вы можете пополнить Apple ID на любую сумму "
                "и пользоваться доступными предложениями.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]]),
                parse_mode="HTML"
            )
            return

        if query.data == "faq_time":
            await query.edit_message_text(
                "🔹 Сколько времени занимает?\n\n"
                "🇰🇿 Пополнение Apple ID (Казахстан) — 15-30 минут после подтверждения оплаты.\n\n"
                "🎁 Gift Card (США, ОАЭ, Турция, СА) — до 15 минут. "
                "Бот отправит вам код для пополнения Apple ID.\n\n"
                "В редких случаях может занять больше времени из-за высокой нагрузки.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_payment":
            await query.edit_message_text(
                "🔹 Какие способы оплаты доступны?\n\n"
                "• ЮMoney (пополнение кошелька)\n"
                "• OZON банк (перевод по ссылке)\n"
                "• Криптовалюта:\n"
                "  — Bybit (перевод по UID)\n"
                "  — Bybit (адрес, USDT BSC/BEP20)\n"
                "  — Телеграм кошелёк (USDT TRC20)\n\n"
                "⚠️ Для заказов свыше 8 500 ₽ доступна только оплата криптой.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_commission":
            await query.edit_message_text(
                "🔹 Какая комиссия сервиса?\n\n"
                "Комиссия зависит от региона:\n\n"
                "🇰🇿 Казахстан — 15%\n"
                "🇺🇸 США — 15%\n"
                "🇦🇪 ОАЭ — 15%\n"
                "🇸🇦 Саудовская Аравия — 15%\n"
                "🇹🇷 Турция — 10%",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_problems":
            await query.edit_message_text(
                "🔹 Что делать, если возникли проблемы?\n\n"
                "Напишите нам — кнопка «Написать менеджеру» доступна в заказе, "
                "или свяжитесь напрямую с поддержкой.\n\n"
                "📧 Для жалоб, предложений, сотрудничества и запросов в службу поддержки:\n"
                "<code>popolnyaskaservice@icloud.com</code>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📞 Написать в поддержку", url="https://t.me/poplnyaska_halper")],
                    [InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]
                ]),
                parse_mode="HTML"
            )
            return

        if query.data == "faq_safety":
            await query.edit_message_text(
                "🔹 Безопасно ли это?\n\n"
                "Да! Это абсолютно безопасно. Мы используем минимум личной информации "
                "(UserID, username и информацию о заказе) для статистики, а вашу Apple почту "
                "используем непосредственно для пополнения Apple ID напрямую и не сохраняем!\n\n"
                "Ваши данные в безопасности!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "back_to_faq":
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\n"
                "Выберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        # === ПОПОЛНЕНИЕ APPLE ID — ВЫБОР РЕГИОНА ===
        if query.data == "apple_topup":
            keyboard = [
                [InlineKeyboardButton("🇺🇸 США", callback_data="region_US")],
                [InlineKeyboardButton("🇦🇪 ОАЭ", callback_data="region_AE")],
                [InlineKeyboardButton("🇹🇷 Турция", callback_data="region_TR")],
                [InlineKeyboardButton("🇰🇿 Казахстан", callback_data="region_KZ")],
                [InlineKeyboardButton("🇸🇦 Саудовская Аравия", callback_data="region_SA")]
            ]
            await query.edit_message_text(
                "🍏 Пополнение Apple ID\n\n"
                "Выбери регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === РЕГИОН КАЗАХСТАН — ВЫБОР ТАРИФА ===
        elif query.data == "region_KZ":
            keyboard = [
                [InlineKeyboardButton("🍏 5 000 KZT", callback_data="apple_5000")],
                [InlineKeyboardButton("🍏 10 000 KZT", callback_data="apple_10000")],
                [InlineKeyboardButton("🍏 15 000 KZT", callback_data="apple_15000")],
                [InlineKeyboardButton("✏️ Ввести свою сумму", callback_data="apple_custom")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="apple_topup")]
            ]
            await query.edit_message_text(
                "🇰🇿 Казахстан — Пополнение Apple ID\n\n"
                "Выбери сумму пополнения:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === РЕГИОНЫ С ГИФТ-КАРТАМИ — ВЫБОР ТАРИФА ===
        elif query.data in ("region_TR", "region_AE", "region_SA", "region_US"):
            region_code = query.data.replace("region_", "")
            tariffs = GIFT_CARD_TARIFFS[region_code]
            region_name = REGION_DISPLAY[region_code]

            keyboard = []
            for amount, currency, usdt_cost in tariffs:
                keyboard.append([InlineKeyboardButton(
                    f"🍏 {fmt(amount)} {currency}",
                    callback_data=f"gc_{region_code}_{amount}"
                )])
            keyboard.append([InlineKeyboardButton("⬅️ Назад к регионам", callback_data="apple_topup")])

            await query.edit_message_text(
                f"{region_name} — Gift Card Apple\n\n"
                "Выбери номинал гифт-карты:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === ВЫБОР ТАРИФА ГИФТ-КАРТЫ ===
        elif query.data.startswith("gc_"):
            parts = query.data.split("_")
            region_code = parts[1]
            amount = int(parts[2])

            tariffs = GIFT_CARD_TARIFFS.get(region_code, [])
            tariff_info = None
            for t_amount, t_currency, t_usdt in tariffs:
                if t_amount == amount:
                    tariff_info = (t_amount, t_currency, t_usdt)
                    break

            if not tariff_info:
                await query.edit_message_text("❗ Тариф не найден.")
                return

            t_amount, t_currency, t_usdt = tariff_info
            user = query.from_user

            can_create, spam_msg = check_spam(user.id)
            if not can_create:
                await query.answer(spam_msg, show_alert=True)
                return

            usdt_rate = get_usdt_rate()
            commission = REGION_COMMISSION.get(region_code, 1.15)
            commission_pct = round((commission - 1) * 100)
            rub = int(t_usdt * usdt_rate * commission)
            order_number = generate_order()

            if not order_number:
                await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                return

            region_name = REGION_DISPLAY[region_code]
            tariff_name = f"{fmt(t_amount)} {t_currency}"

            context.user_data["order"] = {
                "number": order_number,
                "service": f"Gift Card ({region_name})",
                "tariff": tariff_name,
                "kzt": 0,
                "rub": rub,
                "region": region_code,
                "user": user
            }

            keyboard = [
                [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"region_{region_code}")]
            ]

            await query.edit_message_text(
                f"📦 Информация о заказе\n\n"
                f"Номер заказа: <b>{order_number}</b>\n"
                f"Регион: <b>{region_name}</b>\n"
                f"Тариф: <b>{tariff_name} Gift Card</b>\n"
                f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (комиссия {commission_pct}%)",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            logger.info(f"Пользователь {user.id} создал заказ {order_number} (Gift Card {region_code} {tariff_name})")
            return

        elif query.data == "apple_custom":
            context.user_data["awaiting_apple"] = True
            await query.edit_message_text(
                "Введите сумму пополнения Apple ID (2 000–45 000 KZT)"
            )

        # === ПОСЛЕ ВЫБОРА СЕРВИСА — ПОКАЗЫВАЕМ ЗАЯВКУ ===
        # === ВЫБОР ТАРИФА APPLE ID ===
        elif query.data.startswith("apple_") and query.data in PRICES:
            user = query.from_user
            
            # Проверка антиспам
            can_create, spam_msg = check_spam(user.id)
            if not can_create:
                await query.answer(spam_msg, show_alert=True)
                return
            
            amount = PRICES[query.data]
            rate = get_rate()
            
            if not rate:
                await query.edit_message_text("❌ Ошибка получения курса. Попробуйте позже.")
                return
            
            rub = int(amount * rate * 1.15)
            order_number = generate_order()
            
            if not order_number:
                await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                return
            
            tariff_name = f"{fmt(amount)} KZT"

            context.user_data["order"] = {
                "number": order_number,
                "service": "Apple ID",
                "tariff": tariff_name,
                "kzt": amount,
                "rub": rub,
                "region": "KZ",
                "user": user
            }

            keyboard = [
                [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data="apple_topup")]
            ]

            await query.edit_message_text(
                f"📦 Информация о заказе\n\n"
                f"Номер заказа: <b>{order_number}</b>\n"
                f"Тариф: <b>{tariff_name}</b>\n"
                f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (комиссия 15%)",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            logger.info(f"Пользователь {user.id} создал заказ {order_number}")

        # === НАЖАЛ "ПРОДОЛЖИТЬ" — ЗАЩИТА ОТ ДУБЛЕЙ ===
        elif query.data.startswith("confirm_"):
            order = context.user_data.get("order")
            if not order:
                await query.edit_message_text("⚠️ Заказ не найден. Попробуйте снова.")
                return

            order_number = order["number"]
            user_id = order["user"].id
            
            # Проверка антиспам перед подтверждением
            can_create, spam_msg = check_spam(user_id)
            if not can_create:
                await query.answer(spam_msg, show_alert=True)
                return
            
            # Защита от дублей
            if order_number in ORDER_LOCK:
                await query.edit_message_text("⏳ Заказ уже обрабатывается. Подождите...")
                return
            
            ORDER_LOCK[order_number] = True
            
            try:
                ORDER_USER_MAP[order_number] = user_id

                # === ДОБАВЛЯЕМ В ТАБЛИЦУ ===
                order_data = {
                    "number": order_number,
                    "user_id": user_id,
                    "username": order["user"].username or "Нет ника",
                    "first_name": order["user"].first_name or "Клиент",
                    "service": order["service"],
                    "tariff": order["tariff"],
                    "kzt": order["kzt"],
                    "rub": order["rub"],
                    "region": order.get("region", "KZ")
                }
                
                if not add_order_to_sheet(order_data):
                    await query.edit_message_text("❌ Ошибка сохранения заказа. Попробуйте позже.")
                    del ORDER_LOCK[order_number]
                    return
                
                # Отмечаем создание заказа для антиспама
                mark_order_created(user_id)

                # === СОХРАНЯЕМ ИНФОРМАЦИЮ О ЗАКАЗЕ ===
                ORDER_INFO_MAP[order_number] = {
                    "user_id": user_id,
                    "username": order["user"].username or "Нет ника",
                    "first_name": order["user"].first_name or "Клиент",
                    "service": order["service"],
                    "tariff": order["tariff"],
                    "kzt": order["kzt"],
                    "rub": order["rub"],
                    "region": order.get("region", "KZ")
                }

                payment_id = generate_payment_id()

                PAYMENT_MAP[payment_id] = {
                    "order_number": order_number,
                    "user_id": user_id,
                    "sum_rub": order["rub"],
                    "service": order["service"],
                    "tariff": order["tariff"]
                }

                context.user_data["current_order_number"] = order_number

                # === ВЫБОР СПОСОБА ОПЛАТЫ ===
                usdt_rate = get_usdt_rate()
                amount_usdt = round(order["rub"] / usdt_rate, 2)
                context.user_data["amount_usdt"] = amount_usdt

                if order["rub"] > 8500:
                    pay_buttons = [
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]
                else:
                    pay_buttons = [
                        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]

                await query.edit_message_text(
                    f"✅ Заявка сформирована!\n\n"
                    f"Номер заказа: <b>{order['number']}</b>\n"
                    f"Тариф: <b>{order['tariff']}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b> (~{amount_usdt} USDT)\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup(pay_buttons),
                    parse_mode="HTML"
                )
                logger.info(f"Заказ {order_number} — выбор способа оплаты")

            finally:
                # Удаляем блокировку через 5 секунд
                await asyncio.sleep(5)
                if order_number in ORDER_LOCK:
                    del ORDER_LOCK[order_number]

        # === ОПЛАТА ЧЕРЕЗ ЮMONEY ===
        elif query.data.startswith("pay_yoomoney_"):
            order_number = query.data.replace("pay_yoomoney_", "")
            order = context.user_data.get("order")
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return

            amount_rub = order["rub"]

            await query.edit_message_text(
                f"💳 Оплата через ЮMoney\n\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"💰 К оплате: <b>{fmt(amount_rub)} ₽</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📲 <b>Реквизиты для перевода:</b>\n"
                f"Кошелёк ЮMoney: <code>{YOOMONEY_WALLET}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>Как оплатить:</b>\n"
                f"1. Откройте приложение ЮMoney\n"
                f"2. Переведите <b>точную сумму</b> на кошелёк выше\n"
                f"3. Сделайте скриншот подтверждения\n"
                f"4. Нажмите «✅ Я оплатил» и отправьте скриншот\n\n"
                f"⚠️ Переводите <b>точную сумму</b> — {fmt(amount_rub)} ₽",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_yoomoney_{order_number}")],
                    [InlineKeyboardButton("⬅️ Назад к способам оплаты", callback_data="back_to_payment")]
                ]),
                parse_mode="HTML"
            )
            update_payment_method(order_number, "ЮMoney")
            logger.info(f"Клиент {query.from_user.id} выбрал ЮMoney для {order_number}")

        # === ОПЛАТА ЧЕРЕЗ OZON БАНК ===
        elif query.data.startswith("pay_ozon_"):
            order_number = query.data.replace("pay_ozon_", "")
            order = context.user_data.get("order")
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return

            amount_rub = order["rub"]

            await query.edit_message_text(
                f"💳 Оплата через OZON банк\n\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"💰 К оплате: <b>{fmt(amount_rub)} ₽</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📲 <b>Как оплатить:</b>\n"
                f"1. Нажмите кнопку <b>«Перейти к оплате»</b>\n"
                f"2. Переведите <b>{fmt(amount_rub)} ₽</b>\n"
                f"3. Сделайте скриншот подтверждения\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"💡 <b>Комиссия:</b> с OZON без комиссии / с других банков 1.9%\n\n"
                f"После перевода нажмите «✅ Я оплатил» и отправьте подтверждение.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 Перейти к оплате", url=OZON_PAY_URL)],
                    [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_ozon_{order_number}")],
                    [InlineKeyboardButton("⬅️ Назад к способам оплаты", callback_data="back_to_payment")]
                ]),
                parse_mode="HTML"
            )
            update_payment_method(order_number, "OZON банк")
            logger.info(f"Клиент {query.from_user.id} выбрал OZON банк для {order_number}")

        # === ОПЛАТА КРИПТОЙ ===
        elif query.data.startswith("pay_crypto_"):
            order_number = query.data.replace("pay_crypto_", "")
            order = context.user_data.get("order")
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return

            amount_usdt = context.user_data.get("amount_usdt", 0)
            
            await query.edit_message_text(
                f"💎 Оплата криптой (USDT)\n\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"💰 К оплате: <b>{amount_usdt} USDT</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📲 <b>Способ 1: Bybit (перевод по UID)</b>\n"
                f"UID: <code>{BYBIT_UID}</code>\n"
                f"Сумма: <b>{amount_usdt} USDT</b>\n\n"
                f"📲 <b>Способ 2: Bybit (адрес)</b>\n"
                f"Адрес: <code>{BSC_ADDRESS}</code>\n"
                f"Сеть: <b>BSC (BEP20)</b> | Монета: <b>USDT</b>\n\n"
                f"📲 <b>Способ 3: Телеграм кошелёк</b>\n"
                f"Адрес: <code>{TRC20_ADDRESS}</code>\n"
                f"Сеть: <b>Tron (TRC20)</b> | Монета: <b>USDT</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"После перевода нажмите «✅ Я оплатил» и отправьте скриншот подтверждения.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_crypto_{order_number}")],
                    [InlineKeyboardButton("⬅️ Назад к способам оплаты", callback_data="back_to_payment")]
                ]),
                parse_mode="HTML"
            )
            update_payment_method(order_number, "Crypto")
            logger.info(f"Клиент {query.from_user.id} выбрал крипто-оплату для {order_number}")

        # === ПОДТВЕРЖДЕНИЕ КРИПТО-ОПЛАТЫ ===
        elif query.data.startswith("paid_crypto_"):
            order_number = query.data.replace("paid_crypto_", "")
            user_id = query.from_user.id
            order = context.user_data.get("order")
            amount_usdt = context.user_data.get("amount_usdt", 0)

            # Включаем режим ожидания скриншота
            AWAITING_SCREENSHOT[user_id] = order_number

            order_info = ORDER_INFO_MAP.get(order_number, {})
            region_display = REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', '—'))

            await query.edit_message_text(
                f"📸 <b>Отправьте скриншот оплаты</b>\n\n"
                f"Для завершения оформления заказа отправьте скриншот "
                f"подтверждения оплаты прямо в этот чат.\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"🌍 Регион: <b>{region_display}</b>\n"
                f"📱 Тариф: <b>{order_info.get('tariff', '—')}</b>\n"
                f"💰 Сумма: <b>{amount_usdt} USDT</b>\n"
                f"💳 Оплата: <b>Crypto</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"⏳ После получения скриншота заявка будет отправлена менеджеру на проверку.",
                parse_mode="HTML"
            )
            logger.info(f"Клиент {user_id} нажал 'Я оплатил' (крипто) для {order_number}")

        # === ПОДТВЕРЖДЕНИЕ ЮMONEY-ОПЛАТЫ ===
        elif query.data.startswith("paid_yoomoney_"):
            order_number = query.data.replace("paid_yoomoney_", "")
            user_id = query.from_user.id
            order = context.user_data.get("order")
            amount_rub = order["rub"] if order else 0

            # Включаем режим ожидания скриншота
            AWAITING_SCREENSHOT[user_id] = order_number

            order_info = ORDER_INFO_MAP.get(order_number, {})
            region_display = REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', '—'))

            await query.edit_message_text(
                f"📸 <b>Отправьте скриншот оплаты</b>\n\n"
                f"Для завершения оформления заказа отправьте скриншот "
                f"подтверждения оплаты прямо в этот чат.\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"🌍 Регион: <b>{region_display}</b>\n"
                f"📱 Тариф: <b>{order_info.get('tariff', '—')}</b>\n"
                f"💰 Сумма: <b>{fmt(amount_rub)} ₽</b>\n"
                f"💳 Оплата: <b>ЮMoney</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"⏳ После получения скриншота заявка будет отправлена менеджеру на проверку.",
                parse_mode="HTML"
            )
            logger.info(f"Клиент {user_id} нажал 'Я оплатил' (ЮMoney) для {order_number}")

        # === ПОДТВЕРЖДЕНИЕ OZON-ОПЛАТЫ ===
        elif query.data.startswith("paid_ozon_"):
            order_number = query.data.replace("paid_ozon_", "")
            user_id = query.from_user.id
            order = context.user_data.get("order")
            amount_rub = order["rub"] if order else 0

            # Включаем режим ожидания скриншота
            AWAITING_SCREENSHOT[user_id] = order_number

            order_info = ORDER_INFO_MAP.get(order_number, {})
            region_display = REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', '—'))

            await query.edit_message_text(
                f"📸 <b>Отправьте скриншот оплаты</b>\n\n"
                f"Для завершения оформления заказа отправьте скриншот "
                f"подтверждения оплаты прямо в этот чат.\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"🌍 Регион: <b>{region_display}</b>\n"
                f"📱 Тариф: <b>{order_info.get('tariff', '—')}</b>\n"
                f"💰 Сумма: <b>{fmt(amount_rub)} ₽</b>\n"
                f"💳 Оплата: <b>OZON банк</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"⏳ После получения скриншота заявка будет отправлена менеджеру на проверку.",
                parse_mode="HTML"
            )
            logger.info(f"Клиент {user_id} нажал 'Я оплатил' (OZON) для {order_number}")

        # === ПОМОЩЬ ===
        elif query.data == "help_payment":
            keyboard = [
                [InlineKeyboardButton("📞 Написать в поддержку", url="https://t.me/poplnyaska_halper")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_payment")]
            ]
            await query.edit_message_text(
                "❓ <b>Краткий FAQ</b>\n\n"
                "💳 <b>Способы оплаты:</b>\n"
                "• ЮMoney (пополнение кошелька)\n"
                "• OZON банк (перевод по ссылке)\n"
                "• Криптовалюта:\n"
                "  — Bybit (перевод по UID)\n"
                "  — Bybit (адрес, USDT BSC/BEP20)\n"
                "  — Телеграм кошелёк (USDT TRC20)\n\n"
                "⚠️ Для заказов свыше 8 500 ₽ доступна только оплата криптой.\n\n"
                "⏱ <b>Сроки:</b>\n"
                "🇰🇿 Казахстан — 15-30 минут | 🎁 Gift Card — до 15 минут\n\n"
                "💰 <b>Комиссия:</b> 15% (🇹🇷 Турция — 10%)\n\n"
                "❓ <b>Проблемы с оплатой?</b>\n"
                "Свяжитесь с поддержкой через кнопку ниже.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )

        # === ВЕРНУТЬСЯ К ОПЛАТЕ ===
        elif query.data == "back_to_payment":
            order_number = context.user_data.get("current_order_number")
            
            if not order_number:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Начните снова.")
                return

            order = context.user_data.get("order")
            amount_usdt = context.user_data.get("amount_usdt", 0)
            if order:
                if order['rub'] > 8500:
                    pay_buttons = [
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]
                else:
                    pay_buttons = [
                        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]

                await query.edit_message_text(
                    f"✅ Заявка сформирована!\n\n"
                    f"Номер заказа: <b>{order_number}</b>\n"
                    f"Тариф: <b>{order['tariff']}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b> (~{amount_usdt} USDT)\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup(pay_buttons),
                    parse_mode="HTML"
                )
            else:
                await query.edit_message_text(
                    f"Номер заказа: <b>{order_number}</b>\n\nДля повторной оплаты создайте новый заказ.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]]),
                    parse_mode="HTML"
                )

         # === КОНТАКТ С МЕНЕДЖЕРОМ ===
        elif query.data == "contact_manager":
            user_id = query.from_user.id
            username = query.from_user.username or query.from_user.first_name or "Клиент"
            first_name = query.from_user.first_name or "Клиент"

            await query.edit_message_text(
                "📞 Запрос отправлен!\n\n"
                "Менеджер напишет вам в личку в течение 5 минут.\n"
                "Оставайтесь в Telegram.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_payment")]
                ])
            )

            username_link = f'<a href="https://t.me/{username}">@{username}</a>' if query.from_user.username else first_name
            client_info = f'Имя: <a href="tg://user?id={user_id}">{first_name}</a>\n' \
                         f'Ник: {username_link}\n' \
                         f'ID: <code>{user_id}</code>'

            # Ищем заказы клиента: сначала в памяти, потом в Google Sheets
            client_orders = [order_num for order_num, info in ORDER_INFO_MAP.items() if info["user_id"] == user_id]
            
            if client_orders:
                latest_order = client_orders[-1]
                order_info = ORDER_INFO_MAP[latest_order]
                region_code = order_info.get('region', 'KZ')
                region_display = REGION_DISPLAY.get(region_code, region_code)
                client_info += f"\n\n<b>📦 Последний заказ:</b>\n" \
                              f"Номер: <b>{latest_order}</b>\n" \
                              f"Регион: <b>{region_display}</b>\n" \
                              f"Тариф: <b>{order_info['tariff']}</b>\n" \
                              f"Сумма: <b>{order_info['rub']} ₽</b>"
            else:
                _cm_sheet = get_sheet()
                if _cm_sheet:
                    try:
                        records = _cm_sheet.get_all_records()
                        user_records = [r for r in records if str(r.get("User_ID", "")) == str(user_id)]
                        if user_records:
                            last = user_records[-1]
                            last_region = last.get('Регион', '')
                            last_region_display = REGION_DISPLAY.get(last_region, last_region) if last_region else '—'
                            client_info += f"\n\n<b>📦 Последний заказ:</b>\n" \
                                          f"Номер: <b>{last.get('Номер ордера', 'N/A')}</b>\n" \
                                          f"Регион: <b>{last_region_display}</b>\n" \
                                          f"Тариф: <b>{last.get('Тариф', 'N/A')}</b>\n" \
                                          f"Сумма: <b>{last.get('Сумма RUB', 'N/A')} ₽</b>\n" \
                                          f"Статус: <b>{last.get('Статус', 'N/A')}</b>"
                        else:
                            client_info += "\n\n📦 Заказов не найдено"
                    except Exception as e:
                        logger.warning(f"Ошибка получения заказов из Sheets для contact_manager: {e}")
                        client_info += "\n\n📦 Не удалось загрузить заказы"
                else:
                    client_info += "\n\n📦 Не удалось загрузить заказы"

            try:
                await context.bot.send_message(
                    ADMIN_ID,
                    f"📞 Клиент запросил связь с менеджером\n\n"
                    f"<b>👤 Информация о клиенте:</b>\n"
                    f"{client_info}",
                    parse_mode="HTML"
                )
                logger.info(f"Клиент {user_id} запросил связь с менеджером")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления о запросе связи: {e}")

        # === КНОПКА ДЛЯ АДМИНА ===
        elif query.data.startswith("open_client_dm_"):
            parts = query.data.split("_")
            client_id = int(parts[3])
            reason = parts[4] if len(parts) > 4 else "order"

            client_name = "Клиент"
            client_region = None
            client_tariff = None

            for order_num, info in ORDER_INFO_MAP.items():
                if info["user_id"] == client_id:
                    client_name = info["first_name"]
                    client_region = REGION_DISPLAY.get(info.get("region", ""), info.get("region", ""))
                    client_tariff = info["tariff"]
                    break

            if reason == "support":
                msg = (
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"Клиент запросил связь с менеджером.\n"
                    f"Если у него есть заказ:\n"
                    f"  • Регион: {client_region or 'уточнить'}\n"
                    f"  • Тариф: {client_tariff or 'уточнить'}\n\n"
                    f"Напишите ему в личку и помогите!"
                )
            else:
                msg = (
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"<b>📦 Заказ:</b>\n"
                    f"  • Номер: {reason}\n"
                    f"  • Регион: {client_region}\n"
                    f"  • Тариф: {client_tariff}\n\n"
                    f"Напишите ему об оплате или доступе."
                )

            await query.edit_message_text(msg, parse_mode="HTML")

        # === АДМИН-ПАНЕЛЬ: ЗАКАЗЫ ===
        elif query.data == "admin_orders":
            if query.from_user.id != ADMIN_ID:
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            try:
                current_sheet = get_sheet()
                if not current_sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                values = current_sheet.get_all_values()
            except Exception as e:
                logger.error(f"Ошибка получения заказов: {e}")
                await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                return
            
            if len(values) <= 1:
                await query.edit_message_text(
                    "📦 Нет заказов пока.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
                )
                return

            msg = "📦 Последние заказы (последние 10):\n\n"
            for row in reversed(values[1:][-10:]):
                if len(row) >= 9:
                    order_num = row[0]
                    user_id = row[1]
                    region = row[3]
                    tariff = row[4]
                    rub_amt = row[5]
                    status = row[8]
                    region_display = REGION_DISPLAY.get(region, region)
                    
                    msg += f"🔹 <b>{order_num}</b>\n"
                    msg += f"   Статус: {status}\n"
                    msg += f"   Регион: {region_display}\n"
                    msg += f"   Тариф: {tariff}\n"
                    msg += f"   Сумма: {rub_amt} ₽\n"
                    msg += f"   ID: <code>{user_id}</code>\n\n"
            
            keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

        # === ОБЩАЯ СТАТИСТИКА ===
        elif query.data == "stats_general":
            if query.from_user.id != ADMIN_ID:
                return
            try:
                current_sheet = get_sheet()
                if not current_sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                records = current_sheet.get_all_records()
                
                total_orders = len(records)
                unique_users = len(set(str(r.get("User_ID", "")) for r in records if r.get("User_ID")))
                
                # По статусам
                statuses = {}
                for r in records:
                    status = r.get("Статус", "Неизвестен")
                    statuses[status] = statuses.get(status, 0) + 1
                
                # По регионам
                regions = {}
                for r in records:
                    reg = r.get("Регион", "—")
                    if not reg:
                        reg = "—"
                    regions[reg] = regions.get(reg, 0) + 1
                
                # Выручка (только выполненные)
                revenue = sum(int(r.get("Сумма RUB", 0) or 0) for r in records if r.get("Статус") == "Выполнен")
                
                # Выручка по регионам (выполненные)
                region_revenue = {}
                for r in records:
                    if r.get("Статус") == "Выполнен":
                        reg = r.get("Регион", "—") or "—"
                        rub_val = int(r.get("Сумма RUB", 0) or 0)
                        region_revenue[reg] = region_revenue.get(reg, 0) + rub_val
                
                # Средний чек
                completed = [r for r in records if r.get("Статус") == "Выполнен"]
                avg_check = int(revenue / len(completed)) if completed else 0
                
                # Конверсия
                paid_count = statuses.get("Оплачен", 0) + statuses.get("Выполнен", 0)  
                conversion = int(paid_count / total_orders * 100) if total_orders > 0 else 0
                
                msg = (
                    "📊 <b>ОБЩАЯ СТАТИСТИКА</b>\n\n"
                    f"👥 Уникальных клиентов: <b>{unique_users}</b>\n"
                    f"📦 Всего заказов: <b>{total_orders}</b>\n\n"
                    f"<b>📈 ПО СТАТУСАМ:</b>\n"
                )
                for status, count in statuses.items():
                    msg += f"• {status}: <b>{count}</b>\n"
                
                msg += f"\n<b>🌍 ПО РЕГИОНАМ:</b>\n"
                for reg, count in regions.items():
                    reg_name = REGION_DISPLAY.get(reg, reg)
                    msg += f"• {reg_name}: <b>{count}</b>\n"
                
                msg += (
                    f"\n<b>💰 ФИНАНСЫ:</b>\n"
                    f"• Выручка: <b>{fmt(revenue)} ₽</b>\n"
                    f"• Средний чек: <b>{fmt(avg_check)} ₽</b>\n"
                    f"• Конверсия: <b>{conversion}%</b>\n"
                )
                
                if region_revenue:
                    msg += f"\n<b>💰 ВЫРУЧКА ПО РЕГИОНАМ:</b>\n"
                    for reg, rev in region_revenue.items():
                        reg_name = REGION_DISPLAY.get(reg, reg)
                        msg += f"• {reg_name}: <b>{fmt(rev)} ₽</b>\n"
                
                await query.edit_message_text(
                    msg,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка получения статистики: {e}")
                await query.edit_message_text("⚠️ Ошибка получения статистики.")

        # === АДМИН-ПАНЕЛЬ: УПРАВЛЕНИЕ СТАТУСАМИ ===
        elif query.data == "admin_manage_orders":
            if query.from_user.id != ADMIN_ID:
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            try:
                current_sheet = get_sheet()
                if not current_sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                values = current_sheet.get_all_values()
            except Exception as e:
                logger.error(f"Ошибка получения заказов: {e}")
                await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                return
            
            if len(values) <= 1:
                await query.edit_message_text(
                    "📦 Нет заказов.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
                )
                return

            msg = "🔄 Выберите заказ для изменения статуса:\n\n"
            keyboard = []
            for row in reversed(values[1:][-20:]):
                if len(row) >= 9:
                    order_num = row[0]
                    region = row[3]
                    tariff = row[4]
                    rub_amt = row[5]
                    status = row[8]
                    region_display = REGION_DISPLAY.get(region, region)
                    # Скрываем выполненные и отменённые заказы
                    if status in ["Выполнен", "Отменён"]:
                        continue
                    msg += f"🔹 <b>{order_num}</b> — {region_display} ({rub_amt} ₽) — {status}\n"
                    keyboard.append([InlineKeyboardButton(f"📝 {order_num}", callback_data=f"admin_select_order_{order_num}")])
            
            if not keyboard:
                await query.edit_message_text(
                    "📦 Нет активных заказов для изменения статуса.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
                )
                return
            
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")])
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

        # === АДМИН: ВЫБОР НОВОГО СТАТУСА ===
        elif query.data.startswith("admin_select_order_"):
            if query.from_user.id != ADMIN_ID:
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            order_num = query.data.replace("admin_select_order_", "")
            
            # Получаем информацию о заказе из таблицы
            order_info = ""
            try:
                current_sheet = get_sheet()
                if current_sheet:
                    values = current_sheet.get_all_values()
                    for row in values[1:]:
                        if len(row) >= 9 and row[0] == order_num:
                            region = row[3]
                            tariff = row[4]
                            amount_rub = row[5]
                            region_display = REGION_DISPLAY.get(region, region)
                            order_info = f"📦 Заказ: <b>{order_num}</b>\n🌍 Регион: {region_display}\n📋 Тариф: {tariff}\n💰 Сумма: {amount_rub} ₽\n\n"
                            break
            except Exception as e:
                logger.error(f"Ошибка получения информации о заказе: {e}")
            
            if not order_info:
                order_info = f"📦 Заказ: <b>{order_num}</b>\n\n"
            
            keyboard = [
                [InlineKeyboardButton("💰 Оплачен", callback_data=f"admin_set_status_{order_num}_paid")],
                [InlineKeyboardButton("✅ Выполнен", callback_data=f"admin_set_status_{order_num}_completed")],
                [InlineKeyboardButton("❌ Отменён", callback_data=f"admin_set_status_{order_num}_cancelled")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="admin_manage_orders")]
            ]
            await query.edit_message_text(
                f"{order_info}Выберите новый статус:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )

        # === АДМИН: ПРИМЕНЕНИЕ НОВОГО СТАТУСА ===
        elif query.data.startswith("admin_set_status_"):
            # ПРОВЕРКА БЕЗОПАСНОСТИ: только админ может менять статусы
            if query.from_user.id != ADMIN_ID:
                logger.warning(f"⚠️ Попытка изменения статуса от не-админа: {query.from_user.id}")
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            
            parts = query.data.replace("admin_set_status_", "").rsplit("_", 1)
            order_num = parts[0]
            new_status = parts[1]
            status_name = ORDER_STATUSES.get(new_status, new_status)
            
            success = update_order_status(order_num, status_name)
            
            if success:
                # Получаем user_id из таблицы (ORDER_USER_MAP может быть пустым после перезапуска)
                user_id = ORDER_USER_MAP.get(order_num)
                if not user_id:
                    try:
                        current_sheet = get_sheet()
                        if current_sheet:
                            values = current_sheet.get_all_values()
                            for row in values[1:]:
                                if len(row) >= 2 and row[0] == order_num:
                                    user_id = int(row[1])
                                    break
                    except Exception as e:
                        logger.error(f"Ошибка получения user_id из таблицы: {e}")
                
                if user_id:
                    # Определяем тип заказа по региону (KZ или Gift Card)
                    order_region = ""
                    try:
                        _sheet = get_sheet()
                        if _sheet:
                            _values = _sheet.get_all_values()
                            for _row in _values[1:]:
                                if len(_row) >= 4 and _row[0] == order_num:
                                    order_region = _row[3]  # столбец "Регион"
                                    break
                    except Exception as e:
                        logger.error(f"Ошибка определения региона заказа: {e}")

                    is_gift_card = order_region in ("TR", "US", "AE", "SA")

                    if new_status == "paid" and is_gift_card:
                        # Gift Card регионы: не запрашиваем почту, ожидание кода
                        client_message = (
                            "💰 Ваша оплата подтверждена! Заказ в обработке.\n\n"
                            "⏳ Ожидайте получения кода — бот отправит его вам.\n\n"
                            "⚠️ <b>Обратите внимание:</b> после получения кода средства возврату не подлежат. "
                            "При возникновении проблем обращайтесь в службу поддержки."
                        )
                        # Показываем админу кнопку для отправки кода
                        try:
                            admin_code_keyboard = [
                                [InlineKeyboardButton("📤 Отправить код клиенту", callback_data=f"send_code_{order_num}_{user_id}")],
                                [InlineKeyboardButton("💬 Связаться с клиентом", url=f"tg://user?id={user_id}")]
                            ]
                            await context.bot.send_message(
                                ADMIN_ID,
                                f"✅ Оплата подтверждена — Gift Card\n\n"
                                f"📦 Заказ: <b>{order_num}</b>\n"
                                f"🌍 Регион: {REGION_DISPLAY.get(order_region, order_region)}\n\n"
                                f"📤 Нажмите кнопку ниже, чтобы отправить код клиенту:",
                                reply_markup=InlineKeyboardMarkup(admin_code_keyboard),
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            logger.error(f"Ошибка отправки кнопки кода админу: {e}")
                    elif new_status == "paid":
                        # KZ: запрашиваем почту
                        client_message = "💰 Ваша оплата подтверждена! Заказ в обработке.\n\n📧 Пожалуйста, отправьте вашу почту Apple ID (email), на которую нужно выполнить пополнение:"
                        AWAITING_EMAIL[user_id] = order_num
                    else:
                        status_messages = {
                            "completed": "✅ Ваш заказ выполнен! Спасибо за покупку.",
                            "cancelled": "❌ Ваш заказ отменён. Если есть вопросы — свяжитесь с поддержкой."
                        }
                        client_message = status_messages.get(new_status, f"Статус заказа изменён на: {status_name}")
                    
                    try:
                        await context.bot.send_message(
                            user_id,
                            f"📦 <b>Заказ {order_num}</b>\n\n{client_message}",
                            parse_mode="HTML"
                        )
                        logger.info(f"Клиент {user_id} уведомлён о статусе {status_name}")
                    except Exception as e:
                        logger.error(f"Ошибка уведомления клиента о статусе: {e}")
                
                await query.edit_message_text(
                    f"✅ Статус заказа <b>{order_num}</b> изменён на: <b>{status_name}</b>\n\n"
                    f"{'✉️ Клиент уведомлён.' if user_id else '⚠️ Не удалось уведомить клиента (ID не найден).'}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 К списку заказов", callback_data="admin_manage_orders")],
                        [InlineKeyboardButton("⬅️ В админ-панель", callback_data="back_to_admin")]
                    ]),
                    parse_mode="HTML"
                )
                logger.info(f"Админ изменил статус {order_num} на {status_name}")
            else:
                await query.edit_message_text(
                    f"❌ Ошибка изменения статуса заказа <b>{order_num}</b>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_manage_orders")]]),
                    parse_mode="HTML"
                )

        # === ОТПРАВКА КОДА КЛИЕНТУ (Gift Card) ===
        elif query.data.startswith("send_code_"):
            if query.from_user.id != ADMIN_ID:
                return
            
            parts = query.data.replace("send_code_", "").split("_")
            order_num = parts[0]
            client_id = int(parts[1]) if len(parts) > 1 else None
            
            if not client_id:
                await query.edit_message_text("❌ Не удалось определить клиента.")
                return
            
            # Ставим админа в режим ожидания ввода кода
            AWAITING_CODE[ADMIN_ID] = {"order_num": order_num, "client_id": client_id}
            
            await query.edit_message_text(
                f"📤 <b>Отправка кода</b>\n\n"
                f"📦 Заказ: <b>{order_num}</b>\n\n"
                f"Введите код Gift Card для отправки клиенту:",
                parse_mode="HTML"
            )
            logger.info(f"Админ готовится отправить код для заказа {order_num}")

        # === ПОПОЛНЕНИЕ ПРОИЗВЕДЕНО (после получения почты) ===
        elif query.data.startswith("topup_done_"):
            if query.from_user.id != ADMIN_ID:
                return
            
            parts = query.data.replace("topup_done_", "").split("_")
            order_num = parts[0]
            client_id = int(parts[1]) if len(parts) > 1 else None
            
            # Меняем статус на "Выполнен"
            update_order_status(order_num, ORDER_STATUSES["completed"])
            
            # Уведомляем клиента
            if client_id:
                try:
                    await context.bot.send_message(
                        client_id,
                        f"🎉 <b>Пополнение выполнено!</b>\n\n"
                        f"📦 Заказ: <b>{order_num}</b>\n\n"
                        f"✅ Ваш Apple ID успешно пополнен!\n"
                        f"Спасибо, что воспользовались нашим сервисом! 🍏",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка уведомления клиента о пополнении: {e}")
            
            await query.edit_message_text(
                f"✅ Заказ <b>{order_num}</b> выполнен!\n\n"
                f"Клиент уведомлён о пополнении.",
                parse_mode="HTML"
            )
            logger.info(f"Админ отметил пополнение выполненным: {order_num}")

        # === НАЗАД В АДМИН-ПАНЕЛЬ ===
        elif query.data == "back_to_admin":
            if query.from_user.id != ADMIN_ID:
                return
            keyboard = [
                [InlineKeyboardButton("📊 Общая статистика", callback_data="stats_general")],
                [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
                [InlineKeyboardButton("🔄 Изменить статус заказа", callback_data="admin_manage_orders")]
            ]
            await query.edit_message_text(
                "⚙️ Админ панель",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass  # Игнорируем — пользователь нажал ту же кнопку
        else:
            logger.error(f"Ошибка в buttons: {e}")
            try:
                await query.edit_message_text("❌ Произошла ошибка. Попробуйте позже.")
            except:
                pass
    except Exception as e:
        logger.error(f"Ошибка в buttons: {e}")
        try:
            await query.edit_message_text("❌ Произошла ошибка. Попробуйте позже.")
        except:
            pass


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка скриншотов от клиентов"""
    user_id = update.message.from_user.id
    
    try:
        # Проверяем, ожидаем ли скриншот от этого пользователя
        order_number = AWAITING_SCREENSHOT.get(user_id)
        
        if not order_number:
            return  # Не ожидаем скриншот, игнорируем фото
        
        # Получаем информацию о заказе
        order_info = ORDER_INFO_MAP.get(order_number, {})
        
        # Пересылаем фото админу с информацией о заказе
        try:
            await context.bot.send_photo(
                ADMIN_ID,
                photo=update.message.photo[-1].file_id,  # Берём самое большое фото
                caption=(
                    f"📸 <b>Скриншот оплаты!</b>\n\n"
                    f"<b>📦 Заказ:</b> {order_number}\n"
                    f"<b>Регион:</b> {REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', 'N/A'))}\n"
                    f"<b>Тариф:</b> {order_info.get('tariff', 'N/A')}\n"
                    f"<b>Сумма:</b> {fmt(order_info.get('rub', 0))} ₽\n\n"
                    f"<b>👤 Клиент:</b>\n"
                    f"Имя: {update.message.from_user.first_name or 'Неизвестно'}\n"
                    f"Ник: @{update.message.from_user.username if update.message.from_user.username else 'нет'}\n"
                    f"ID: <code>{user_id}</code>"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить оплату", callback_data=f"admin_set_status_{order_number}_paid")],
                    [InlineKeyboardButton("❌ Отклонить", callback_data=f"admin_set_status_{order_number}_cancelled")],
                    [InlineKeyboardButton("💬 Написать клиенту", url=f"tg://user?id={user_id}")]
                ]),
                parse_mode="HTML"
            )
            logger.info(f"Скриншот от {user_id} переслан админу для заказа {order_number}")
        except Exception as e:
            logger.error(f"Ошибка пересылки скриншота админу: {e}")
        
        # Подтверждаем клиенту
        await update.message.reply_text(
            f"✅ <b>Скриншот получен!</b>\n\n"
            f"Заказ: <b>{order_number}</b>\n\n"
            f"Ваш скриншот отправлен на проверку менеджеру.\n"
            f"Ожидайте подтверждения оплаты.",
            parse_mode="HTML"
        )
        
        # Убираем из ожидания (можно оставить для повторных скриншотов)
        # del AWAITING_SCREENSHOT[user_id]
        
    except Exception as e:
        logger.error(f"Ошибка в photo_handler: {e}")


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    try:
        # === ОБРАБОТКА КОДА ОТ АДМИНА (Gift Card) ===
        if user_id == ADMIN_ID and ADMIN_ID in AWAITING_CODE:
            code_data = AWAITING_CODE[ADMIN_ID]
            code_order = code_data["order_num"]
            code_client = code_data["client_id"]
            gift_code = text
            
            del AWAITING_CODE[ADMIN_ID]
            
            # Меняем статус на "Выполнен"
            update_order_status(code_order, ORDER_STATUSES["completed"])
            
            # Отправляем код клиенту
            try:
                await context.bot.send_message(
                    code_client,
                    f"🎉 <b>Ваш код получен!</b>\n\n"
                    f"📦 Заказ: <b>{code_order}</b>\n\n"
                    f"🔑 Код Gift Card:\n<code>{gift_code}</code>\n\n"
                    f"Активируйте код в App Store / iTunes.\n"
                    f"Спасибо, что воспользовались нашим сервисом! 🍏",
                    parse_mode="HTML"
                )
                logger.info(f"Код отправлен клиенту {code_client} для заказа {code_order}")
            except Exception as e:
                logger.error(f"Ошибка отправки кода клиенту: {e}")
                await update.message.reply_text(
                    f"❌ Не удалось отправить код клиенту. Ошибка: {e}"
                )
                return
            
            await update.message.reply_text(
                f"✅ Код отправлен клиенту!\n\n"
                f"📦 Заказ: <b>{code_order}</b>\n"
                f"📊 Статус: Выполнен",
                parse_mode="HTML"
            )
            return

        # === ОБРАБОТКА ПОЧТЫ APPLE ID ===
        if user_id in AWAITING_EMAIL:
            order_number = AWAITING_EMAIL.get(user_id)
            email = text.lower()
            
            # Простая проверка на email
            if "@" in email and "." in email:
                # Убираем из ожидания
                del AWAITING_EMAIL[user_id]
                
                # Получаем информацию о пользователе
                user = update.message.from_user
                user_name = user.full_name or "Без имени"
                username = f"@{user.username}" if user.username else "Нет username"
                
                # Получаем информацию о заказе для админа
                order_details = ""
                try:
                    _sheet = get_sheet()
                    if _sheet:
                        _values = _sheet.get_all_values()
                        for _row in _values[1:]:
                            if len(_row) >= 7 and _row[0] == order_number:
                                region_display = REGION_DISPLAY.get(_row[3], _row[3])
                                order_details = (
                                    f"🌍 Регион: {region_display}\n"
                                    f"📋 Тариф: {_row[4]}\n"
                                    f"💰 Сумма: {_row[5]} ₽\n\n"
                                )
                                break
                except Exception as e:
                    logger.error(f"Ошибка получения деталей заказа: {e}")

                # Уведомляем админа с кнопками
                try:
                    admin_keyboard = [
                        [InlineKeyboardButton("✅ Пополнение произведено", callback_data=f"topup_done_{order_number}_{user_id}")],
                        [InlineKeyboardButton("💬 Связаться с клиентом", url=f"tg://user?id={user_id}")]
                    ]
                    await context.bot.send_message(
                        ADMIN_ID,
                        f"📧 <b>Получена почта Apple ID</b>\n\n"
                        f"📦 Заказ: <b>{order_number}</b>\n\n"
                        f"{order_details}"
                        f"📧 Почта для пополнения:\n"
                        f"<code>{email}</code>\n\n"
                        f"👤 Клиент:\n"
                        f"Имя: {user_name}\n"
                        f"Ник: {username}\n"
                        f"ID: <code>{user_id}</code>",
                        reply_markup=InlineKeyboardMarkup(admin_keyboard),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки почты админу: {e}")
                
                await update.message.reply_text(
                    f"✅ Почта <b>{email}</b> получена!\n\n"
                    f"📦 Заказ: <b>{order_number}</b>\n\n"
                    f"Мы пополним ваш Apple ID в ближайшее время. Ожидайте уведомления!",
                    parse_mode="HTML"
                )
                logger.info(f"Клиент {user_id} отправил почту {email} для заказа {order_number}")
                return
            else:
                await update.message.reply_text(
                    "❌ Некорректный email. Пожалуйста, отправьте правильную почту Apple ID:"
                )
                return

        # === ОБРАБОТКА КНОПОК REPLY KEYBOARD ===
        if text == "🍏 Пополнить Apple ID":
            keyboard = [
                [InlineKeyboardButton("🇺🇸 США", callback_data="region_US")],
                [InlineKeyboardButton("🇦🇪 ОАЭ", callback_data="region_AE")],
                [InlineKeyboardButton("🇹🇷 Турция", callback_data="region_TR")],
                [InlineKeyboardButton("🇰🇿 Казахстан", callback_data="region_KZ")],
                [InlineKeyboardButton("🇸🇦 Саудовская Аравия", callback_data="region_SA")]
            ]
            await update.message.reply_text(
                "🍏 Пополнение Apple ID\n\n"
                "Выбери регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if text == "⭐ Отзывы":
            keyboard = [
                [InlineKeyboardButton("📢 Отзывы в канале", url="https://t.me/popolnyaskaservice")],
                [InlineKeyboardButton("✍️ Оставить отзыв", url="https://t.me/poplnyaska_halper")]
            ]
            await update.message.reply_text(
                "⭐ <b>Отзывы наших клиентов</b>\n\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Пополнил Apple ID за 15 минут, всё чётко! Рекомендую\"</i>\n"
                "— Клиент из Казахстана\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Заказал Gift Card США, код пришёл быстро. Сервис огонь 🔥\"</i>\n"
                "— Клиент из России\n\n"
                "⭐⭐⭐⭐⭐\n"
                "<i>\"Второй раз пользуюсь, всё работает без проблем\"</i>\n"
                "— Постоянный клиент\n\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                "📢 Больше отзывов — в нашем канале\n"
                "✍️ Хотите оставить отзыв? Напишите менеджеру!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            return

        if text == "❓ FAQ":
            await update.message.reply_text(
                "❓ Часто задаваемые вопросы\n\n"
                "Выберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        if text == "📋 Заказы":
            try:
                current_sheet = get_sheet()
                if not current_sheet:
                    await update.message.reply_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                records = current_sheet.get_all_records()
            except Exception as e:
                logger.error(f"Ошибка получения записей из таблицы: {e}")
                await update.message.reply_text("⚠️ Ошибка доступа к таблице.")
                return
            
            user_records = [r for r in records if str(r.get("User_ID", "")) == str(user_id)]
            
            if not user_records:
                await update.message.reply_text(
                    "📋 У вас пока нет заказов.\n\n"
                    "Нажми «🍏 Пополнить Apple ID» чтобы создать заказ."
                )
                return
            
            msg = "📋 Ваши заказы:\n\n"
            for record in user_records:
                order_num = record.get("Номер ордера", "N/A")
                tariff = record.get("Тариф", "N/A")
                rub_amt = record.get("Сумма RUB", "N/A")
                status = record.get("Статус", "Ожидает оплаты")
                region = record.get("Регион", "")
                region_display = REGION_DISPLAY.get(region, region) if region else "—"
                
                msg += (
                    f"🔹 {order_num}\n"
                    f"   Регион: {region_display}\n"
                    f"   Тариф: {tariff}\n"
                    f"   Сумма: {rub_amt} ₽\n"
                    f"   Статус: {status}\n\n"
                )
            
            await update.message.reply_text(msg)
            logger.info(f"Пользователь {user_id} просмотрел {len(user_records)} заказов")
            return

        # === ВВОД КАСТОМНОЙ СУММЫ ===
        if context.user_data.get("awaiting_apple", False):
            try:
                amount = int(text)
                if not (2000 <= amount <= 45000):
                    await update.message.reply_text(
                        "❌ Неверный диапазон.\n\nВведите сумму от 2 000 до 45 000 KZT:"
                    )
                    return
                
                # Проверка антиспам
                can_create, spam_msg = check_spam(user_id)
                if not can_create:
                    await update.message.reply_text(spam_msg)
                    return

                rate = get_rate()
                if not rate:
                    await update.message.reply_text("❌ Ошибка получения курса. Попробуйте позже.")
                    return
                
                rub = int(amount * rate * 1.15)
                order_number = generate_order()
                
                if not order_number:
                    await update.message.reply_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                    return
                
                user = update.message.from_user
                tariff_name = f"{fmt(amount)} KZT"

                context.user_data["order"] = {
                    "number": order_number,
                    "service": "Apple ID",
                    "tariff": tariff_name,
                    "kzt": amount,
                    "rub": rub,
                    "region": "KZ",
                    "user": user
                }

                keyboard = [
                    [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="apple_topup")]
                ]

                await update.message.reply_text(
                    f"📦 Информация о заказе\n\n"
                    f"Номер заказа: <b>{order_number}</b>\n"
                    f"Тариф: <b>{tariff_name}</b>\n"
                    f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (комиссия 15%)",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["awaiting_apple"] = False
                logger.info(f"Пользователь {user_id} создал заказ Apple на {amount} KZT")
                return

            except ValueError:
                await update.message.reply_text(
                    "❌ Введите только число.\n\nПовторите ввод суммы (2 000–45 000 KZT):"
                )
                return

    except Exception as e:
        logger.error(f"Ошибка в text_handler: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")


async def pre_checkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение pre-checkout запроса от Telegram"""
    query = update.pre_checkout_query
    try:
        await query.answer(ok=True)
        logger.info(f"Pre-checkout подтверждён для заказа {query.invoice_payload}")
    except Exception as e:
        logger.error(f"Ошибка pre-checkout: {e}")
        await query.answer(ok=False, error_message="Ошибка обработки платежа. Попробуйте позже.")


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка успешной оплаты"""
    try:
        payment = update.message.successful_payment
        order_number = payment.invoice_payload
        user_id = update.message.from_user.id
        total_amount = payment.total_amount / 100  # копейки → рубли

        # Обновляем статус заказа
        update_order_status(order_number, ORDER_STATUSES["paid"])

        # Уведомляем клиента
        await update.message.reply_text(
            f"✅ Оплата прошла успешно!\n\n"
            f"Номер заказа: <b>{order_number}</b>\n"
            f"Сумма: <b>{int(total_amount)} ₽</b>\n\n"
            f"Менеджер свяжется с вами в течение 30 минут для предоставления доступа.",
            parse_mode="HTML"
        )

        # Уведомляем админа
        user = update.message.from_user
        username_display = f'<a href="https://t.me/{user.username}">@{user.username}</a>' if user.username else 'Нет'
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"💰 Получена оплата!\n\n"
                f"<b>📦 Заказ:</b> {order_number}\n"
                f"<b>💵 Сумма:</b> {int(total_amount)} ₽\n\n"
                f"<b>👤 Клиент:</b>\n"
                f'Имя: <a href="tg://user?id={user_id}">{user.first_name or "Клиент"}</a>\n'
                f"Ник: {username_display}\n"
                f"ID: <code>{user_id}</code>\n\n"
                f"⚡ Статус автоматически изменён на <b>Оплачен</b>",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления админа об оплате: {e}")

        logger.info(f"✅ Оплата получена: {order_number}, {int(total_amount)} ₽, user {user_id}")

    except Exception as e:
        logger.error(f"Ошибка обработки успешной оплаты: {e}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    logger.error(f"Ошибка при обработке запроса: {context.error}")


# === ЗАПУСК БОТА ===

if __name__ == "__main__":
    import asyncio
    import signal

    app = ApplicationBuilder().token(TOKEN).build()
    _bot_app = app

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout_handler))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    async def shutdown(sig=None):
        """Корректное завершение работы бота"""
        if sig:
            logger.info(f"Получен сигнал {sig.name}, завершаем работу...")
        
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            logger.info("✅ Бот остановлен")
        except Exception as e:
            logger.error(f"Ошибка остановки бота: {e}")
        
        cleanup_memory()
        logger.info("✅ Завершение работы")

    async def main():
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
        except NotImplementedError:
            pass
        
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Бот успешно запущен")
        
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await shutdown()

    print("✅ Бот запущен!")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
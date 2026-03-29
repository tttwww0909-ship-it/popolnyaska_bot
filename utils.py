"""
Утилиты: TimedDict, форматирование, курсы валют, антиспам, in-memory хранилища.
"""

import time
import logging
import threading
import sqlite3
import requests

logger = logging.getLogger(__name__)


# === ХРАНИЛИЩЕ ДАННЫХ (с лимитом времени жизни) ===
class TimedDict(dict):
    """Словарь, который автоматически удаляет старые записи"""
    def __init__(self, max_age_seconds=86400):
        super().__init__()
        self.max_age = max_age_seconds
        self.timestamps = {}

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.timestamps[key] = time.time()

    def __getitem__(self, key):
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

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False

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


# === IN-MEMORY ХРАНИЛИЩА ===
ORDER_USER_MAP = TimedDict(max_age_seconds=86400)     # 24 часа
ORDER_INFO_MAP = TimedDict(max_age_seconds=604800)    # 7 дней
ORDER_LOCK = {}       # Защита от дублей: {order_number: True}
AWAITING_SCREENSHOT = TimedDict(max_age_seconds=86400)
AWAITING_EMAIL = TimedDict(max_age_seconds=86400)
AWAITING_CODE = {}    # admin_id: {"order_num": ..., "client_id": ...}
AWAITING_REVIEW_COMMENT = {}  # user_id: {"order_num": ..., "rating": ...}

_ORDER_COUNTER_LOCK = threading.Lock()

# === АНТИСПАМ ===
USER_ORDER_TIMES = {}
ORDER_COOLDOWN = 60
MAX_ORDERS_IN_PERIOD = 3
ORDER_PERIOD = 1200


def check_spam(user_id: int) -> tuple[bool, str]:
    """Проверка на спам. Возвращает (можно_создать, сообщение_ошибки)"""
    now = time.time()

    if user_id in USER_ORDER_TIMES:
        USER_ORDER_TIMES[user_id] = [t for t in USER_ORDER_TIMES[user_id] if now - t < ORDER_PERIOD]

    if user_id in USER_ORDER_TIMES and USER_ORDER_TIMES[user_id]:
        last_order = max(USER_ORDER_TIMES[user_id])
        elapsed = now - last_order
        if elapsed < ORDER_COOLDOWN:
            wait = int(ORDER_COOLDOWN - elapsed)
            return False, f"⏳ Подождите {wait} сек. перед созданием нового заказа."

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


# === ВАЛИДАЦИЯ ===
import re
_EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')


def validate_email(email: str) -> bool:
    """Проверяет формат email"""
    return bool(_EMAIL_RE.match(email))


# === ФОРМАТИРОВАНИЕ ===
def fmt(num):
    """Форматирует число с пробелами между тысячами: 25000 → 25 000"""
    return f"{int(num):,}".replace(",", " ")


# === КУРСЫ ВАЛЮТ ===
rate_cache = {"value": None, "time": 0}
usdt_cache = {"value": None, "time": 0}

_CBR_URL = "https://www.cbr-xml-daily.ru/daily_json.js"


def _fetch_cbr() -> dict | None:
    """Загружает JSON ЦБ РФ с кэшированием на уровне модуля."""
    try:
        return requests.get(_CBR_URL, timeout=10).json()
    except Exception as e:
        logger.error(f"Ошибка запроса ЦБ РФ: {e}")
        return None


def get_rate():
    """Получение курса KZT → RUB (ЦБ РФ, кэш 1 ч)."""
    if time.time() - rate_cache["time"] < 3600 and rate_cache["value"] is not None:
        return rate_cache["value"]
    data = _fetch_cbr()
    if data:
        try:
            value = data["Valute"]["KZT"]["Value"]
            nominal = data["Valute"]["KZT"]["Nominal"]
            rate = value / nominal
            rate_cache["value"] = rate
            rate_cache["time"] = time.time()
            logger.debug(f"Курс KZT обновлён: {rate}")
            return rate
        except (KeyError, ZeroDivisionError) as e:
            logger.error(f"Ошибка парсинга курса KZT: {e}")
    if rate_cache["value"] is not None:
        return rate_cache["value"]
    return None


def get_usdt_rate():
    """Получает курс USDT/RUB.

    Источник — ЦБ РФ (USD/RUB), т.к. USDT ≈ USD с отклонением <0.5%.
    Кэш 1 час.
    """
    if time.time() - usdt_cache["time"] < 3600 and usdt_cache["value"] is not None:
        return usdt_cache["value"]
    data = _fetch_cbr()
    if data:
        try:
            price = data["Valute"]["USD"]["Value"]
            usdt_cache["value"] = price
            usdt_cache["time"] = time.time()
            logger.debug(f"Курс USDT/RUB (via ЦБ USD): {price}")
            return price
        except KeyError as e:
            logger.error(f"Ошибка парсинга курса USD: {e}")
    if usdt_cache["value"] is not None:
        return usdt_cache["value"]
    return None


def generate_order():
    """Генерация номера ордера. Lock защищает от race condition."""
    with _ORDER_COUNTER_LOCK:
        try:
            max_number = 1000
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


def cleanup_memory():
    """Очищает память от устаревших данных"""
    try:
        ORDER_USER_MAP.cleanup()
        ORDER_INFO_MAP.cleanup()
        logger.info("Память очищена")
    except Exception as e:
        logger.error(f"Ошибка при очистке памяти: {e}")

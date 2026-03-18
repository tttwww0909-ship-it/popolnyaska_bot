import requests
import gspread
import time
import hmac
import hashlib
import json
import logging
import os
import asyncio
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiohttp import web

# === ЗАГРУЖАЕМ ПЕРЕМЕННЫЕ ИЗ .env ===
load_dotenv()
from database import db

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET", "")
YOOMONEY_SECRET = os.getenv("YOOMONEY_SECRET", "")

# Платёжные реквизиты
OZON_PAY_URL = os.getenv("OZON_PAY_URL", "")
BYBIT_UID = os.getenv("BYBIT_UID", "")
TRC20_ADDRESS = os.getenv("TRC20_ADDRESS", "")

# Проверяем, что все переменные загружены
if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не установлен в .env файле!")
if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID не установлен в .env файле!")

print(f"✅ Переменные окружения загружены")
print(f"   TOKEN: {TOKEN[:10]}... (скрыто)")
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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
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
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp > self.max_age
        ]
        for key in expired_keys:
            try:
                del self[key]
                del self.timestamps[key]
                logger.info(f"Cleaned up expired key: {key}")
            except KeyError:
                pass


user_orders = {}  # Формат: {user_id: [timestamp1, timestamp2, ...]}
ORDER_USER_MAP = TimedDict(max_age_seconds=86400)  # 24 часа
PAYMENT_MAP = TimedDict(max_age_seconds=3600)  # 1 час
ORDER_INFO_MAP = TimedDict(max_age_seconds=604800)  # 7 дней
ORDER_LOCK = {}  # Защита от дублей: {order_number: True}
AWAITING_SCREENSHOT = TimedDict(max_age_seconds=86400)  # user_id: order_number


# === GOOGLE SHEETS ===
def get_sheet():
    """Получает объект таблицы с обработкой ошибок"""
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
        sheet = client.open("Pay&UsetgBot").sheet1
        logger.info("✅ Подключение к Google Sheets успешно")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        return None


sheet = get_sheet()


PRICES = {
    "apple_5000": 5000,
    "apple_10000": 10000,
    "apple_25000": 25000
}

ORDER_STATUSES = {
    "new": "Новый",
    "paid": "Оплачен",
    "processing": "В обработке",
    "completed": "Выполнен",
    "cancelled": "Отменён"
}


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
        if sheet:
            try:
                records = sheet.get_all_records()
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
            import sqlite3
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
        if sheet:
            try:
                from datetime import datetime
                current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
                sheet.append_row([
                    order_data["number"],
                    order_data["user_id"],
                    order_data["username"],
                    order_data["service"],
                    order_data["tariff"],
                    order_data["kzt"],
                    order_data["rub"],
                    "",  # Способ оплаты - заполнится позже
                    current_date,
                    ORDER_STATUSES["new"]
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
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении заказа: {e}")
        return False

def update_payment_method(order_number, payment_method):
    """Записывает способ оплаты в Google Sheets (колонка H)"""
    try:
        if sheet:
            cell = sheet.find(order_number)
            if cell:
                sheet.update_cell(cell.row, 8, payment_method)
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
        if sheet:
            try:
                cell = sheet.find(order_number)
                if cell:
                    sheet.update_cell(cell.row, 10, new_status)
                    logger.info(f"✅ Статус {order_number} обновлён в Google Sheets")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обновления Google Sheets: {e}")
        
        logger.info(f"✅ Статус {order_number} изменён на {new_status}")
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
        
        # Очищаем user_orders старше 20 минут
        current_time = time.time()
        for user_id in list(user_orders.keys()):
            user_orders[user_id] = [
                t for t in user_orders[user_id]
                if current_time - t < 1200
            ]
            if not user_orders[user_id]:
                del user_orders[user_id]
        
        logger.info("Память очищена")
    except Exception as e:
        logger.error(f"Ошибка при очистке памяти: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню"""
    try:
        keyboard = [
            [InlineKeyboardButton("🍎 Пополнить Apple ID", callback_data="apple_topup")],
            [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")]
        ]
        await update.message.reply_text(
            "Добро пожаловать в Pay&Use! 🚀\n\n"
            "Пополнение Apple ID в Казахстане.",
            reply_markup=InlineKeyboardMarkup(keyboard)
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
            [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
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
            user_records = [r for r in records if str(r.get("ID", "")) == str(user_id)]
            
            if not user_records:
                await query.edit_message_text(
                    "📋 У вас пока нет заказов.\n\n"
                    "Создайте новый заказ!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🧾 Новый заказ", callback_data="new_order")],
                        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]
                    ])
                )
                logger.info(f"Пользователь {user_id} проверил заказы - нет заказов")
                return
            
            msg = "📋 Ваши заказы:\n\n"
            
            for record in user_records:
                order_num = record.get("Номер ордера", "N/A")
                service = record.get("Сервис", "N/A")
                tariff = record.get("Тариф", "N/A")
                rub_amt = record.get("Сумма RUB", "N/A")
                status = record.get("Статус", "Новый")
                
                msg += (
                    f"🔹 {order_num}\n"
                    f"   Сервис: {service}\n"
                    f"   Тариф: {tariff}\n"
                    f"   Сумма: {rub_amt} ₽\n"
                    f"   Статус: {status}\n\n"
                )
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]
            ]
            
            await query.edit_message_text(
                msg,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            logger.info(f"Пользователь {user_id} просмотрел {len(user_records)} заказов")
            return
        
        # === НАЗАД В ГЛАВНОЕ МЕНЮ ===
        if query.data == "back_to_start":
            keyboard = [
                [InlineKeyboardButton("🍎 Пополнить Apple ID", callback_data="apple_topup")],
                [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")]
            ]
            await query.edit_message_text(
                "Добро пожаловать в Pay&Use! 🚀\n\n"
                "Пополнение Apple ID в Казахстане.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === ПОПОЛНЕНИЕ APPLE ID ===
        if query.data == "apple_topup":
            keyboard = [
                [InlineKeyboardButton("🍏 5 000 KZT", callback_data="apple_5000")],
                [InlineKeyboardButton("🍏 10 000 KZT", callback_data="apple_10000")],
                [InlineKeyboardButton("🍏 25 000 KZT", callback_data="apple_25000")],
                [InlineKeyboardButton("✏️ Ввести свою сумму", callback_data="apple_custom")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]
            ]
            await query.edit_message_text(
                "🍎 Пополнение Apple ID\n\n"
                "Выберите сумму пополнения:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        elif query.data == "apple_custom":
            context.user_data["awaiting_apple"] = True
            await query.edit_message_text(
                "Введите сумму пополнения Apple ID (400–45 000 KZT)"
            )

        # === ПОСЛЕ ВЫБОРА СЕРВИСА — ПОКАЗЫВАЕМ ЗАЯВКУ ===
        # === ВЫБОР ТАРИФА APPLE ID ===
        elif query.data.startswith("apple_") and query.data in PRICES:
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
            
            user = query.from_user
            tariff_name = f"{fmt(amount)} KZT"

            context.user_data["order"] = {
                "number": order_number,
                "service": "Apple ID",
                "tariff": tariff_name,
                "kzt": amount,
                "rub": rub,
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
            
            # Защита от дублей
            if order_number in ORDER_LOCK:
                await query.edit_message_text("⏳ Заказ уже обрабатывается. Подождите...")
                return
            
            ORDER_LOCK[order_number] = True
            
            try:
                user_id = order["user"].id
                ORDER_USER_MAP[order_number] = user_id

                now = time.time()

                if user_id not in user_orders:
                    user_orders[user_id] = []

                user_orders[user_id] = [t for t in user_orders[user_id] if now - t < 1200]

                if len(user_orders[user_id]) >= 3:
                    await query.edit_message_text(
                        "⚠️ Слишком много заявок.\n\nПопробуйте снова через 20 минут."
                    )
                    del ORDER_LOCK[order_number]
                    return

                user_orders[user_id].append(now)

                # === ДОБАВЛЯЕМ В ТАБЛИЦУ ===
                order_data = {
                    "number": order_number,
                    "user_id": user_id,
                    "username": order["user"].username or "Нет ника",
                    "service": order["service"],
                    "tariff": order["tariff"],
                    "kzt": order["kzt"],
                    "rub": order["rub"]
                }
                
                if not add_order_to_sheet(order_data):
                    await query.edit_message_text("❌ Ошибка сохранения заказа. Попробуйте позже.")
                    del ORDER_LOCK[order_number]
                    return

                # === СОХРАНЯЕМ ИНФОРМАЦИЮ О ЗАКАЗЕ ===
                ORDER_INFO_MAP[order_number] = {
                    "user_id": user_id,
                    "username": order["user"].username or "Нет ника",
                    "first_name": order["user"].first_name or "Клиент",
                    "service": order["service"],
                    "tariff": order["tariff"],
                    "kzt": order["kzt"],
                    "rub": order["rub"]
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

                # === ОТПРАВЛЯЕМ АДМИНУ ===
                try:
                    username_text = f'@{order["user"].username}' if order['user'].username else 'Нет'
                    await context.bot.send_message(
                        ADMIN_ID,
                        f"🆕 Новый заказ ждёт оплаты\n\n"
                        f"<b>📦 Информация о заказе:</b>\n"
                        f"Номер: <b>{order['number']}</b>\n"
                        f"Сервис: <b>{order['service']}</b>\n"
                        f"Тариф: <b>{order['tariff']}</b>\n"
                        f"Сумма: <b>{fmt(order['rub'])} ₽</b> ({fmt(order['kzt'])} KZT)\n\n"
                        f"<b>👤 Клиент:</b> {order['user'].first_name or 'Неизвестно'} ({username_text})\n\n"
                        f"<b>📊 Статус:</b> Новый (ожидание оплаты)",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения админу: {e}")

                # === ВЫБОР СПОСОБА ОПЛАТЫ ===
                usdt_rate = get_usdt_rate()
                amount_usdt = round(order["rub"] / usdt_rate, 2)
                context.user_data["amount_usdt"] = amount_usdt

                await query.edit_message_text(
                    f"✅ Заявка сформирована!\n\n"
                    f"Номер заказа: <b>{order['number']}</b>\n"
                    f"Тариф: <b>{order['tariff']}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b> (~{amount_usdt} USDT)\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]),
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
            update_payment_method(order_number, "OZON")
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
                f"1. Откройте приложение <b>Bybit</b>\n"
                f"2. Перейдите в раздел <b>Перевод</b>\n"
                f"3. Введите UID: <code>{BYBIT_UID}</code>\n"
                f"4. Сумма: <b>{amount_usdt} USDT</b>\n\n"
                f"📲 <b>Способ 2: TRC20 (любой кошелёк)</b>\n"
                f"Адрес: <code>{TRC20_ADDRESS}</code>\n"
                f"Сеть: <b>TRON (TRC20)</b>\n"
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

            await query.edit_message_text(
                f"⏳ Заявка на проверку отправлена!\n\n"
                f"Заказ: <b>{order_number}</b>\n"
                f"Сумма: <b>{amount_usdt} USDT</b>\n\n"
                f"📸 <b>Отправьте скриншот подтверждения оплаты</b> прямо в этот чат.\n"
                f"Скриншот будет автоматически переслан менеджеру.",
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

            await query.edit_message_text(
                f"⏳ Заявка на проверку отправлена!\n\n"
                f"Заказ: <b>{order_number}</b>\n"
                f"Сумма: <b>{fmt(amount_rub)} ₽</b>\n\n"
                f"📸 <b>Отправьте скриншот подтверждения оплаты</b> прямо в этот чат.\n"
                f"Скриншот будет автоматически переслан менеджеру.",
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

            await query.edit_message_text(
                f"⏳ Заявка на проверку отправлена!\n\n"
                f"Заказ: <b>{order_number}</b>\n"
                f"Сумма: <b>{fmt(amount_rub)} ₽</b>\n\n"
                f"📸 <b>Отправьте скриншот подтверждения оплаты</b> прямо в этот чат.\n"
                f"Скриншот будет автоматически переслан менеджеру.",
                parse_mode="HTML"
            )
            logger.info(f"Клиент {user_id} нажал 'Я оплатил' (OZON) для {order_number}")

        # === ПОМОЩЬ ===
        elif query.data == "help_payment":
            keyboard = [
                [InlineKeyboardButton("👨‍💼 Связаться с администратором", callback_data="contact_manager")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_payment")]
            ]
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\n"
                "1️⃣ <b>Как оплатить?</b>\n"
                "Доступные способы оплаты:\n"
                "• 💳 ЮMoney — перевод на кошелёк\n"
                "• 💳 OZON банк — без комиссии с OZON / 1.9% с других\n"
                f"• 💎 Крипта USDT — через Bybit (UID: <code>{BYBIT_UID}</code>) "
                f"или TRC20: <code>{TRC20_ADDRESS}</code>\n\n"
                "2️⃣ <b>Когда я получу доступ?</b>\n"
                "После подтверждения оплаты — в течение 30 минут.\n\n"
                "3️⃣ <b>Что если платёж не прошёл?</b>\n"
                "Свяжитесь с менеджером через кнопку ниже.",
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
                await query.edit_message_text(
                    f"✅ Заявка отправлена!\n\n"
                    f"Номер заказа: <b>{order_number}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b> (~{amount_usdt} USDT)\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💳 Оплатить через ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💎 Оплатить криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ Помощь", callback_data="help_payment")]
                    ]),
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
                client_info += f"\n\n<b>📦 Последний заказ:</b>\n" \
                              f"Номер: <b>{latest_order}</b>\n" \
                              f"Сервис: <b>{order_info['service']}</b>\n" \
                              f"Тариф: <b>{order_info['tariff']}</b>\n" \
                              f"Сумма: <b>{order_info['rub']} ₽</b> ({order_info['kzt']} KZT)"
            elif sheet:
                try:
                    records = sheet.get_all_records()
                    user_records = [r for r in records if str(r.get("ID", "")) == str(user_id)]
                    if user_records:
                        last = user_records[-1]
                        client_info += f"\n\n<b>📦 Последний заказ:</b>\n" \
                                      f"Номер: <b>{last.get('Номер ордера', 'N/A')}</b>\n" \
                                      f"Сервис: <b>{last.get('Сервис', 'N/A')}</b>\n" \
                                      f"Тариф: <b>{last.get('Тариф', 'N/A')}</b>\n" \
                                      f"Сумма: <b>{last.get('Сумма RUB', 'N/A')} ₽</b> ({last.get('Сумма KZT', 'N/A')} KZT)\n" \
                                      f"Статус: <b>{last.get('Статус', 'N/A')}</b>"
                    else:
                        client_info += "\n\n📦 Заказов не найдено"
                except Exception as e:
                    logger.warning(f"Ошибка получения заказов из Sheets для contact_manager: {e}")
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
            client_service = None
            client_tariff = None

            for order_num, info in ORDER_INFO_MAP.items():
                if info["user_id"] == client_id:
                    client_name = info["first_name"]
                    client_service = info["service"]
                    client_tariff = info["tariff"]
                    break

            if reason == "support":
                msg = (
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"Клиент запросил связь с менеджером.\n"
                    f"Если у него есть заказ:\n"
                    f"  • Сервис: {client_service or 'уточнить'}\n"
                    f"  • Тариф: {client_tariff or 'уточнить'}\n\n"
                    f"Напишите ему в личку и помогите!"
                )
            else:
                msg = (
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"<b>📦 Заказ:</b>\n"
                    f"  • Номер: {reason}\n"
                    f"  • Сервис: {client_service}\n"
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
                sheet = get_sheet()
                if not sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                values = sheet.get_all_values()
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
                if len(row) >= 10:
                    order_num = row[0]
                    user_id = row[1]
                    service = row[3]
                    tariff = row[4]
                    rub_amt = row[6]
                    status = row[9]
                    
                    msg += f"🔹 <b>{order_num}</b>\n"
                    msg += f"   Статус: {status}\n"
                    msg += f"   Сервис: {service}\n"
                    msg += f"   Тариф: {tariff}\n"
                    msg += f"   Сумма: {rub_amt} ₽\n"
                    msg += f"   ID: <code>{user_id}</code>\n\n"
            
            keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

        # === АДМИН-ПАНЕЛЬ: СТАТИСТИКА ===
        elif query.data == "admin_stats":
            if query.from_user.id != ADMIN_ID:
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            stats = db.get_stats()
            await query.edit_message_text(
                "📊 Статистика\n\n"
                f"👥 Пользователей: <b>{stats.get('users', 0)}</b>\n"
                f"📦 Всего заказов: <b>{stats.get('orders', 0)}</b>\n"
                f"✅ Оплаченных: <b>{stats.get('paid_orders', 0)}</b>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]),
                parse_mode="HTML"
            )

        # === АДМИН-ПАНЕЛЬ: УПРАВЛЕНИЕ СТАТУСАМИ ===
        elif query.data == "admin_manage_orders":
            if query.from_user.id != ADMIN_ID:
                await query.answer("❌ У вас нет доступа", show_alert=True)
                return
            try:
                sheet = get_sheet()
                if not sheet:
                    await query.edit_message_text("⚠️ Ошибка доступа к таблице.")
                    return
                
                values = sheet.get_all_values()
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
                if len(row) >= 10:
                    order_num = row[0]
                    service = row[3]
                    tariff = row[4]
                    amount_kzt = row[5]
                    status = row[9]
                    # Скрываем выполненные и отменённые заказы
                    if status in ["Выполнен", "Отменён"]:
                        continue
                    msg += f"🔹 <b>{order_num}</b> — {service} ({amount_kzt} KZT) — {status}\n"
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
                sheet = get_sheet()
                if sheet:
                    values = sheet.get_all_values()
                    for row in values[1:]:
                        if len(row) >= 10 and row[0] == order_num:
                            service = row[3]
                            tariff = row[4]
                            amount_kzt = row[5]
                            amount_rub = row[6]
                            order_info = f"📦 Заказ: <b>{order_num}</b>\n🛒 Сервис: {service}\n📋 Тариф: {tariff}\n💰 Сумма: {amount_kzt} KZT ({amount_rub} ₽)\n\n"
                            break
            except Exception as e:
                logger.error(f"Ошибка получения информации о заказе: {e}")
            
            if not order_info:
                order_info = f"📦 Заказ: <b>{order_num}</b>\n\n"
            
            keyboard = [
                [InlineKeyboardButton("💰 Оплачен", callback_data=f"admin_set_status_{order_num}_paid")],
                [InlineKeyboardButton("⏳ В обработке", callback_data=f"admin_set_status_{order_num}_processing")],
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
                        sheet = get_sheet()
                        if sheet:
                            values = sheet.get_all_values()
                            for row in values[1:]:
                                if len(row) >= 2 and row[0] == order_num:
                                    user_id = int(row[1])
                                    break
                    except Exception as e:
                        logger.error(f"Ошибка получения user_id из таблицы: {e}")
                
                if user_id:
                    status_messages = {
                        "paid": "💰 Ваша оплата подтверждена! Заказ в обработке.",
                        "processing": "⏳ Ваш заказ обрабатывается. Ожидайте уведомления.",
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

        # === НАЗАД В АДМИН-ПАНЕЛЬ ===
        elif query.data == "back_to_admin":
            if query.from_user.id != ADMIN_ID:
                return
            keyboard = [
                [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
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
                    f"<b>Сервис:</b> {order_info.get('service', 'N/A')}\n"
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
                    [InlineKeyboardButton("✔️ Выполнен", callback_data=f"admin_set_status_{order_number}_completed")],
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

    try:
        if context.user_data.get("awaiting_apple", False):
            text = update.message.text.strip()
            try:
                amount = int(text)
                if not (400 <= amount <= 45000):
                    await update.message.reply_text(
                        "❌ Неверный диапазон.\n\nВведите сумму от 400 до 45 000 KZT:"
                    )
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
                    "❌ Введите только число.\n\nПовторите ввод суммы (400–45 000 KZT):"
                )
                return

        await update.message.reply_text(
            "ℹ️ Напишите /start для начала работы с ботом."
        )
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


# === ЮMONEY WEBHOOK СЕРВЕР ===
_bot_app = None  # глобальная ссылка на приложение бота

async def yoomoney_webhook(request: web.Request) -> web.Response:
    """Обработчик HTTP-уведомлений от ЮMoney"""
    try:
        data = await request.post()

        notification_type = data.get("notification_type", "")
        operation_id     = data.get("operation_id", "")
        amount           = data.get("amount", "")
        currency         = data.get("currency", "")
        datetime_str     = data.get("datetime", "")
        sender           = data.get("sender", "")
        codepro          = data.get("codepro", "")
        label            = data.get("label", "")
        sha1_hash        = data.get("sha1_hash", "")

        # Проверяем подпись
        check_str = "&".join([
            notification_type, operation_id, amount, currency,
            datetime_str, sender, codepro, YOOMONEY_SECRET, label
        ])
        expected_hash = hashlib.sha1(check_str.encode("utf-8")).hexdigest()

        if expected_hash != sha1_hash:
            logger.warning(f"ЮMoney: неверная подпись. label={label}")
            return web.Response(status=400, text="Bad signature")

        logger.info(f"✅ ЮMoney платёж подтверждён: label={label}, amount={amount} ₽, operation_id={operation_id}")

        # Автоматически обновляем статус заказа на "Оплачен"
        if label:
            order_number = label
            update_order_status(order_number, ORDER_STATUSES["paid"])
            logger.info(f"✅ Статус {order_number} автоматически изменён на 'Оплачен'")

        # Уведомляем через бота
        if _bot_app and label:
            order_number = label
            
            # Получаем user_id из ORDER_USER_MAP или из Google Sheets
            user_id = ORDER_USER_MAP.get(order_number)
            if not user_id:
                try:
                    sheet = get_sheet()
                    if sheet:
                        values = sheet.get_all_values()
                        for row in values[1:]:
                            if len(row) >= 2 and row[0] == order_number:
                                user_id = int(row[1])
                                break
                except Exception as e:
                    logger.error(f"Ошибка получения user_id для ЮMoney: {e}")
            
            # Уведомление клиенту
            if user_id:
                try:
                    await _bot_app.bot.send_message(
                        user_id,
                        f"✅ Оплата через ЮMoney получена!\n\n"
                        f"📦 Заказ: <b>{order_number}</b>\n"
                        f"💰 Сумма: <b>{amount} ₽</b>\n\n"
                        f"Статус заказа автоматически изменён на «Оплачен».\n"
                        f"Менеджер активирует ваш заказ в ближайшее время.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления клиенту: {e}")

            # Уведомление админу
            try:
                await _bot_app.bot.send_message(
                    ADMIN_ID,
                    f"💳 Получена оплата через ЮMoney!\n\n"
                    f"📦 Заказ: <b>{order_number}</b>\n"
                    f"💰 Сумма: <b>{amount} ₽</b>\n"
                    f"🆔 Operation ID: <code>{operation_id}</code>\n"
                    f"👤 Отправитель: {sender or 'Анонимно'}\n\n"
                    f"✅ Статус автоматически изменён на «Оплачен».",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления админа о ЮMoney платеже: {e}")

        return web.Response(status=200, text="OK")

    except Exception as e:
        logger.error(f"Ошибка обработки ЮMoney webhook: {e}")
        return web.Response(status=500, text="Error")


async def start_webhook_server():
    """Запускает aiohttp сервер для ЮMoney webhook"""
    web_app = web.Application()
    web_app.router.add_post("/yoomoney/webhook", yoomoney_webhook)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("✅ ЮMoney webhook сервер запущен на порту 8080")
    return runner


if __name__ == "__main__":
    import asyncio
    import signal

    app = ApplicationBuilder().token(TOKEN).build()
    _bot_app = app
    _webhook_runner = None

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
        
        # Останавливаем бота
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            logger.info("✅ Бот остановлен")
        except Exception as e:
            logger.error(f"Ошибка остановки бота: {e}")
        
        # Останавливаем webhook сервер
        if _webhook_runner:
            try:
                await _webhook_runner.cleanup()
                logger.info("✅ Webhook сервер остановлен")
            except Exception as e:
                logger.error(f"Ошибка остановки webhook: {e}")
        
        # Очищаем память
        cleanup_memory()
        logger.info("✅ Завершение работы")

    async def main():
        global _webhook_runner
        
        # Регистрируем обработчики сигналов (для Linux)
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
        except NotImplementedError:
            # Windows не поддерживает add_signal_handler
            pass
        
        _webhook_runner = await start_webhook_server()
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        print("✅ Бот запущен!")
        logger.info("✅ Бот успешно запущен")
        
        try:
            await asyncio.Event().wait()  # держим процесс живым
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
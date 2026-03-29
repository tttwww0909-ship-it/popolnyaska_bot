"""
Конфигурация бота: переменные окружения, логирование, константы.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from telegram import InlineKeyboardButton

# === ЗАГРУЖАЕМ ПЕРЕМЕННЫЕ ИЗ .env ===
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET", "")

# Платёжные реквизиты
OZON_PAY_URL = os.getenv("OZON_PAY_URL", "")
BYBIT_UID = os.getenv("BYBIT_UID", "")
BSC_ADDRESS = os.getenv("BSC_ADDRESS", "")
TRC20_ADDRESS = os.getenv("TRC20_ADDRESS", "")

# Проверяем критические переменные
if not TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN не установлен в .env файле!")
if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID не установлен в .env файле!")

# === ЛОГИРОВАНИЕ ===
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

# Проверяем платёжные реквизиты (предупреждения, не ошибки)
if not YOOMONEY_WALLET:
    logger.warning("⚠️ YOOMONEY_WALLET не установлен в .env")
if not OZON_PAY_URL:
    logger.warning("⚠️ OZON_PAY_URL не установлен в .env")
if not BYBIT_UID:
    logger.warning("⚠️ BYBIT_UID не установлен в .env")
if not BSC_ADDRESS:
    logger.warning("⚠️ BSC_ADDRESS не установлен в .env")
if not TRC20_ADDRESS:
    logger.warning("⚠️ TRC20_ADDRESS не установлен в .env")

# Глушим HTTP-спам
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

# === ТАРИФЫ ===
PRICES = {
    "apple_5000": 5000,
    "apple_10000": 10000,
    "apple_15000": 15000
}

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
    "AE": "🇦🇪 ОАЭ Premium",
    "SA": "🇸🇦 Саудовская Аравия Premium",
}

REGION_COMMISSION = {
    "KZ": 1.15,
    "TR": 1.12,
    "US": 1.15,
    "AE": 1.15,
    "SA": 1.15,
}

# Тексты кнопок с тегами (Solution-Based UI)
GIFT_CARD_LABELS = {
    "TR": {
        25:   "25 TL",
        50:   "50 TL",
        100:  "100 TL  ☁️ iCloud 50GB",
        250:  "250 TL  🔥 Music + Cloud",
        1000: "1 000 TL",
    },
    "US": {
        5:   "$5",
        10:  "$10  🎮 Apps & Games",
        25:  "$25",
        50:  "$50  💎 Premium Packs",
        100: "$100",
        200: "$200",
        300: "$300",
        500: "$500",
    },
}

# Описание региона на экране выбора номинала
REGION_DESCRIPTIONS = {
    "AE": "🌍 <i>Идеально для обхода региональных ограничений.</i>",
    "SA": "🌍 <i>Идеально для обхода региональных ограничений.</i>",
}

# Подсказки «на что хватит» (Check-List п.5)
GIFT_CARD_HINTS = {
    "TR": {
        25:   "небольших покупок в App Store",
        50:   "Apple Music (~1 мес.)",
        100:  "iCloud 50GB + Apple Music (~1 мес.)",
        250:  "Apple Music + iCloud 200GB (~2 мес.)",
        1000: "Apple One Premium (~3 мес.)",
    },
    "US": {
        5:   "небольших покупок в App Store",
        10:  "игровых приложений и мелких покупок",
        25:  "Apple TV+ (~3 мес.)",
        50:  "Apple One (~2 мес.) или Premium-игры",
        100: "Apple One Family (~3 мес.)",
        200: "Apple One Family (~6 мес.)",
        300: "крупных покупок и подписок",
        500: "годовых подписок Apple One",
    },
}

ORDER_STATUSES = {
    "pending": "Ожидает оплаты",
    "paid": "Оплачен",
    "completed": "Выполнен",
    "cancelled": "Отменён"
}

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

FAQ_KEYBOARD = [
    [InlineKeyboardButton("🔹 Как работает сервис?", callback_data="faq_how")],
    [InlineKeyboardButton("🔹 Сколько времени занимает?", callback_data="faq_time")],
    [InlineKeyboardButton("🔹 Способы оплаты", callback_data="faq_payment")],
    [InlineKeyboardButton("🔹 Какая комиссия?", callback_data="faq_commission")],
    [InlineKeyboardButton("🔹 Что делать при проблемах?", callback_data="faq_problems")],
    [InlineKeyboardButton("🔹 Безопасно ли это?", callback_data="faq_safety")]
]

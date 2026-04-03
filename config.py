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
TRC20_ADDRESS = os.getenv("TRC20_ADDRESS", "TWn8rgevKujfC9znz7KS1mNjp3z8SPyRSe")

# CryptoPay (@CryptoBot) — автоматический приём крипто-платежей
CRYPTOPAY_TOKEN = os.getenv("CRYPTOPAY_TOKEN", "")
CRYPTOPAY_WEBHOOK_PATH = os.getenv("CRYPTOPAY_WEBHOOK_PATH", "/cryptopay/webhook")
CRYPTOPAY_WEBHOOK_PORT = int(os.getenv("CRYPTOPAY_WEBHOOK_PORT", "8443"))

# Группа для публикации отзывов
REVIEWS_CHAT_ID = os.getenv("REVIEWS_CHAT_ID", "@popolnyaskachat")

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
if not CRYPTOPAY_TOKEN:
    logger.warning("⚠️ CRYPTOPAY_TOKEN не установлен — автооплата CryptoPay отключена")

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
    "TR": 1.12,
    "US": 1.15,
    "AE": 1.15,
    "SA": 1.15,
}

# === VIP-СКИДКА ===
VIP_DISCOUNT = 0.02           # 2% скидка за крипто-оплату (крупные заказы)
VIP_THRESHOLD = 8500          # Порог для VIP-заказов (₽)

# === РЕФЕРАЛЬНАЯ ПРОГРАММА ===
REF_THRESHOLD = 300          # Минимальный чек (₽) для начисления % бонуса
FIXED_PARTNER_BONUS = 10     # Фикс. бонус партнёру при чеке < REF_THRESHOLD
MAX_BONUS_PAYMENT = 0.50     # Максимальная доля оплаты баллами (50%)

# === ОГРАНИЧЕНИЯ ВВОДА ===
MAX_EMAIL_LENGTH = 100       # Максимальная длина email
MAX_REVIEW_LENGTH = 250      # Максимальная длина комментария к отзыву

# Ступенчатая таблица: комиссия → (% партнёру, % скидка рефералу)
# Ключ — комиссия как множитель (1.20 = 20%)
REFERRAL_RATES = {
    1.20: (0.03, 0.02),   # 20% комиссия → 3% партнёру, 2% скидка
    1.15: (0.03, 0.02),   # 15% → 3%, 2%
    1.12: (0.02, 0.02),   # 12% → 2%, 2%
    1.11: (0.015, 0.01),  # 11% → 1.5%, 1%
}

# Тексты кнопок с тегами (Solution-Based UI)
GIFT_CARD_LABELS = {
    "TR": {
        25:   "25 TL",
        50:   "50 TL",
        100:  "100 TL  ☁️ iCloud 50GB",
        250:  "250 TL  ✨ Выгодно",
        1000: "1 000 TL  ⭐ Премиум",
    },
    "US": {
        5:   "$5",
        10:  "$10  🎮 Apps & Games",
        25:  "$25",
        50:  "$50  💎 Premium",
        100: "$100",
        200: "$200",
        300: "$300",
        500: "$500  🏆 Apple One",
    },
}

# Описание региона на экране выбора номинала
REGION_DESCRIPTIONS = {
    "TR": "🔥 <i>Самый популярный регион для экономии на подписках.</i>",
    "US": "🌎 <i>Регион с самым богатым выбором контента и игр.</i>",
    "AE": "🌍 <i>Регион для обхода санкций и специфического ПО. Идеально для активации приложений, удалённых из других сторов.</i>",
    "SA": "🌍 <i>Регион для обхода санкций и специфического ПО. Идеально для активации приложений, удалённых из других сторов.</i>",
}

# Подсказки «на что хватит» (Check-List п.5)
GIFT_CARD_HINTS = {
    "TR": {
        25:   "1–2 месяца iCloud 50 ГБ. Идеально для разовой проверки сервиса",
        50:   "1–2 месяца iCloud 50 ГБ. Идеально для разовой проверки сервиса",
        100:  "3 месяца iCloud 50 ГБ или 1 месяц Apple Music. Самый частый выбор",
        250:  "💰 Выгодно: полгода iCloud 50 ГБ или несколько популярных приложений",
        1000: "⭐ Премиум: год Apple Music или 6–8 месяцев iCloud 200 ГБ",
    },
    "US": {
        5:   "покупки инди-игр или валюты в мобильных играх (Brawl Stars, Roblox)",
        10:  "покупки инди-игр или валюты в мобильных играх (Brawl Stars, Roblox)",
        25:  "1 месяц Apple One / Arcade или крупное игровое дополнение (DLC)",
        50:  "полноценную игру класса ААА или несколько месяцев профессиональных приложений",
        100: "полноценную игру класса ААА или несколько месяцев Final Cut / Logic Pro",
        200: "годовые подписки или оптовую закупку игровой валюты с максимальной выгодой",
        300: "годовые подписки или оптовую закупку игровой валюты с максимальной выгодой",
        500: "годовую подписку Apple One + запас на покупки",
    },
    "KZ": {
        5000:  "базовый пакет: 2 месяца Apple Music или 3 месяца iCloud 200 ГБ",
        10000: "семейный: Family Sharing на 1–2 месяца — музыка, кино и облако для всех",
        15000: "6–8 месяцев семейных подписок Apple",
    },
    "AE": {"_default": "оплату сервисов, доступных только в арабском регионе. Идеально для активации приложений, удалённых из других сторов"},
    "SA": {"_default": "оплату сервисов, доступных только в арабском регионе. Идеально для активации приложений, удалённых из других сторов"},
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
    [InlineKeyboardButton("🔹 Безопасно ли это?", callback_data="faq_safety")],
    [InlineKeyboardButton("💡 Как выбрать номинал?", callback_data="faq_guide")],
    [InlineKeyboardButton("💳 Как оплатить криптой (USDT)?", callback_data="faq_usdt_guide")]
]

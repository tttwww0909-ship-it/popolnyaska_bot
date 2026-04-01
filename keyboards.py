"""
Переиспользуемые клавиатуры и текстовые блоки.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import BYBIT_UID, BSC_ADDRESS, TRC20_ADDRESS, CRYPTOPAY_TOKEN
from utils import fmt


def region_selection_keyboard():
    """Клавиатура выбора региона Apple ID"""
    return [
        [InlineKeyboardButton("🇺🇸 США", callback_data="region_US")],
        [InlineKeyboardButton("🇹🇷 Турция", callback_data="region_TR")],
        [InlineKeyboardButton("🇰🇿 Казахстан", callback_data="region_KZ")],
        [InlineKeyboardButton("🇦🇪 ОАЭ Premium", callback_data="region_AE")],
        [InlineKeyboardButton("🇸🇦 Саудовская Аравия Premium", callback_data="region_SA")]
    ]


def admin_panel_keyboard():
    """Клавиатура админ-панели"""
    return [
        [InlineKeyboardButton("📊 Общая статистика", callback_data="stats_general")],
        [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
        [InlineKeyboardButton("🔄 Изменить статус заказа", callback_data="admin_manage_orders")]
    ]


def rating_keyboard(order_num: str):
    """Клавиатура оценки сервиса (1-5 звёзд + пропуск)"""
    return [
        [
            InlineKeyboardButton("1⭐️", callback_data=f"review_rate_1_{order_num}"),
            InlineKeyboardButton("2⭐️", callback_data=f"review_rate_2_{order_num}"),
            InlineKeyboardButton("3⭐️", callback_data=f"review_rate_3_{order_num}"),
            InlineKeyboardButton("4⭐️", callback_data=f"review_rate_4_{order_num}"),
            InlineKeyboardButton("5⭐️", callback_data=f"review_rate_5_{order_num}"),
        ],
        [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"review_skip_{order_num}")]
    ]


def payment_buttons(order_number: str, is_large_order: bool):
    """Кнопки выбора способа оплаты"""
    if is_large_order:
        return [
            [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
            [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
        ]
    return [
        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
    ]


def crypto_payment_text(order_number: str, amount_usdt, amount_rub=None, is_vip=False):
    """Текст инструкции по крипто-оплате"""
    title = "💎 VIP-оплата криптой (USDT)" if is_vip else "💎 Оплата криптой (USDT)"
    if amount_rub:
        sum_line = f"💰 К оплате: <b>{amount_usdt} USDT</b> ({fmt(amount_rub)} ₽)"
    else:
        sum_line = f"💰 К оплате: <b>{amount_usdt} USDT</b>"
    return (
        f"{title}\n\n"
        f"📦 Заказ: <b>{order_number}</b>\n"
        f"{sum_line}\n\n"
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
        f"После перевода нажмите «✅ Я оплатил» и отправьте скриншот подтверждения."
    )


def vip_promo_text(order_number: str, saving: int, rub_discounted: int, usdt_suffix: str):
    """Текст VIP-промо для крупных заказов"""
    return (
        f"💎 <b>Крупный заказ — особые условия!</b>\n\n"
        f"Сумма вашего заказа превышает 8 500 ₽. Для обеспечения максимальной "
        f"безопасности и скорости обработки крупные платежи принимаются в USDT.\n\n"
        f"<b>Ваши преимущества:</b>\n"
        f"✅ Скидка 2% — вы экономите <b>{fmt(saving)} ₽</b>\n"
        f"✅ Итоговая сумма: <b>{fmt(rub_discounted)} ₽</b>{usdt_suffix}\n"
        f"✅ Приоритетная выдача кода\n"
        f"✅ Отсутствие рисков блокировки банком\n\n"
        f"<i>Нет криптокошелька? Оператор поможет за 2 минуты.</i>"
    )


def vip_promo_keyboard(order_number: str):
    """Клавиатура VIP-промо"""
    return [
        [InlineKeyboardButton("💎 Оплатить криптой (−2%)", callback_data=f"vip_crypto_{order_number}")],
        [InlineKeyboardButton("📱 Как купить USDT за 2 мин?", callback_data="vip_usdt_guide")],
        [InlineKeyboardButton("💬 Связаться с оператором", url="https://t.me/popolnyaska_halper")],
    ]


USDT_GUIDE_TEXT = (
    "💳 <b>Как оплатить через Telegram Wallet (за 2 минуты)</b>\n\n"
    "Оплата заказов от 8 500 ₽ производится в USDT. Это безопасный способ "
    "оплаты картой любого банка через внутренний сервис Telegram.\n\n"
    "➕ <b>Шаг 1. Откройте кошелёк</b>\n"
    "1. В поиске Telegram найдите @wallet\n"
    "2. Нажмите «Начать» / «Открыть кошелёк»\n\n"
    "➕ <b>Шаг 2. Покупка USDT (P2P Маркет)</b>\n"
    "<i>Это покупка крипты у другого человека переводом по карте, под защитой Telegram.</i>\n"
    "1. В меню кошелька → «P2P Маркет» → «Купить»\n"
    "2. Выберите <b>USDT</b>, введите сумму заказа в рублях\n"
    "3. Выберите удобный банк (Сбер, Т-Банк и др.)\n"
    "4. Фильтр: продавец с рейтингом <b>95%+</b> сделок\n"
    "5. Нажмите «Купить» и подтвердите сделку\n\n"
    "➕ <b>Шаг 3. Оплата продавцу</b>\n"
    "1. Бот покажет реквизиты продавца (номер карты / тел.)\n"
    "2. Переведите указанную сумму <b>в рублях</b> со своей карты\n"
    "3. Нажмите «Оплачено» в боте — USDT зачислятся на кошелёк\n\n"
    "➕ <b>Шаг 4. Перевод USDT продавцу</b>\n"
    "1. В кошельке → «Отправить» → введите адрес и сумму USDT\n"
    "2. Подтвердите перевод — готово!"
)


def cryptopay_enabled() -> bool:
    return bool(CRYPTOPAY_TOKEN)


def crypto_payment_buttons(order_number: str, pay_url: str | None = None):
    """Кнопки крипто-оплаты: CryptoPay (если есть) + ручной перевод."""
    buttons = []
    if pay_url:
        buttons.append([InlineKeyboardButton("⚡ Оплатить через CryptoPay", url=pay_url)])
        buttons.append([InlineKeyboardButton("💼 Оплатить вручную (Bybit/TRC20)", callback_data=f"pay_crypto_manual_{order_number}")])
    else:
        buttons.append([InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_crypto_{order_number}")])
    buttons.append([InlineKeyboardButton("⬅️ Назад к способам оплаты", callback_data="back_to_payment")])
    return buttons


def cryptopay_invoice_text(order_number: str, amount_usdt: float, amount_rub=None, is_vip=False):
    """Текст для CryptoPay-оплаты."""
    title = "💎 VIP-оплата криптой" if is_vip else "💎 Оплата криптой (USDT)"
    if amount_rub:
        sum_line = f"💰 К оплате: <b>{amount_usdt} USDT</b> ({fmt(amount_rub)} ₽)"
    else:
        sum_line = f"💰 К оплате: <b>{amount_usdt} USDT</b>"
    return (
        f"{title}\n\n"
        f"📦 Заказ: <b>{order_number}</b>\n"
        f"{sum_line}\n\n"
        f"Нажмите кнопку ниже для автоматической оплаты через @CryptoBot.\n"
        f"Оплата подтверждается <b>мгновенно</b> — скриншот не нужен.\n\n"
        f"💡 Если у вас нет @CryptoBot — выберите «Оплатить вручную»."
    )

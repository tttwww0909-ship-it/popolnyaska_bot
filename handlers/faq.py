"""
FAQ: часто задаваемые вопросы.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import FAQ_KEYBOARD, VIP_THRESHOLD
from utils import fmt

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# CALLBACK HANDLERS
# ═══════════════════════════════════════════════

async def handle_faq_menu(query, context):
    await query.edit_message_text(
        "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
        reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
    )


async def handle_faq_how(query, context):
    await query.edit_message_text(
        "🔹 Как работает сервис?\n\n"
        "Мы помогаем гражданам РФ пополнить Apple ID методом смены региона.\n\n"
        "🇰🇿 <b>Казахстан</b> — вы отправляете почту, привязанную к Apple ID, "
        "а мы отправляем подарочный код на вашу почту для пополнения баланса.\n\n"
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


async def handle_faq_time(query, context):
    await query.edit_message_text(
        "🔹 Сколько времени занимает?\n\n"
        "🇰🇿 Пополнение Apple ID (Казахстан) — до 30 минут после подтверждения оплаты.\n\n"
        "🎁 Gift Card (США, ОАЭ, Турция, Сауд. Аравия) — до 15 минут. "
        "Бот отправит вам код для пополнения Apple ID.\n\n"
        "В редких случаях может занять больше времени из-за высокой нагрузки.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
    )


async def handle_faq_payment(query, context):
    await query.edit_message_text(
        "🔹 Какие способы оплаты доступны?\n\n"
        "• ЮMoney (пополнение кошелька)\n"
        "• OZON банк (перевод по ссылке)\n"
        "• Криптовалюта:\n"
        "  — Bybit (перевод по UID)\n"
        "  — CryptoPay (@CryptoBot)\n"
        "  — Telegram Wallet (TRC20)\n\n"
        f"⚠️ Для заказов свыше {fmt(VIP_THRESHOLD)} ₽ доступна только оплата криптой.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
    )


async def handle_faq_commission(query, context):
    await query.edit_message_text(
        "🔹 Какая комиссия сервиса?\n\n"
        "Комиссия зависит от региона и суммы:\n\n"
        "🇺🇸 США — 15% (до $50) / 12% ($51–$300) / 11% (свыше $300)\n"
        "🇹🇷 Турция — 12%\n"
        "🇰🇿 Казахстан — 20% (до 10к) / 15% (10к–30к) / 12% (свыше 30к)\n"
        "🇦🇪 ОАЭ Premium — 15%\n"
        "🇸🇦 Саудовская Аравия Premium — 15%",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
    )


async def handle_faq_problems(query, context):
    await query.edit_message_text(
        "🔹 Что делать, если возникли проблемы?\n\n"
        "Напишите нам — кнопка «Написать менеджеру» доступна в заказе, "
        "или свяжитесь напрямую с поддержкой.\n\n"
        "📧 Для жалоб, предложений, сотрудничества и запросов в службу поддержки:\n"
        "<code>popolnyaskaservice@icloud.com</code>\n\n"
        "⏳ Срок ожидания ответа на эл. почту — до 15 дней.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📞 Написать в поддержку", url="https://t.me/popolnyaska_halper")],
            [InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]
        ]),
        parse_mode="HTML"
    )


async def handle_faq_safety(query, context):
    await query.edit_message_text(
        "🔹 Безопасно ли это?\n\n"
        "Да! Это абсолютно безопасно. Мы используем минимум личной информации "
        "(UserID, username и информацию о заказе) для статистики, а вашу почту Apple ID "
        "используем только для пополнения и не сохраняем!\n\n"
        "Ваши данные в безопасности!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
    )


async def handle_faq_usdt_guide(query, context):
    from config import BYBIT_UID, TRC20_ADDRESS
    await query.edit_message_text(
        "💳 <b>Как оплатить криптой (USDT)</b>\n\n"
        f"Оплата заказов от {fmt(VIP_THRESHOLD)} ₽ производится в USDT.\n\n"
        "➕ <b>Способ 1: CryptoPay (автоматически)</b>\n"
        "1. Нажмите кнопку «⚡ Оплатить через CryptoPay»\n"
        "2. Оплата подтверждается мгновенно — скриншот не нужен\n\n"
        "➕ <b>Способ 2: Bybit (перевод по UID)</b>\n"
        f"1. Откройте Bybit → «Перевод» → «Bybit UID»\n"
        f"2. Введите UID: <code>{BYBIT_UID}</code>\n"
        "3. Укажите сумму USDT и подтвердите\n"
        "4. Отправьте скриншот подтверждения в этот чат\n\n"
        "➕ <b>Способ 3: Telegram Wallet (TRC20)</b>\n"
        "1. Откройте @wallet в Telegram\n"
        "2. Выберите USDT → «Отправить» → «На адрес»\n"
        f"3. Вставьте адрес: <code>{TRC20_ADDRESS}</code>\n"
        "4. Выберите сеть <b>TRC20 (Tron)</b>\n"
        "5. Укажите сумму и подтвердите\n"
        "6. Отправьте скриншот подтверждения в этот чат\n\n"
        "✅ <b>Готово!</b> Оператор выдаст заказ в кратчайшие сроки.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]]),
        parse_mode="HTML"
    )


async def handle_vip_usdt_guide(query, context):
    from config import BYBIT_UID, TRC20_ADDRESS
    await query.edit_message_text(
        "💳 <b>Как оплатить криптой (USDT)</b>\n\n"
        f"Оплата заказов от {fmt(VIP_THRESHOLD)} ₽ производится в USDT.\n\n"
        "➕ <b>Способ 1: CryptoPay (автоматически)</b>\n"
        "1. Нажмите кнопку «⚡ Оплатить через CryptoPay»\n"
        "2. Оплата подтверждается мгновенно — скриншот не нужен\n\n"
        "➕ <b>Способ 2: Bybit (перевод по UID)</b>\n"
        f"1. Откройте Bybit → «Перевод» → «Bybit UID»\n"
        f"2. Введите UID: <code>{BYBIT_UID}</code>\n"
        "3. Укажите сумму USDT и подтвердите\n"
        "4. Отправьте скриншот подтверждения в этот чат\n\n"
        "➕ <b>Способ 3: Telegram Wallet (TRC20)</b>\n"
        "1. Откройте @wallet в Telegram\n"
        "2. Выберите USDT → «Отправить» → «На адрес»\n"
        f"3. Вставьте адрес: <code>{TRC20_ADDRESS}</code>\n"
        "4. Выберите сеть <b>TRC20 (Tron)</b>\n"
        "5. Укажите сумму и подтвердите\n"
        "6. Отправьте скриншот подтверждения в этот чат\n\n"
        "✅ <b>Готово!</b> Оператор выдаст заказ в кратчайшие сроки.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_to_vip_promo")]]),
        parse_mode="HTML"
    )


async def handle_faq_guide(query, context):
    await query.edit_message_text(
        "💡 Не знаете, сколько купить? Мы подскажем!\n\n"
        "☁️ <b>Для базовых нужд (iCloud 50 ГБ):</b>\n"
        "Турция 100–250 TL или Казахстан 5 000 KZT.\n"
        "Хватит на несколько месяцев спокойного пользования.\n\n"
        "👨‍👩‍👧‍👦 <b>Для семьи (Family Sharing):</b>\n"
        "Казахстан от 10 000 KZT.\n"
        "Одной оплаты хватит для всех участников семьи.\n\n"
        "🎮 <b>Для геймеров:</b>\n"
        "США от $25 или Турция от 500 TL.\n"
        "Хватит на Battle Pass или крупный пак валюты.\n\n"
        "💼 <b>Для профи (Final Cut, Logic Pro):</b>\n"
        "США от $100 — доступ к профессиональному софту без ограничений.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]]),
        parse_mode="HTML"
    )


async def handle_back_to_faq(query, context):
    await query.edit_message_text(
        "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
        reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
    )

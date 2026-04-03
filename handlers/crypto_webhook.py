"""
CryptoPay webhook: обработка автоматических платежей.
"""

import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import ADMIN_ID, ORDER_STATUSES, REGION_DISPLAY
from utils import (
    fmt, ORDER_USER_MAP, ORDER_INFO_MAP, AWAITING_EMAIL,
)
from sheets import update_order_status
from database import db

logger = logging.getLogger(__name__)


async def handle_cryptopay_webhook(bot, payload: dict):
    """Обрабатывает вебхук от CryptoPay при успешной оплате.

    Автоматически подтверждает оплату: обновляет статус, уведомляет клиента и админа.
    Идемпотентен: повторный webhook с тем же invoice_id игнорируется.
    """
    if payload.get("update_type") != "invoice_paid":
        return

    invoice = payload.get("payload", {})
    order_number = invoice.get("payload")  # мы передали order_number как payload при создании
    if not order_number:
        logger.warning("CryptoPay webhook: no order_number in payload")
        return

    invoice_id = str(invoice.get("invoice_id", ""))
    amount = invoice.get("amount", "?")
    asset = invoice.get("asset", "USDT")

    # Идемпотентность: проверяем, не обработан ли уже этот invoice
    if invoice_id:
        existing = await asyncio.to_thread(db.get_order_by_payment_id, invoice_id)
        if existing:
            logger.info(f"CryptoPay webhook: invoice {invoice_id} уже обработан, пропускаем")
            return

    logger.info(f"CryptoPay: оплата получена — заказ {order_number}, {amount} {asset}")

    # Записываем invoice_id в payments для идемпотентности
    order = await asyncio.to_thread(db.get_order, order_number)
    if order and invoice_id:
        await asyncio.to_thread(db.add_payment, invoice_id, order["id"], float(amount) if amount != "?" else 0)

    # Обновляем статус на "Оплачен"
    status_name = ORDER_STATUSES.get("paid", "Оплачен")
    success = await asyncio.to_thread(update_order_status, order_number, status_name)
    if not success:
        logger.error(f"CryptoPay webhook: не удалось обновить статус для {order_number}")
        return

    # Ищем user_id
    user_id = ORDER_USER_MAP.get(order_number)
    order_info = ORDER_INFO_MAP.get(order_number, {})
    if not user_id:
        user_id = order_info.get("user_id")
    if not user_id:
        user_id = await asyncio.to_thread(db.get_telegram_id_for_order, order_number)

    order_region = order_info.get("region", "")
    is_gift_card = order_region in ("TR", "US", "AE", "SA")

    # Уведомляем клиента
    if user_id:
        try:
            if is_gift_card:
                await bot.send_message(
                    user_id,
                    "✅ <b>Оплата подтверждена автоматически!</b>\n\n"
                    f"📦 Заказ: <b>{order_number}</b>\n"
                    f"💰 Сумма: <b>{amount} {asset}</b>\n\n"
                    "⏳ Ожидайте получения кода — бот отправит его вам.\n\n"
                    "⚠️ <b>Обратите внимание:</b> после получения кода средства возврату не подлежат.",
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(
                    user_id,
                    "✅ <b>Оплата подтверждена автоматически!</b>\n\n"
                    f"📦 Заказ: <b>{order_number}</b>\n"
                    f"💰 Сумма: <b>{amount} {asset}</b>\n\n"
                    "📧 Теперь отправьте вашу почту Apple ID для пополнения:",
                    parse_mode="HTML"
                )
                AWAITING_EMAIL[user_id] = order_number
        except Exception as e:
            logger.error(f"CryptoPay: ошибка уведомления клиента {user_id}: {e}")

    # Уведомляем админа
    try:
        first_name = order_info.get("first_name", "—")
        username = order_info.get("username", "—")
        tariff = order_info.get("tariff", "—")
        region = REGION_DISPLAY.get(order_region, order_region or "—")

        admin_text = (
            f"⚡ <b>CryptoPay: автоплатёж</b>\n\n"
            f"📦 Заказ: <b>{order_number}</b>\n"
            f"👤 Клиент: {first_name} (@{username})\n"
            f"🌍 Регион: {region}\n"
            f"📱 Тариф: {tariff}\n"
            f"💰 Сумма: <b>{amount} {asset}</b>\n"
            f"📊 Статус: <b>{status_name}</b>"
        )

        admin_buttons = []
        if is_gift_card and user_id:
            admin_buttons.append([InlineKeyboardButton("📤 Отправить код клиенту", callback_data=f"send_code_{order_number}_{user_id}")])
        if user_id:
            admin_buttons.append([InlineKeyboardButton("💬 Связаться с клиентом", url=f"tg://user?id={user_id}")])

        await bot.send_message(
            ADMIN_ID,
            admin_text,
            reply_markup=InlineKeyboardMarkup(admin_buttons) if admin_buttons else None,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"CryptoPay: ошибка уведомления админа: {e}")

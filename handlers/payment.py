"""
Оплата: ЮMoney, OZON, крипта, VIP-крипта, скриншоты, фото-хэндлер.
"""

import asyncio
import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    ADMIN_ID, YOOMONEY_WALLET, OZON_PAY_URL,
    REGION_DISPLAY, ORDER_STATUSES,
    VIP_DISCOUNT, VIP_THRESHOLD, MAX_BONUS_PAYMENT,
)
from utils import (
    fmt, esc, get_usdt_rate,
    ORDER_INFO_MAP,
    AWAITING_SCREENSHOT,
)
from keyboards import (
    payment_buttons, crypto_payment_text, vip_promo_text, vip_promo_keyboard,
    crypto_payment_buttons, cryptopay_invoice_text,
)
from sheets import update_payment_method, update_order_status, update_order_amount_in_sheet
from database import db
from handlers.common import (
    _safe_edit, _get_order_data, _cryptopay,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# VIP HELPER
# ═══════════════════════════════════════════════

async def _proceed_vip_crypto(query, context, order_number: str, rub_final: int):
    """Общий финал VIP крипто-оплаты."""
    usdt_rate = await asyncio.to_thread(get_usdt_rate)
    amount_usdt = round(rub_final / usdt_rate, 2) if usdt_rate else context.user_data.get("amount_usdt", 0)
    context.user_data["amount_usdt"] = amount_usdt

    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]["rub"] = rub_final
        ORDER_INFO_MAP[order_number]["usdt"] = amount_usdt
    await asyncio.to_thread(update_order_amount_in_sheet, order_number, rub_final)

    pay_url = None
    if _cryptopay and amount_usdt:
        try:
            invoice = await _cryptopay.create_invoice(amount_usdt, order_number, description=f"VIP заказ {order_number}")
            pay_url = invoice.get("mini_app_invoice_url") or invoice.get("pay_url")
            context.user_data["cryptopay_invoice_id"] = invoice.get("invoice_id")
        except Exception as e:
            logger.warning("CryptoPay invoice failed (VIP), fallback to manual: %s", e)

    if pay_url:
        await query.edit_message_text(
            cryptopay_invoice_text(order_number, amount_usdt, amount_rub=rub_final, is_vip=True),
            reply_markup=InlineKeyboardMarkup(crypto_payment_buttons(order_number, pay_url, is_vip=True)),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            crypto_payment_text(order_number, amount_usdt, amount_rub=rub_final, is_vip=True),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_crypto_{order_number}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_vip_promo")]
            ]),
            parse_mode="HTML"
        )
    await asyncio.to_thread(update_payment_method, order_number, "Crypto (VIP)")
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]['payment_method'] = 'Crypto (VIP)'
    logger.info(f"Клиент {query.from_user.id} — VIP крипто-оплата для {order_number}, сумма {rub_final}₽ ({amount_usdt} USDT)")


# ═══════════════════════════════════════════════
# ОПЛАТА ЮMONEY
# ═══════════════════════════════════════════════

async def handle_pay_yoomoney(query, context):
    order_number = query.data.replace("pay_yoomoney_", "")
    order = _get_order_data(context, order_number)
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
    await asyncio.to_thread(update_payment_method, order_number, "ЮMoney")
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]['payment_method'] = 'ЮMoney'
    logger.info(f"Клиент {query.from_user.id} выбрал ЮMoney для {order_number}")


# ═══════════════════════════════════════════════
# ОПЛАТА OZON
# ═══════════════════════════════════════════════

async def handle_pay_ozon(query, context):
    order_number = query.data.replace("pay_ozon_", "")
    order = _get_order_data(context, order_number)
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
    await asyncio.to_thread(update_payment_method, order_number, "OZON банк")
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]['payment_method'] = 'OZON банк'
    logger.info(f"Клиент {query.from_user.id} выбрал OZON банк для {order_number}")


# ═══════════════════════════════════════════════
# VIP
# ═══════════════════════════════════════════════

async def handle_vip_decline(query, context):
    order_number = query.data.replace("vip_decline_", "")
    await asyncio.to_thread(update_order_status, order_number, ORDER_STATUSES["cancelled"])
    context.user_data.pop("vip_order_number", None)
    context.user_data.pop("rub_discounted", None)
    context.user_data.pop("order", None)
    context.user_data.pop("current_order_number", None)
    if order_number in ORDER_INFO_MAP:
        del ORDER_INFO_MAP[order_number]
    await query.edit_message_text(
        f"❌ Заказ <b>{order_number}</b> отменён.\n\n"
        f"Вы всегда можете создать новый заказ. Если есть вопросы — "
        f"напишите в поддержку.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🍏 Новый заказ", callback_data="apple_topup")],
            [InlineKeyboardButton("📞 Написать в поддержку", url="https://t.me/popolnyaska_halper")],
        ])
    )
    logger.info(f"Клиент {query.from_user.id} отказался от VIP-заказа {order_number}")


async def handle_vip_crypto(query, context):
    order_number = query.data.replace("vip_crypto_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
    context.user_data["rub_discounted"] = rub_discounted

    user_id = query.from_user.id
    bonus_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
    if bonus_balance > 0:
        max_bonus = int(rub_discounted * MAX_BONUS_PAYMENT)
        usable = min(int(bonus_balance), max_bonus)
        if usable >= 1:
            usdt_rate = await asyncio.to_thread(get_usdt_rate)
            usdt_suffix = f" (~{round(rub_discounted / usdt_rate, 2)} USDT)" if usdt_rate else ""
            after_bonus = rub_discounted - usable
            usdt_after = f" (~{round(after_bonus / usdt_rate, 2)} USDT)" if usdt_rate else ""
            await query.edit_message_text(
                f"🎁 <b>У вас есть бонусные баллы!</b>\n\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"💎 Сумма со скидкой 2%: <b>{fmt(rub_discounted)} ₽</b>{usdt_suffix}\n"
                f"💰 Баланс баллов: <b>{fmt(int(bonus_balance))}</b>\n"
                f"📝 Можно списать: <b>{fmt(usable)} ₽</b>\n"
                f"✅ Итого после списания: <b>{fmt(after_bonus)} ₽</b>{usdt_after}\n\n"
                f"Списать баллы?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"🎁 Списать {fmt(usable)} баллов", callback_data=f"use_bonus_vip_{order_number}")],
                    [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"skip_bonus_vip_{order_number}")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_vip_promo")],
                ]),
                parse_mode="HTML"
            )
            return

    await _proceed_vip_crypto(query, context, order_number, rub_discounted)


async def handle_use_bonus_vip(query, context):
    order_number = query.data.replace("use_bonus_vip_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    user_id = query.from_user.id
    rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
    bonus_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
    max_bonus = int(rub_discounted * MAX_BONUS_PAYMENT)
    usable = min(int(bonus_balance), max_bonus)
    if usable < 1:
        await query.answer("Недостаточно баллов для списания.", show_alert=True)
        return
    spent = await asyncio.to_thread(
        db.spend_bonus, user_id, usable, order_number,
        f"Оплата VIP-заказа {order_number}"
    )
    if not spent:
        await query.answer("Ошибка списания баллов.", show_alert=True)
        return
    new_rub = rub_discounted - usable
    context.user_data["rub_discounted"] = new_rub
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]["bonus_used"] = usable
    logger.info(f"Пользователь {user_id} списал {usable} баллов для VIP-заказа {order_number}")
    await _proceed_vip_crypto(query, context, order_number, new_rub)


async def handle_skip_bonus_vip(query, context):
    order_number = query.data.replace("skip_bonus_vip_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
    await _proceed_vip_crypto(query, context, order_number, rub_discounted)


async def handle_back_to_vip_promo(query, context):
    order_number = context.user_data.get("vip_order_number")
    if not order_number:
        await query.answer("Сессия истекла. Начните заказ заново.", show_alert=True)
        return
    order = _get_order_data(context, order_number)
    if not order:
        await query.answer("Сессия истекла. Начните заказ заново.", show_alert=True)
        return
    usdt_rate = await asyncio.to_thread(get_usdt_rate)
    rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
    saving = order["rub"] - rub_discounted
    usdt_suffix = f" (~{round(rub_discounted / usdt_rate, 2)} USDT)" if usdt_rate else ""
    await query.edit_message_text(
        vip_promo_text(order_number, saving, rub_discounted, usdt_suffix),
        reply_markup=InlineKeyboardMarkup(vip_promo_keyboard(order_number)),
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════
# КРИПТО-ОПЛАТА
# ═══════════════════════════════════════════════

async def handle_pay_crypto(query, context):
    order_number = query.data.replace("pay_crypto_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    amount_usdt = context.user_data.get("amount_usdt", 0)

    pay_url = None
    if _cryptopay and amount_usdt:
        try:
            invoice = await _cryptopay.create_invoice(amount_usdt, order_number)
            pay_url = invoice.get("mini_app_invoice_url") or invoice.get("pay_url")
            context.user_data["cryptopay_invoice_id"] = invoice.get("invoice_id")
        except Exception as e:
            logger.warning("CryptoPay invoice failed, fallback to manual: %s", e)

    if pay_url:
        await query.edit_message_text(
            cryptopay_invoice_text(order_number, amount_usdt),
            reply_markup=InlineKeyboardMarkup(crypto_payment_buttons(order_number, pay_url)),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            crypto_payment_text(order_number, amount_usdt),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_crypto_{order_number}")],
                [InlineKeyboardButton("⬅️ Назад к способам оплаты", callback_data="back_to_payment")]
            ]),
            parse_mode="HTML"
        )
    await asyncio.to_thread(update_payment_method, order_number, "Crypto")
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]['payment_method'] = 'Crypto'
    logger.info(f"Клиент {query.from_user.id} выбрал крипто-оплату для {order_number}")


async def handle_pay_crypto_manual(query, context):
    order_number = query.data.replace("pay_crypto_manual_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    amount_usdt = context.user_data.get("amount_usdt", 0)
    is_vip = context.user_data.get("vip_order_number") == order_number
    back_callback = "back_to_vip_promo" if is_vip else "back_to_payment"
    await query.edit_message_text(
        crypto_payment_text(order_number, amount_usdt, amount_rub=order.get("rub") if is_vip else None, is_vip=is_vip),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_crypto_{order_number}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)]
        ]),
        parse_mode="HTML"
    )
    logger.info(f"Клиент {query.from_user.id} выбрал ручную крипто-оплату для {order_number}")


# ═══════════════════════════════════════════════
# ПОДТВЕРЖДЕНИЕ ОПЛАТЫ
# ═══════════════════════════════════════════════

async def handle_paid(query, context):
    if query.data.startswith("paid_crypto_"):
        order_number = query.data.replace("paid_crypto_", "")
        pay_label = "Crypto"
    elif query.data.startswith("paid_yoomoney_"):
        order_number = query.data.replace("paid_yoomoney_", "")
        pay_label = "ЮMoney"
    else:
        order_number = query.data.replace("paid_ozon_", "")
        pay_label = "OZON банк"

    user_id = query.from_user.id
    AWAITING_SCREENSHOT[user_id] = order_number

    order_info = ORDER_INFO_MAP.get(order_number, {})
    region_display = REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', '—'))

    if pay_label == "Crypto":
        amount_usdt = context.user_data.get("amount_usdt", 0)
        sum_display = f"<b>{amount_usdt} USDT</b>"
    else:
        amount_rub = order_info.get("rub", 0)
        sum_display = f"<b>{fmt(amount_rub)} ₽</b>"

    await query.edit_message_text(
        f"📸 <b>Отправьте скриншот оплаты</b>\n\n"
        f"Для завершения оформления заказа отправьте скриншот "
        f"подтверждения оплаты прямо в этот чат.\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 Заказ: <b>{order_number}</b>\n"
        f"🌍 Регион: <b>{region_display}</b>\n"
        f"📱 Тариф: <b>{order_info.get('tariff', '—')}</b>\n"
        f"💰 Сумма: {sum_display}\n"
        f"💳 Оплата: <b>{pay_label}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"⏳ После получения скриншота заявка будет отправлена менеджеру на проверку.",
        parse_mode="HTML"
    )
    logger.info(f"Клиент {user_id} нажал 'Я оплатил' ({pay_label}) для {order_number}")


async def handle_help_payment(query, context):
    keyboard = [
        [InlineKeyboardButton("📞 Написать в поддержку", url="https://t.me/popolnyaska_halper")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_payment")]
    ]
    await query.edit_message_text(
        "❓ <b>Краткий FAQ</b>\n\n"
        "💳 <b>Способы оплаты:</b>\n"
        "• ЮMoney (пополнение кошелька)\n"
        "• OZON банк (перевод по ссылке)\n"
        "• Криптовалюта:\n"
        "  — Bybit (перевод по UID)\n"
        "  — CryptoPay (@CryptoBot)\n"
        "  — Telegram Wallet (TRC20)\n\n"
        f"⚠️ Для заказов свыше {fmt(VIP_THRESHOLD)} ₽ доступна только оплата криптой.\n\n"
        "⏱ <b>Сроки:</b>\n"
        "🇰🇿 Казахстан — до 30 минут | 🎁 Gift Card — до 15 минут\n\n"
        "❓ <b>Проблемы с оплатой?</b>\n"
        "Свяжитесь с поддержкой через кнопку ниже.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def handle_back_to_payment(query, context):
    order_number = context.user_data.get("current_order_number")
    if not order_number:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Начните снова.")
        return
    order = _get_order_data(context, order_number)
    amount_usdt = context.user_data.get("amount_usdt", 0)
    if order:
        pay_btns = payment_buttons(order_number, is_large_order=(order['rub'] > VIP_THRESHOLD))
        usdt_suffix = f" (~{amount_usdt} USDT)" if amount_usdt else ""
        await query.edit_message_text(
            f"✅ Заявка сформирована!\n\n"
            f"Номер заказа: <b>{order_number}</b>\n"
            f"Тариф: <b>{order['tariff']}</b>\n"
            f"Сумма: <b>{fmt(order['rub'])} ₽</b>{usdt_suffix}\n\n"
            f"Выберите способ оплаты:",
            reply_markup=InlineKeyboardMarkup(pay_btns),
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            f"Номер заказа: <b>{order_number}</b>\n\nДля повторной оплаты создайте новый заказ.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start")]]),
            parse_mode="HTML"
        )


async def handle_resend_screenshot(query, context):
    order_number = query.data.replace("resend_screenshot_", "")
    user_id = query.from_user.id
    resend_counts = context.user_data.setdefault("screenshot_resends", {})
    attempts = resend_counts.get(order_number, 0)
    if attempts >= 3:
        await query.answer("Превышен лимит попыток (3). Обратитесь к оператору.", show_alert=True)
        return
    resend_counts[order_number] = attempts + 1
    AWAITING_SCREENSHOT[user_id] = order_number
    await query.edit_message_text(
        f"📸 <b>Отправьте новый скриншот</b>\n\n"
        f"Заказ: <b>{order_number}</b>\n\n"
        f"Отправьте скриншот подтверждения оплаты прямо в этот чат.\n"
        f"<i>Попытка {attempts + 1} из 3</i>",
        parse_mode="HTML"
    )
    logger.info(f"Клиент {user_id} запросил переотправку скриншота для {order_number} (попытка {attempts + 1}/3)")


# ═══════════════════════════════════════════════
# СВЯЗЬ С МЕНЕДЖЕРОМ
# ═══════════════════════════════════════════════

async def handle_contact_manager(query, context):
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name or "Клиент"
    first_name = query.from_user.first_name or "Клиент"

    await query.edit_message_text(
        "📞 Запрос отправлен!\n\n"
        "Менеджер напишет вам в личку в течение 5 минут.\n"
        "Оставайтесь в Telegram.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_payment")]])
    )

    username_link = f'<a href="https://t.me/{esc(username)}">@{esc(username)}</a>' if query.from_user.username else esc(first_name)
    client_info = f'Имя: <a href="tg://user?id={user_id}">{esc(first_name)}</a>\n' \
                 f'Ник: {username_link}\n' \
                 f'ID: <code>{user_id}</code>'

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
        user_orders = await asyncio.to_thread(db.get_user_orders_by_telegram_id, user_id)
        if user_orders:
            last = user_orders[0]
            client_info += f"\n\n<b>📦 Последний заказ:</b>\n" \
                          f"Номер: <b>{last.get('order_number', 'N/A')}</b>\n" \
                          f"Тариф: <b>{last.get('tariff', 'N/A')}</b>\n" \
                          f"Сумма: <b>{last.get('amount_rub', 'N/A')} ₽</b>\n" \
                          f"Статус: <b>{last.get('status', 'N/A')}</b>"
        else:
            client_info += "\n\n📦 Заказов не найдено"

    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"📞 Клиент запросил связь с менеджером\n\n"
            f"<b>👤 Информация о клиенте:</b>\n{client_info}",
            parse_mode="HTML"
        )
        logger.info(f"Клиент {user_id} запросил связь с менеджером")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о запросе связи: {e}")


# ═══════════════════════════════════════════════
# ФОТО (СКРИНШОТЫ)
# ═══════════════════════════════════════════════

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка скриншотов от клиентов"""
    user_id = update.message.from_user.id

    try:
        order_number = AWAITING_SCREENSHOT.get(user_id)
        if not order_number:
            return

        order_info = ORDER_INFO_MAP.get(order_number, {})
        is_crypto = order_info.get('payment_method') == 'Crypto'
        if is_crypto:
            amount_usdt = order_info.get('usdt', '?')
            sum_line = f"{amount_usdt} USDT"
            payment_line = "💎 Криптой (USDT)"
        else:
            sum_line = f"{fmt(order_info.get('rub', 0))} ₽"
            payment_line = order_info.get('payment_method', '—')

        try:
            await context.bot.send_photo(
                ADMIN_ID,
                photo=update.message.photo[-1].file_id,
                caption=(
                    f"📸 <b>Скриншот оплаты!</b>\n\n"
                    f"<b>📦 Заказ:</b> {order_number}\n"
                    f"<b>Регион:</b> {REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', 'N/A'))}\n"
                    f"<b>Тариф:</b> {order_info.get('tariff', 'N/A')}\n"
                    f"<b>Сумма:</b> {sum_line}\n"
                    f"<b>Оплата:</b> {payment_line}\n\n"
                    f"<b>👤 Клиент:</b>\n"
                    f"Имя: {esc(update.message.from_user.first_name) or 'Неизвестно'}\n"
                    f"Ник: @{esc(update.message.from_user.username) if update.message.from_user.username else 'нет'}\n"
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

        await update.message.reply_text(
            f"✅ <b>Скриншот получен!</b>\n\n"
            f"Заказ: <b>{order_number}</b>\n\n"
            f"Ваш скриншот отправлен на проверку менеджеру.\n"
            f"Ожидайте подтверждения оплаты.\n\n"
            f"⚠️ Отправили не тот скриншот? Нажмите кнопку ниже.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📎 Отправить другой скриншот", callback_data=f"resend_screenshot_{order_number}")]
            ]),
            parse_mode="HTML"
        )

        del AWAITING_SCREENSHOT[user_id]

    except Exception as e:
        logger.error(f"Ошибка в photo_handler: {e}")

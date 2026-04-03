"""
Заказы: выбор региона, тарифа, подтверждение, кастомная сумма, списание баллов, навигация.
"""

import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    ADMIN_ID, PRICES, GIFT_CARD_TARIFFS, REGION_DISPLAY, REGION_COMMISSION,
    GIFT_CARD_LABELS, REGION_DESCRIPTIONS, GIFT_CARD_HINTS,
    VIP_DISCOUNT, VIP_THRESHOLD, MAX_BONUS_PAYMENT,
)
from utils import (
    fmt, esc, get_rate, get_usdt_rate, get_kz_commission, get_us_commission,
    smart_round, check_spam, mark_order_created, generate_order, validate_email,
    ORDER_USER_MAP, ORDER_INFO_MAP, ORDER_LOCK,
    AWAITING_EMAIL,
)
from keyboards import (
    region_selection_keyboard, payment_buttons,
    vip_promo_text, vip_promo_keyboard,
)
from sheets import add_order_to_sheet, update_order_amount_in_sheet
from database import db
from handlers.common import (
    _safe_edit, _get_order_data, _calc_referral_discount,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# НАВИГАЦИЯ
# ═══════════════════════════════════════════════

async def handle_back_to_start(query, context):
    keyboard = [
        [InlineKeyboardButton("🍏 Пополнить Apple ID", callback_data="apple_topup")],
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="cabinet")],
        [InlineKeyboardButton("❓ FAQ", callback_data="faq_menu")],
    ]
    await query.edit_message_text(
        "🍏 Главное меню\n\nВыберите действие или используйте кнопки внизу:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ═══════════════════════════════════════════════
# ПОПОЛНЕНИЕ APPLE ID
# ═══════════════════════════════════════════════

async def handle_apple_topup(query, context):
    keyboard = region_selection_keyboard()
    context.user_data.pop("awaiting_apple", None)
    await query.edit_message_text(
        "🍏 Пополнение Apple ID\n\nВыберите регион своего Apple ID:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_region_kz(query, context):
    context.user_data.pop("awaiting_apple", None)
    keyboard = [
        [InlineKeyboardButton("🍏 5 000 KZT", callback_data="apple_5000")],
        [InlineKeyboardButton("🍏 10 000 KZT", callback_data="apple_10000")],
        [InlineKeyboardButton("🍏 15 000 KZT", callback_data="apple_15000")],
        [InlineKeyboardButton("✏️ Ввести свою сумму", callback_data="apple_custom")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="apple_topup")]
    ]
    await query.edit_message_text(
        "🇰🇿 Казахстан — Пополнение Apple ID\n\n"
        "🌟 <i>Эксклюзивное ручное пополнение.</i>\n\n"
        "Выберите сумму пополнения:\n\n"
        "⚠️ <b>Важно:</b> <i>App Store в Казахстане начисляет НДС (12%) сверх цены подписки. "
        "Имейте это в виду, когда выбираете тариф. "
        "Мы не несём ответственности, если у вас не получится совершить покупку.</i>",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def handle_region_gc(query, context):
    region_code = query.data.replace("region_", "")
    tariffs = GIFT_CARD_TARIFFS[region_code]
    region_name = REGION_DISPLAY[region_code]
    labels = GIFT_CARD_LABELS.get(region_code, {})
    keyboard = []
    for amount, currency, usdt_cost in tariffs:
        label = labels.get(amount, f"{fmt(amount)} {currency}")
        keyboard.append([InlineKeyboardButton(
            f"🍏 {label}",
            callback_data=f"gc_{region_code}_{amount}"
        )])
    keyboard.append([InlineKeyboardButton("⬅️ Назад к регионам", callback_data="apple_topup")])
    region_desc = REGION_DESCRIPTIONS.get(region_code, "")
    extra = f"\n\n{region_desc}" if region_desc else ""
    await query.edit_message_text(
        f"{region_name} — Gift Card Apple\n\nВыберите номинал гифт-карты:{extra}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def handle_gc_tariff(query, context):
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

    usdt_rate = await asyncio.to_thread(get_usdt_rate)
    if not usdt_rate:
        await query.edit_message_text("⚠️ Курс валют временно недоступен. Попробуйте через несколько минут.")
        return
    commission = get_us_commission(t_amount) if region_code == "US" else REGION_COMMISSION.get(region_code, 1.15)
    commission_pct = round((commission - 1) * 100)
    rub = smart_round(int(t_usdt * usdt_rate * commission))
    order_number = await asyncio.to_thread(generate_order)
    if not order_number:
        await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
        return

    region_name = REGION_DISPLAY[region_code]
    tariff_name = f"{fmt(t_amount)} {t_currency}"

    ref_info = await _calc_referral_discount(user.id, rub, commission)
    rub_final = ref_info["rub_discounted"]

    context.user_data["order"] = {
        "number": order_number,
        "service": f"Gift Card ({region_name})",
        "tariff": tariff_name,
        "rub": rub_final,
        "rub_original": rub,
        "region": region_code,
        "user": user,
        "commission": commission,
        "ref_discount": ref_info["discount_rub"],
        "partner_pct": ref_info["partner_pct"],
    }
    keyboard = [
        [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
        [InlineKeyboardButton("❌ Отмена", callback_data=f"region_{region_code}")]
    ]
    _hints_r = GIFT_CARD_HINTS.get(region_code, {})
    hint = _hints_r.get(t_amount) or _hints_r.get("_default")
    hint_line = f"\n\n💡 <i>Этого номинала хватит на: {hint}.</i>" if hint else ""

    if ref_info["discount_rub"] > 0:
        price_line = (
            f"Сумма к оплате: <s>{fmt(rub)} ₽</s> → <b>{fmt(rub_final)} ₽</b> "
            f"(скидка {round(ref_info['discount_pct'] * 100)}% по реф. ссылке)"
        )
    else:
        price_line = f"Сумма к оплате: <b>{fmt(rub_final)} ₽</b> (комиссия {commission_pct}%)"

    await query.edit_message_text(
        f"📦 Информация о заказе\n\n"
        f"Номер заказа: <b>{order_number}</b>\n"
        f"Регион: <b>{region_name}</b>\n"
        f"Тариф: <b>{tariff_name} Gift Card</b>\n"
        f"{price_line}"
        f"{hint_line}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {user.id} создал заказ {order_number} (Gift Card {region_code} {tariff_name})")


async def handle_apple_custom(query, context):
    context.user_data["awaiting_apple"] = True
    await query.edit_message_text(
        "Введите сумму пополнения Apple ID (5 000–45 000 KZT)",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="region_KZ")]])
    )


async def handle_apple_tariff(query, context):
    user = query.from_user
    can_create, spam_msg = check_spam(user.id)
    if not can_create:
        await query.answer(spam_msg, show_alert=True)
        return

    amount = PRICES[query.data]
    rate = await asyncio.to_thread(get_rate)
    if not rate:
        await query.edit_message_text("❌ Ошибка получения курса. Попробуйте позже.")
        return

    commission = get_kz_commission(amount)
    commission_pct = round((commission - 1) * 100)
    rub = smart_round(int(amount * rate * commission))
    order_number = await asyncio.to_thread(generate_order)
    if not order_number:
        await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
        return

    tariff_name = f"{fmt(amount)} KZT"

    ref_info = await _calc_referral_discount(user.id, rub, commission)
    rub_final = ref_info["rub_discounted"]

    context.user_data["order"] = {
        "number": order_number,
        "service": "Apple ID",
        "tariff": tariff_name,
        "rub": rub_final,
        "rub_original": rub,
        "region": "KZ",
        "user": user,
        "commission": commission,
        "ref_discount": ref_info["discount_rub"],
        "partner_pct": ref_info["partner_pct"],
    }
    keyboard = [
        [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
        [InlineKeyboardButton("❌ Отмена", callback_data="region_KZ")]
    ]
    kz_hint = GIFT_CARD_HINTS.get("KZ", {}).get(amount)
    hint_line = f"\n\n💡 <i>Этого номинала хватит на: {kz_hint}.</i>" if kz_hint else ""

    if ref_info["discount_rub"] > 0:
        price_line = (
            f"Сумма к оплате: <s>{fmt(rub)} ₽</s> → <b>{fmt(rub_final)} ₽</b> "
            f"(скидка {round(ref_info['discount_pct'] * 100)}% по реф. ссылке)"
        )
    else:
        price_line = f"Сумма к оплате: <b>{fmt(rub_final)} ₽</b> (сервисный сбор {commission_pct}%)"

    await query.edit_message_text(
        f"📦 Информация о заказе\n\n"
        f"Номер заказа: <b>{order_number}</b>\n"
        f"Тариф: <b>{tariff_name}</b>\n"
        f"{price_line}"
        f"{hint_line}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {user.id} создал заказ {order_number}")


async def handle_confirm(query, context):
    order = context.user_data.get("order")
    if not order:
        await query.edit_message_text("⚠️ Заказ не найден. Попробуйте снова.")
        return

    order_number = order["number"]
    user_id = order["user"].id

    can_create, spam_msg = check_spam(user_id)
    if not can_create:
        await query.answer(spam_msg, show_alert=True)
        return

    if order_number in ORDER_LOCK:
        await query.edit_message_text("⏳ Заказ уже обрабатывается. Подождите...")
        return

    ORDER_LOCK[order_number] = True
    try:
        ORDER_USER_MAP[order_number] = user_id

        order_data = {
            "number": order_number,
            "user_id": user_id,
            "username": order["user"].username or "Нет ника",
            "first_name": order["user"].first_name or "Клиент",
            "service": order["service"],
            "tariff": order["tariff"],
            "rub": order["rub"],
            "region": order.get("region", "KZ")
        }

        db_ok, sheets_ok = await asyncio.to_thread(add_order_to_sheet, order_data)
        if not db_ok:
            await query.edit_message_text("❌ Ошибка сохранения заказа. Попробуйте позже.")
            return
        if not sheets_ok:
            try:
                await context.bot.send_message(
                    ADMIN_ID,
                    f"⚠️ Заказ <b>{order_number}</b> сохранён в БД, но <b>НЕ</b> в Google Sheets!\n"
                    f"Проверьте доступ к таблице.",
                    parse_mode="HTML",
                )
            except Exception:
                pass

        mark_order_created(user_id)

        usdt_rate = await asyncio.to_thread(get_usdt_rate)
        amount_usdt = round(order["rub"] / usdt_rate, 2) if usdt_rate else None
        context.user_data["amount_usdt"] = amount_usdt

        ORDER_INFO_MAP[order_number] = {
            "user_id": user_id,
            "username": order["user"].username or "Нет ника",
            "first_name": order["user"].first_name or "Клиент",
            "service": order["service"],
            "tariff": order["tariff"],
            "rub": order["rub"],
            "usdt": amount_usdt or 0,
            "region": order.get("region", "KZ"),
            "commission": order.get("commission", 0),
            "partner_pct": order.get("partner_pct", 0),
            "ref_discount": order.get("ref_discount", 0),
            "rub_original": order.get("rub_original", order["rub"]),
            "bonus_used": 0,
        }
        context.user_data["current_order_number"] = order_number

        if order["rub"] > VIP_THRESHOLD:
            rub_discounted = round(order["rub"] * (1 - VIP_DISCOUNT))
            saving = order["rub"] - rub_discounted
            usdt_suffix = f" (~{round(rub_discounted / usdt_rate, 2)} USDT)" if usdt_rate else ""
            await query.edit_message_text(
                vip_promo_text(order_number, saving, rub_discounted, usdt_suffix),
                reply_markup=InlineKeyboardMarkup(vip_promo_keyboard(order_number)),
                parse_mode="HTML"
            )
            context.user_data["rub_discounted"] = rub_discounted
            context.user_data["vip_order_number"] = order_number
            logger.info(f"Заказ {order_number} — промо VIP-экран (>{VIP_THRESHOLD}₽)")
        else:
            pay_btns = payment_buttons(order_number, is_large_order=False)
            bonus_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
            bonus_line = ""
            if bonus_balance > 0:
                max_bonus = int(order["rub"] * MAX_BONUS_PAYMENT)
                usable = min(int(bonus_balance), max_bonus)
                if usable >= 1:
                    pay_btns.insert(0, [InlineKeyboardButton(
                        f"🎁 Списать {fmt(usable)} баллов",
                        callback_data=f"use_bonus_{order_number}"
                    )])
                    bonus_line = f"\n💰 Доступно баллов: {fmt(int(bonus_balance))} (можно списать до {fmt(usable)} ₽)\n"
            usdt_suffix = f" (~{amount_usdt} USDT)" if amount_usdt else ""
            await query.edit_message_text(
                f"✅ Заявка сформирована!\n\n"
                f"Номер заказа: <b>{order['number']}</b>\n"
                f"Тариф: <b>{order['tariff']}</b>\n"
                f"Сумма: <b>{fmt(order['rub'])} ₽</b>{usdt_suffix}\n"
                f"{bonus_line}\n"
                f"Выберите способ оплаты:",
                reply_markup=InlineKeyboardMarkup(pay_btns),
                parse_mode="HTML"
            )
            logger.info(f"Заказ {order_number} — выбор способа оплаты")
    finally:
        ORDER_LOCK.pop(order_number, None)


async def handle_use_bonus(query, context):
    order_number = query.data.replace("use_bonus_", "")
    order = _get_order_data(context, order_number)
    if not order:
        await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
        return
    user_id = query.from_user.id
    bonus_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
    max_bonus = int(order["rub"] * MAX_BONUS_PAYMENT)
    usable = min(int(bonus_balance), max_bonus)
    if usable < 1:
        await query.answer("Недостаточно баллов для списания.", show_alert=True)
        return

    spent = await asyncio.to_thread(
        db.spend_bonus, user_id, usable, order_number,
        f"Оплата заказа {order_number}"
    )
    if not spent:
        await query.answer("Ошибка списания баллов. Попробуйте снова.", show_alert=True)
        return

    new_rub = order["rub"] - usable
    order["rub"] = new_rub
    context.user_data["order"] = order
    if order_number in ORDER_INFO_MAP:
        ORDER_INFO_MAP[order_number]["rub"] = new_rub
        ORDER_INFO_MAP[order_number]["bonus_used"] = usable
    await asyncio.to_thread(update_order_amount_in_sheet, order_number, new_rub)

    usdt_rate = await asyncio.to_thread(get_usdt_rate)
    amount_usdt = round(new_rub / usdt_rate, 2) if usdt_rate else None
    context.user_data["amount_usdt"] = amount_usdt

    pay_btns = payment_buttons(order_number, is_large_order=(new_rub > VIP_THRESHOLD))
    usdt_suffix = f" (~{amount_usdt} USDT)" if amount_usdt else ""
    await query.edit_message_text(
        f"✅ Списано <b>{fmt(usable)}</b> баллов!\n\n"
        f"Номер заказа: <b>{order['number']}</b>\n"
        f"Тариф: <b>{order['tariff']}</b>\n"
        f"Сумма: <b>{fmt(new_rub)} ₽</b>{usdt_suffix}\n\n"
        f"Выберите способ оплаты:",
        reply_markup=InlineKeyboardMarkup(pay_btns),
        parse_mode="HTML"
    )
    logger.info(f"Пользователь {user_id} списал {usable} баллов для заказа {order_number}")


# ═══════════════════════════════════════════════
# TEXT HANDLERS
# ═══════════════════════════════════════════════

async def handle_email_text(update, context):
    """Обработка ввода email Apple ID"""
    user_id = update.message.from_user.id
    order_number = AWAITING_EMAIL.get(user_id)
    if not order_number:
        del AWAITING_EMAIL[user_id]
        return True
    email = update.message.text.strip().lower()

    if validate_email(email):
        del AWAITING_EMAIL[user_id]

        user = update.message.from_user
        user_name = user.full_name or "Без имени"
        username = f"@{user.username}" if user.username else "Нет username"

        order_details = ""
        order_db_data = await asyncio.to_thread(db.get_order, order_number)
        if order_db_data:
            order_details = (
                f"📋 Тариф: {order_db_data.get('tariff', '—')}\n"
                f"💰 Сумма: {order_db_data.get('amount_rub', 0)} ₽\n\n"
            )

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
                f"<code>{esc(email)}</code>\n\n"
                f"👤 Клиент:\nИмя: {esc(user_name)}\nНик: {esc(username)}\nID: <code>{user_id}</code>",
                reply_markup=InlineKeyboardMarkup(admin_keyboard),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки почты админу: {e}")

        await update.message.reply_text(
            f"✅ Почта <b>{esc(email)}</b> получена!\n\n"
            f"📦 Заказ: <b>{order_number}</b>\n\n"
            f"Мы отправим код на вашу почту. Ожидайте уведомления!\n\n"
            f"📱 <b>Как активировать код:</b>\n"
            f"1. Откройте App Store на iPhone/iPad\n"
            f"2. Нажмите на иконку профиля → «Погасить подарочную карту или код»\n"
            f"3. Введите полученный код\n\n"
            f"Готово! Баланс Apple ID будет пополнен.",
            parse_mode="HTML"
        )
        logger.info(f"Клиент {user_id} отправил почту {email} для заказа {order_number}")
    else:
        await update.message.reply_text("❌ Некорректный email. Пожалуйста, отправьте правильную почту Apple ID:")
    return True


async def handle_custom_amount_text(update, context):
    """Обработка ввода кастомной суммы KZT"""
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    try:
        amount = int(text)
        if not (5000 <= amount <= 45000):
            await update.message.reply_text("❌ Неверный диапазон.\n\nВведите сумму от 5 000 до 45 000 KZT (шаг 500):")
            return True
        if amount % 500 != 0:
            await update.message.reply_text("❌ Сумма должна быть кратна 500 KZT.\n\nНапример: 5 000, 5 500, 6 000 и т.д.")
            return True

        can_create, spam_msg = check_spam(user_id)
        if not can_create:
            await update.message.reply_text(spam_msg)
            return True

        rate = await asyncio.to_thread(get_rate)
        if not rate:
            await update.message.reply_text("❌ Ошибка получения курса. Попробуйте позже.")
            return True

        commission = get_kz_commission(amount)
        commission_pct = round((commission - 1) * 100)
        rub = smart_round(int(amount * rate * commission))
        order_number = await asyncio.to_thread(generate_order)
        if not order_number:
            await update.message.reply_text("❌ Ошибка генерации заказа. Попробуйте позже.")
            return True

        user = update.message.from_user
        tariff_name = f"{fmt(amount)} KZT"

        ref_info = await _calc_referral_discount(user.id, rub, commission)
        rub_final = ref_info["rub_discounted"]

        context.user_data["order"] = {
            "number": order_number,
            "service": "Apple ID",
            "tariff": tariff_name,
            "rub": rub_final,
            "rub_original": rub,
            "region": "KZ",
            "user": user,
            "commission": commission,
            "ref_discount": ref_info["discount_rub"],
            "partner_pct": ref_info["partner_pct"],
        }
        keyboard = [
            [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
            [InlineKeyboardButton("❌ Отмена", callback_data="region_KZ")]
        ]

        if ref_info["discount_rub"] > 0:
            price_line = (
                f"Сумма к оплате: <s>{fmt(rub)} ₽</s> → <b>{fmt(rub_final)} ₽</b> "
                f"(скидка {round(ref_info['discount_pct'] * 100)}% по реф. ссылке)"
            )
        else:
            price_line = f"Сумма к оплате: <b>{fmt(rub_final)} ₽</b> (сервисный сбор {commission_pct}%)"

        await update.message.reply_text(
            f"📦 Информация о заказе\n\n"
            f"Номер заказа: <b>{order_number}</b>\n"
            f"Тариф: <b>{tariff_name}</b>\n"
            f"{price_line}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
        context.user_data["awaiting_apple"] = False
        logger.info(f"Пользователь {user_id} создал заказ Apple на {amount} KZT")
        return True
    except ValueError:
        await update.message.reply_text("❌ Введите только число.\n\nПовторите ввод суммы (5 000–45 000 KZT):")
        return True

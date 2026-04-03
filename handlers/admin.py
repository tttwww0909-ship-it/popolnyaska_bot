"""
Админ-панель: управление заказами, статусами, отправка кодов,
рассылка клиентам, управление бонусами пользователей.
"""

import asyncio
import logging
from html import escape as html_escape

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    ADMIN_ID, ORDER_STATUSES, REGION_DISPLAY, REVIEWS_CHAT_ID,
)
from utils import (
    fmt, esc,
    ORDER_USER_MAP, ORDER_INFO_MAP,
    AWAITING_EMAIL, AWAITING_CODE,
)
from keyboards import admin_panel_keyboard, rating_keyboard
from sheets import update_order_status, find_order_user_in_sheets
from database import db
from handlers.common import (
    _safe_edit, _credit_partner_bonus,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ-панель"""
    try:
        if update.message.from_user.id != ADMIN_ID:
            await update.message.reply_text("❌ У вас нет доступа.")
            logger.warning(f"Попытка доступа к админ-панели от {update.message.from_user.id}")
            return

        keyboard = admin_panel_keyboard()
        await update.message.reply_text(
            "⚙️ Админ-панель",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info(f"Админ {ADMIN_ID} открыл админ-панель")
    except Exception as e:
        logger.error(f"Ошибка в admin: {e}")


async def reviews_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все отзывы — только для админа"""
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет доступа.")
        return

    reviews = await asyncio.to_thread(db.get_all_reviews)
    if not reviews:
        await update.message.reply_text("📭 Отзывов пока нет.")
        return

    chunk_size = 10
    total = len(reviews)
    for i in range(0, total, chunk_size):
        chunk = reviews[i:i + chunk_size]
        text = f"⭐ <b>Отзывы ({i+1}–{min(i+chunk_size, total)} из {total})</b>\n\n"
        for r in chunk:
            stars = "⭐" * r["rating"]
            comment = f"\n💬 <i>{html_escape(r['comment'])}</i>" if r.get("comment") else ""
            date = str(r.get("created_at", ""))[:10]
            text += (
                f"<b>{r['order_number']}</b> · @{html_escape(r.get('username', ''))} · {date}\n"
                f"{stars}{comment}\n"
                f"{'─' * 20}\n"
            )
        await update.message.reply_text(text, parse_mode="HTML")


# ═══════════════════════════════════════════════
# CALLBACK: ОТКРЫТЬ ЛС С КЛИЕНТОМ
# ═══════════════════════════════════════════════

async def handle_open_client_dm(query, context):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return
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
            f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{esc(client_name)}</a> (ID: <code>{client_id}</code>)\n\n'
            f"Клиент запросил связь с менеджером.\n"
            f"Если у него есть заказ:\n"
            f"  • Регион: {esc(client_region) or 'уточнить'}\n"
            f"  • Тариф: {esc(client_tariff) or 'уточнить'}\n\n"
            f"Напишите ему в личку и помогите!"
        )
    else:
        msg = (
            f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{esc(client_name)}</a> (ID: <code>{client_id}</code>)\n\n'
            f"<b>📦 Заказ:</b>\n"
            f"  • Номер: {esc(reason)}\n"
            f"  • Регион: {esc(client_region)}\n"
            f"  • Тариф: {esc(client_tariff)}\n\n"
            f"Напишите ему об оплате или доступе."
        )
    await query.edit_message_text(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════
# CALLBACK: ЗАКАЗЫ
# ═══════════════════════════════════════════════

async def handle_admin_orders(query, context):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return

    orders = await asyncio.to_thread(db.get_recent_orders, 10)
    if not orders:
        await query.edit_message_text(
            "📦 Заказов пока нет.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
        )
        return

    msg = "📦 Последние 10 заказов:\n\n"
    for o in orders:
        order_info_cached = ORDER_INFO_MAP.get(o['order_number'], {})
        region_code = order_info_cached.get('region', '')
        if region_code:
            region_display = REGION_DISPLAY.get(region_code, region_code)
        else:
            region_display = o.get('service', '—')
        msg += f"🔹 <b>{esc(o['order_number'])}</b>\n"
        msg += f"   Статус: {esc(o.get('status', '—'))}\n"
        msg += f"   Сервис: {esc(region_display)}\n"
        msg += f"   Тариф: {esc(o.get('tariff', '—'))}\n"
        msg += f"   Сумма: {o.get('amount_rub', 0)} ₽\n"
        msg += f"   ID: <code>{o.get('telegram_id', '—')}</code>\n\n"

    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


# ═══════════════════════════════════════════════
# CALLBACK: СТАТИСТИКА
# ═══════════════════════════════════════════════

async def handle_stats_general(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    try:
        stats = await asyncio.to_thread(db.get_admin_stats)
        if not stats:
            await query.edit_message_text("⚠️ Ошибка получения статистики.")
            return
        msg = (
            "📊 <b>ОБЩАЯ СТАТИСТИКА</b>\n\n"
            f"👥 Уникальных клиентов: <b>{stats.get('unique_users', 0)}</b>\n"
            f"📦 Всего заказов: <b>{stats.get('total_orders', 0)}</b>\n\n"
            f"<b>📈 ПО СТАТУСАМ:</b>\n"
        )
        for status, count in stats.get('statuses', {}).items():
            msg += f"• {status}: <b>{count}</b>\n"
        msg += (
            f"\n<b>💰 ФИНАНСЫ:</b>\n"
            f"• Выручка: <b>{fmt(stats.get('revenue', 0))} ₽</b>\n"
            f"• Средний чек: <b>{fmt(stats.get('avg_check', 0))} ₽</b>\n"
            f"• Конверсия: <b>{stats.get('conversion', 0)}%</b>\n"
        )
        await query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        await query.edit_message_text("⚠️ Ошибка получения статистики.")


# ═══════════════════════════════════════════════
# CALLBACK: УПРАВЛЕНИЕ СТАТУСАМИ
# ═══════════════════════════════════════════════

async def handle_admin_manage_orders(query, context):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return

    orders = await asyncio.to_thread(db.get_active_orders, 20)
    if not orders:
        await query.edit_message_text(
            "📦 Нет активных заказов для изменения статуса.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
        )
        return

    msg = "🔄 Выберите заказ для изменения статуса:\n\n"
    keyboard = []
    for o in orders:
        order_num = o['order_number']
        rub_amt = o.get('amount_rub', 0)
        status = o.get('status', '—')
        tariff = o.get('tariff', '—')
        msg += f"🔹 <b>{order_num}</b> — {tariff} ({rub_amt} ₽) — {status}\n"
        keyboard.append([InlineKeyboardButton(f"📝 {order_num}", callback_data=f"admin_select_order_{order_num}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")])
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def handle_admin_select_order(query, context):
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return
    order_num = query.data.replace("admin_select_order_", "")

    order_db = await asyncio.to_thread(db.get_order, order_num)
    if order_db:
        order_info_text = (
            f"📦 Заказ: <b>{order_num}</b>\n"
            f"📋 Тариф: {order_db.get('tariff', '—')}\n"
            f"💰 Сумма: {order_db.get('amount_rub', 0)} ₽\n\n"
        )
    else:
        order_info_text = f"📦 Заказ: <b>{order_num}</b>\n\n"

    keyboard = [
        [InlineKeyboardButton("💰 Оплачен", callback_data=f"admin_set_status_{order_num}_paid")],
        [InlineKeyboardButton("✅ Выполнен", callback_data=f"admin_set_status_{order_num}_completed")],
        [InlineKeyboardButton("❌ Отменён", callback_data=f"admin_set_status_{order_num}_cancelled")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin_manage_orders")]
    ]
    await query.edit_message_text(
        f"{order_info_text}Выберите новый статус:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )


async def handle_admin_set_status(query, context):
    if query.from_user.id != ADMIN_ID:
        logger.warning(f"⚠️ Попытка изменения статуса от не-админа: {query.from_user.id}")
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return

    parts = query.data.replace("admin_set_status_", "").rsplit("_", 1)
    order_num = parts[0]
    new_status = parts[1]
    status_name = ORDER_STATUSES.get(new_status, new_status)

    success = await asyncio.to_thread(update_order_status, order_num, status_name)

    if success:
        user_id = ORDER_USER_MAP.get(order_num)
        order_region = ""

        if user_id:
            info = ORDER_INFO_MAP.get(order_num, {})
            order_region = info.get('region', '')
        else:
            info = ORDER_INFO_MAP.get(order_num, {})
            if info:
                user_id = info.get('user_id')
                order_region = info.get('region', '')

            if not user_id:
                user_id = await asyncio.to_thread(db.get_telegram_id_for_order, order_num)

            if not user_id:
                sheets_uid, sheets_region = await asyncio.to_thread(find_order_user_in_sheets, order_num)
                if sheets_uid:
                    user_id = sheets_uid
                if not order_region and sheets_region:
                    order_region = sheets_region

        if user_id:
            is_gift_card = order_region in ("TR", "US", "AE", "SA")

            if new_status == "paid" and is_gift_card:
                client_message = (
                    "💰 Ваша оплата подтверждена! Заказ в обработке.\n\n"
                    "⏳ Ожидайте получения кода — бот отправит его вам.\n\n"
                    "⚠️ <b>Обратите внимание:</b> после получения кода средства возврату не подлежат. "
                    "При возникновении проблем обращайтесь в службу поддержки."
                )
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
                client_message = "💰 Ваша оплата подтверждена! Заказ в обработке.\n\n📧 Пожалуйста, отправьте вашу почту Apple ID (email), на которую нужно выполнить пополнение:"
                AWAITING_EMAIL[user_id] = order_num
            else:
                status_messages = {
                    "completed": "✅ Ваш заказ выполнен! Спасибо за покупку.\n\n❓ При проблемах — раздел FAQ или напишите в поддержку.",
                    "cancelled": "❌ Ваш заказ отменён. Если есть вопросы — свяжитесь с поддержкой."
                }
                client_message = status_messages.get(new_status, f"Статус заказа изменён на: {status_name}")

            try:
                if new_status == "completed":
                    await context.bot.send_message(
                        user_id,
                        f"📦 <b>Заказ {order_num} выполнен!</b>\n\n"
                        f"✅ Спасибо за покупку!\n\n"
                        f"❓ При проблемах — раздел FAQ или напишите в поддержку.\n\n"
                        f"⭐ Оцените качество нашего сервиса:",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(rating_keyboard(order_num))
                    )
                    await _credit_partner_bonus(context.bot, order_num, user_id)
                else:
                    await context.bot.send_message(
                        user_id,
                        f"📦 <b>Заказ {order_num}</b>\n\n{client_message}",
                        parse_mode="HTML"
                    )
                logger.info(f"Клиент {user_id} уведомлён о статусе {status_name}")
            except Exception as e:
                logger.error(f"Ошибка уведомления клиента о статусе: {e}")

        order_db_info = await asyncio.to_thread(db.get_order, order_num)
        order_details_lines = ""
        if order_db_info:
            order_details_lines = (
                f"📋 Тариф: <b>{order_db_info.get('tariff', '—')}</b>\n"
                f"💰 Сумма: <b>{order_db_info.get('amount_rub', 0)} ₽</b>\n"
            )
        elif order_num in ORDER_INFO_MAP:
            oi = ORDER_INFO_MAP[order_num]
            order_details_lines = (
                f"🌍 Регион: <b>{REGION_DISPLAY.get(oi.get('region', ''), oi.get('region', '—'))}</b>\n"
                f"📋 Тариф: <b>{oi.get('tariff', '—')}</b>\n"
                f"💰 Сумма: <b>{oi.get('rub', 0)} ₽</b>\n"
            )

        await _safe_edit(
            query,
            f"✅ Статус заказа <b>{order_num}</b> изменён на: <b>{status_name}</b>\n\n"
            f"{order_details_lines}"
            f"{'✉️ Клиент уведомлён.' if user_id else '⚠️ Не удалось уведомить клиента (ID не найден).'}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 К списку заказов", callback_data="admin_manage_orders")],
                [InlineKeyboardButton("⬅️ В админ-панель", callback_data="back_to_admin")]
            ]),
            parse_mode="HTML"
        )
        logger.info(f"Админ изменил статус {order_num} на {status_name}")
    else:
        await _safe_edit(
            query,
            f"❌ Ошибка изменения статуса заказа <b>{order_num}</b>",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_manage_orders")]]),
            parse_mode="HTML"
        )


# ═══════════════════════════════════════════════
# CALLBACK: ОТПРАВКА КОДА
# ═══════════════════════════════════════════════

async def handle_send_code(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    parts = query.data.replace("send_code_", "").split("_")
    order_num = parts[0]
    client_id = int(parts[1]) if len(parts) > 1 else None
    if not client_id:
        await query.edit_message_text("❌ Не удалось определить клиента.")
        return
    AWAITING_CODE[ADMIN_ID] = {"order_num": order_num, "client_id": client_id}
    await query.edit_message_text(
        f"📤 <b>Отправка кода</b>\n\n"
        f"📦 Заказ: <b>{order_num}</b>\n\n"
        f"Введите код Gift Card для отправки клиенту:",
        parse_mode="HTML"
    )
    logger.info(f"Админ готовится отправить код для заказа {order_num}")


async def handle_topup_done(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    parts = query.data.replace("topup_done_", "").split("_")
    order_num = parts[0]
    client_id = int(parts[1]) if len(parts) > 1 else None

    await asyncio.to_thread(update_order_status, order_num, ORDER_STATUSES["completed"])

    if client_id:
        try:
            await context.bot.send_message(
                client_id,
                f"🎉 <b>Код отправлен на вашу почту!</b>\n\n"
                f"📦 Заказ: <b>{order_num}</b>\n\n"
                f"✅ Проверьте почту — мы отправили код для пополнения Apple ID.\n\n"
                f"📱 <b>Как активировать:</b>\n"
                f"1. App Store → профиль → «Погасить подарочную карту или код»\n"
                f"2. Введите полученный код\n\n"
                f"Спасибо, что воспользовались нашим сервисом! 🍏\n\n"
                f"❓ При проблемах — раздел FAQ или напишите в поддержку.\n\n"
                f"⭐ Оцените качество нашего сервиса:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(rating_keyboard(order_num))
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления клиента о пополнении: {e}")
        await _credit_partner_bonus(context.bot, order_num, client_id)

    await query.edit_message_text(
        f"✅ Заказ <b>{order_num}</b> выполнен!\n\nКлиент уведомлён о пополнении.",
        parse_mode="HTML"
    )
    logger.info(f"Админ отметил пополнение выполненным: {order_num}")


async def handle_back_to_admin(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    # Очищаем все ожидающие состояния админа
    for key in ("admin_awaiting_broadcast", "admin_awaiting_bonus_uid", "admin_awaiting_bonus_amount"):
        context.user_data.pop(key, None)
    keyboard = admin_panel_keyboard()
    await query.edit_message_text("⚙️ Админ-панель", reply_markup=InlineKeyboardMarkup(keyboard))


# ═══════════════════════════════════════════════
# РАССЫЛКА
# ═══════════════════════════════════════════════

async def handle_admin_broadcast(query, context):
    """Начало рассылки: запрашиваем текст сообщения."""
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return
    context.user_data["admin_awaiting_broadcast"] = True
    await query.edit_message_text(
        "📢 <b>Рассылка</b>\n\n"
        "Введите текст сообщения для всех пользователей.\n\n"
        "Поддерживается HTML-разметка (<b>жирный</b>, <i>курсив</i>, <code>код</code>).\n\n"
        "Для отмены нажмите кнопку ⬅️ Назад.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]),
        parse_mode="HTML"
    )


async def handle_broadcast_text(update, context):
    """Обработка текста рассылки от админа."""
    text = update.message.text.strip()
    context.user_data.pop("admin_awaiting_broadcast", None)

    user_ids = await asyncio.to_thread(db.get_all_user_ids)
    if not user_ids:
        await update.message.reply_text("📭 Нет пользователей для рассылки.")
        return True

    await update.message.reply_text(
        f"📢 Начинаю рассылку для <b>{len(user_ids)}</b> пользователей...",
        parse_mode="HTML"
    )

    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(uid, text, parse_mode="HTML")
            sent += 1
        except Exception:
            failed += 1
        # Telegram rate limit: ~30 msg/sec
        if (sent + failed) % 25 == 0:
            await asyncio.sleep(1)

    await update.message.reply_text(
        f"✅ <b>Рассылка завершена</b>\n\n"
        f"📨 Отправлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{failed}</b>",
        parse_mode="HTML"
    )
    logger.info(f"Рассылка: отправлено {sent}, ошибок {failed}")
    return True


# ═══════════════════════════════════════════════
# УПРАВЛЕНИЕ БОНУСАМИ
# ═══════════════════════════════════════════════

async def handle_admin_bonus(query, context):
    """Начало управления бонусами: запрос user_id"""
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ У вас нет доступа", show_alert=True)
        return
    context.user_data["admin_awaiting_bonus_uid"] = True
    await query.edit_message_text(
        "💰 <b>Управление бонусами</b>\n\n"
        "Введите Telegram ID пользователя:\n\n"
        "<i>ID можно найти в заказах или статистике.</i>",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]]),
        parse_mode="HTML"
    )


async def handle_bonus_uid_text(update, context):
    """Обработка ввода user_id для управления бонусами."""
    text = update.message.text.strip()
    try:
        user_id = int(text)
    except ValueError:
        await update.message.reply_text("❌ Некорректный ID. Введите числовой Telegram ID:")
        return True
    context.user_data.pop("admin_awaiting_bonus_uid", None)

    bonus_info = await asyncio.to_thread(db.get_bonus_info, user_id)
    balance = bonus_info["balance"]
    total_earned = bonus_info["total_earned"]
    total_spent = bonus_info["total_spent"]

    user_data = await asyncio.to_thread(db.get_user_by_telegram_id, user_id)
    username = user_data.get("username", "—") if user_data else "—"

    keyboard = [
        [InlineKeyboardButton("➕ Начислить баллы", callback_data=f"admin_bonus_add_{user_id}")],
        [InlineKeyboardButton("➖ Списать баллы", callback_data=f"admin_bonus_deduct_{user_id}")],
        [InlineKeyboardButton("⬅️ В админ-панель", callback_data="back_to_admin")],
    ]
    await update.message.reply_text(
        f"💰 <b>Бонусы пользователя</b>\n\n"
        f"👤 ID: <code>{user_id}</code>\n"
        f"Ник: @{html_escape(username)}\n\n"
        f"💰 Баланс: <b>{fmt(int(balance))} баллов</b>\n"
        f"📈 Всего заработано: <b>{fmt(int(total_earned))} баллов</b>\n"
        f"📉 Всего потрачено: <b>{fmt(int(total_spent))} баллов</b>",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
    return True


async def handle_admin_bonus_add(query, context):
    """Запрос суммы для начисления баллов."""
    if query.from_user.id != ADMIN_ID:
        return
    user_id = int(query.data.replace("admin_bonus_add_", ""))
    context.user_data["admin_awaiting_bonus_amount"] = {"user_id": user_id, "action": "add"}
    await query.edit_message_text(
        f"➕ <b>Начисление баллов</b>\n\n"
        f"👤 ID: <code>{user_id}</code>\n\n"
        f"Введите сумму и причину через пробел:\n"
        f"<code>100 За обращение в поддержку</code>",
        parse_mode="HTML"
    )


async def handle_admin_bonus_deduct(query, context):
    """Запрос суммы для списания баллов."""
    if query.from_user.id != ADMIN_ID:
        return
    user_id = int(query.data.replace("admin_bonus_deduct_", ""))
    context.user_data["admin_awaiting_bonus_amount"] = {"user_id": user_id, "action": "deduct"}
    await query.edit_message_text(
        f"➖ <b>Списание баллов</b>\n\n"
        f"👤 ID: <code>{user_id}</code>\n\n"
        f"Введите сумму и причину через пробел:\n"
        f"<code>50 Корректировка баланса</code>",
        parse_mode="HTML"
    )


async def handle_bonus_amount_text(update, context):
    """Обработка ввода суммы и причины для бонуса."""
    text = update.message.text.strip()
    bonus_data = context.user_data.pop("admin_awaiting_bonus_amount", None)
    if not bonus_data:
        return True

    user_id = bonus_data["user_id"]
    action = bonus_data["action"]

    parts = text.split(maxsplit=1)
    try:
        amount = int(parts[0])
        if amount <= 0:
            raise ValueError
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Некорректная сумма. Введите положительное число.")
        return True

    reason = parts[1] if len(parts) > 1 else ("Ручное начисление" if action == "add" else "Ручное списание")

    if action == "add":
        ok = await asyncio.to_thread(
            db.add_bonus, user_id, amount, "admin_bonus",
            None, f"[Админ] {reason}"
        )
        if ok:
            new_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
            await update.message.reply_text(
                f"✅ <b>Начислено {fmt(amount)} баллов</b>\n\n"
                f"👤 ID: <code>{user_id}</code>\n"
                f"💰 Новый баланс: <b>{fmt(int(new_balance))} баллов</b>\n"
                f"📝 Причина: {html_escape(reason)}",
                parse_mode="HTML"
            )
            # Уведомляем пользователя
            try:
                await context.bot.send_message(
                    user_id,
                    f"🎁 <b>Вам начислены баллы!</b>\n\n"
                    f"Начислено: <b>+{fmt(amount)} баллов</b>\n"
                    f"Баланс: <b>{fmt(int(new_balance))} баллов</b>\n\n"
                    f"<i>1 балл = 1 ₽ • Баллами можно оплатить до 50% заказа</i>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            logger.info(f"Админ начислил {amount} баллов пользователю {user_id}: {reason}")
        else:
            await update.message.reply_text("❌ Ошибка начисления баллов.")
    else:
        ok = await asyncio.to_thread(
            db.spend_bonus, user_id, amount, None,
            f"[Админ] {reason}"
        )
        if ok:
            new_balance = await asyncio.to_thread(db.get_bonus_balance, user_id)
            await update.message.reply_text(
                f"✅ <b>Списано {fmt(amount)} баллов</b>\n\n"
                f"👤 ID: <code>{user_id}</code>\n"
                f"💰 Новый баланс: <b>{fmt(int(new_balance))} баллов</b>\n"
                f"📝 Причина: {html_escape(reason)}",
                parse_mode="HTML"
            )
            logger.info(f"Админ списал {amount} баллов у пользователя {user_id}: {reason}")
        else:
            await update.message.reply_text("❌ Ошибка списания. Возможно, недостаточно баллов.")
    return True


# ═══════════════════════════════════════════════
# TEXT HANDLER: КОД ОТ АДМИНА (Gift Card)
# ═══════════════════════════════════════════════

async def handle_admin_code_text(update, context):
    """Обработка ввода кода Gift Card от админа."""
    code_data = AWAITING_CODE[ADMIN_ID]
    code_order = code_data["order_num"]
    code_client = code_data["client_id"]
    gift_code = update.message.text.strip()
    del AWAITING_CODE[ADMIN_ID]

    await asyncio.to_thread(update_order_status, code_order, ORDER_STATUSES["completed"])

    try:
        await context.bot.send_message(
            code_client,
            f"🎉 <b>Ваш код получен!</b>\n\n"
            f"📦 Заказ: <b>{code_order}</b>\n\n"
            f"🔑 Код Gift Card:\n<code>{html_escape(gift_code)}</code>\n\n"
            f"📱 <b>Как активировать:</b>\n"
            f"1. App Store → профиль → «Погасить подарочную карту или код»\n"
            f"2. Введите код выше\n\n"
            f"Спасибо, что воспользовались нашим сервисом! 🍏\n\n"
            f"❓ При проблемах — раздел FAQ или напишите в поддержку.",
            parse_mode="HTML"
        )
        logger.info(f"Код отправлен клиенту {code_client} для заказа {code_order}")
        try:
            await context.bot.send_message(
                code_client,
                "⭐ Оцените качество нашего сервиса:",
                reply_markup=InlineKeyboardMarkup(rating_keyboard(code_order))
            )
        except Exception as exc:
            logger.error(f"Ошибка отправки запроса отзыва: {exc}")
        await _credit_partner_bonus(context.bot, code_order, code_client)
    except Exception as e:
        logger.error(f"Ошибка отправки кода клиенту: {e}")
        await update.message.reply_text(f"❌ Не удалось отправить код клиенту. Ошибка: {esc(str(e))}")
        return True

    await update.message.reply_text(
        f"✅ Код отправлен клиенту!\n\n📦 Заказ: <b>{code_order}</b>\n📊 Статус: {ORDER_STATUSES['completed']}",
        parse_mode="HTML"
    )
    return True

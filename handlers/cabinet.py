"""
Личный кабинет: заказы, отзывы, реферальная программа, бонусы.
"""

import asyncio
import logging
from html import escape as html_escape

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import BONUS_EXPIRY_MONTHS
from utils import fmt, esc, AWAITING_REVIEW_COMMENT
from keyboards import region_selection_keyboard
from config import FAQ_KEYBOARD
from database import db
from handlers.common import _safe_edit, _get_user_orders_msg

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════
# CALLBACK HANDLERS
# ═══════════════════════════════════════════════

async def handle_my_orders(query, context):
    user_id = query.from_user.id
    ok, msg = await _get_user_orders_msg(user_id)
    await query.edit_message_text(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
        ])
    )
    logger.info(f"Пользователь {user_id} просмотрел заказы")


async def handle_cabinet(query, context):
    keyboard = [
        [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
        [InlineKeyboardButton("📝 Мои отзывы", callback_data="my_reviews")],
        [InlineKeyboardButton("🤝 Реферальная программа", callback_data="ref_program")],
        [InlineKeyboardButton("🎁 Акции и бонусы", callback_data="bonuses")],
    ]
    await _safe_edit(
        query,
        "👤 <b>Личный кабинет</b>\n\nВыберите раздел:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML",
    )


async def handle_ref_program(query, context):
    user_id = query.from_user.id
    completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
    if completed < 1:
        await _safe_edit(
            query,
            "🤝 <b>Реферальная программа</b>\n\n"
            "Для участия в реферальной программе необходимо "
            "оформить хотя бы один заказ.\n\n"
            "Пополните Apple ID — и вы сможете приглашать друзей "
            "и получать бонусные баллы! 🍏",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🍏 Пополнить Apple ID", callback_data="apple_topup")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
            ]),
            parse_mode="HTML",
        )
        return
    ref_count = await asyncio.to_thread(db.get_referral_count, user_id)
    bonus_info = await asyncio.to_thread(db.get_bonus_info, user_id)
    balance = bonus_info["balance"]
    total_earned = bonus_info["total_earned"]

    bot_me = await context.bot.get_me()
    ref_link = f"https://t.me/{bot_me.username}?start=ref_{user_id}"

    text = (
        f"🤝 <b>Реферальная программа</b>\n\n"
        f"Приглашайте друзей и получайте бонусные баллы "
        f"с каждой их покупки!\n\n"
        f"📎 <b>Ваша ссылка:</b>\n<code>{ref_link}</code>\n\n"
        f"👥 Приглашено друзей: <b>{ref_count}</b>\n"
        f"💰 Баланс баллов: <b>{fmt(int(balance))} ₽</b>\n"
        f"📊 Всего заработано: <b>{fmt(int(total_earned))} ₽</b>\n\n"
        f"<b>Как это работает:</b>\n"
        f"1️⃣ Отправьте ссылку другу\n"
        f"2️⃣ Друг получает скидку на первый заказ\n"
        f"3️⃣ Вы получаете бонусные баллы с его покупки\n"
        f"4️⃣ Баллами можно оплатить до 50% заказа\n\n"
        f"<i>1 балл = 1 ₽ · Срок действия — {BONUS_EXPIRY_MONTHS} мес.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("📊 История бонусов", callback_data="bonus_history")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
    ]
    await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def handle_bonuses(query, context):
    user_id = query.from_user.id
    bonus_info = await asyncio.to_thread(db.get_bonus_info, user_id)
    balance = bonus_info["balance"]

    text = (
        f"🎁 <b>Акции и бонусы</b>\n\n"
        f"💰 Ваш баланс: <b>{fmt(int(balance))} баллов</b>\n\n"
        f"<b>Как потратить баллы:</b>\n"
        f"При оформлении заказа вам будет предложено "
        f"списать баллы (до 50% от стоимости).\n\n"
        f"<b>Как заработать:</b>\n"
        f"🤝 Пригласите друга — вы получите баллы с его покупок\n"
        f"💎 Крупные заказы = больше бонусов партнёру\n\n"
        f"<i>⏳ Срок действия баллов — {BONUS_EXPIRY_MONTHS} мес. с момента начисления</i>"
    )
    keyboard = [
        [InlineKeyboardButton("📊 История бонусов", callback_data="bonus_history")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
    ]
    await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def handle_bonus_history(query, context):
    user_id = query.from_user.id
    history = await asyncio.to_thread(db.get_bonus_history, user_id, 10)
    if not history:
        text = "📊 <b>История бонусов</b>\n\nУ вас пока нет бонусных операций."
    else:
        text = "📊 <b>История бонусов</b>\n\n"
        for tx in history:
            sign = "+" if tx["amount"] > 0 else ""
            date = str(tx.get("created_at", ""))[:16]
            desc = tx.get("description") or tx.get("tx_type", "")
            expires = tx.get("expires_at")
            exp_str = f" · до {str(expires)[:10]}" if expires and tx["amount"] > 0 else ""
            text += f"{'🟢' if tx['amount'] > 0 else '🔴'} {sign}{fmt(int(tx['amount']))} ₽ — {esc(desc)}{exp_str}\n   {esc(date)}\n\n"
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="ref_program")],
    ]
    await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


async def handle_my_reviews(query, context):
    user_id = query.from_user.id
    reviews = await asyncio.to_thread(db.get_user_reviews, user_id)
    if not reviews:
        text = "📝 У вас пока нет отзывов."
    else:
        text = "📝 <b>Ваши отзывы:</b>\n\n"
        status_icons = {"pending": "⏳", "approved": "✅", "rejected": "❌"}
        for r in reviews[:10]:
            stars = "⭐" * r["rating"]
            comment = f"\n💬 <i>{html_escape(r['comment'])}</i>" if r.get("comment") else ""
            status = status_icons.get(r.get("status", ""), "❓")
            date = str(r.get("created_at", ""))[:10]
            text += (
                f"🔹 <b>{esc(r.get('order_number', '—'))}</b> · {date}\n"
                f"{stars} {status}{comment}\n"
                f"{'─' * 20}\n"
            )
        text += "\n⏳ — на модерации · ✅ — опубликован · ❌ — отклонён"
    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Все отзывы", url="https://t.me/popolnyaskachat")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")]
        ])
    )


# ═══════════════════════════════════════════════
# TEXT HANDLERS (Reply Keyboard)
# ═══════════════════════════════════════════════

async def handle_cabinet_text(update, context):
    """Обработка нажатия reply-кнопки «Личный кабинет»"""
    keyboard = [
        [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
        [InlineKeyboardButton("📝 Мои отзывы", callback_data="my_reviews")],
        [InlineKeyboardButton("🤝 Реферальная программа", callback_data="ref_program")],
        [InlineKeyboardButton("🎁 Акции и бонусы", callback_data="bonuses")],
    ]
    await update.message.reply_text(
        "👤 <b>Личный кабинет</b>\n\nВыберите раздел:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return True


async def handle_topup_text(update, context):
    """Обработка нажатия reply-кнопки «Пополнить Apple ID»"""
    keyboard = region_selection_keyboard()
    await update.message.reply_text(
        "🍏 Пополнение Apple ID\n\nВыберите регион своего Apple ID:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return True


async def handle_faq_text(update, context):
    """Обработка нажатия reply-кнопки «FAQ»"""
    await update.message.reply_text(
        "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
        reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
    )
    return True

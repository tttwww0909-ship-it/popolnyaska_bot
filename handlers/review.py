"""
Отзывы: оценка, комментарии, модерация, публикация.
"""

import asyncio
import logging
from html import escape as html_escape

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import ADMIN_ID, REVIEWS_CHAT_ID, MAX_REVIEW_LENGTH
from utils import esc, AWAITING_REVIEW_COMMENT
from keyboards import rating_keyboard
from database import db

logger = logging.getLogger(__name__)

REVIEW_GROUP_MSG = "🔗 https://t.me/popolnyaskachat\nЗдесь вы можете найти свой отзыв после прохождения модерации."

SYSTEM_REVIEW_COMMENTS = {
    1: "Тот случай, когда одна звезда — это уже щедрый комплимент.",
    2: "Холодный прием. Как в плохом свидании: искры нет, разговора не вышло, и счет оплачивать не хочется.",
    3: 'Эффект "ну, такое". Вроде и не провал, но и в учебники истории как триумф вы явно не попадете.',
    4: "Почти успех! Но пятую звезду я решил(-а) оставить себе на память :)",
    5: "Молчаливое одобрение — самое ценное. Я не фанат(-ка) лишних слов, когда и так всё супер.",
}


async def send_review_for_moderation(bot, review_id: int, user_id: int, username: str,
                                      order_num: str, rating: int, comment: str | None):
    """Отправляет отзыв админу с кнопками одобрить/отклонить"""
    stars = "⭐" * rating
    comment_text = f"\n💬 Комментарий: <i>{html_escape(comment)}</i>" if comment else ""
    try:
        await bot.send_message(
            ADMIN_ID,
            f"⭐ <b>Новый отзыв (#{review_id})</b>\n\n"
            f"📦 Заказ: <b>{order_num}</b>\n"
            f"👤 Клиент: @{html_escape(username)} (ID: <code>{user_id}</code>)\n"
            f"Оценка: {stars}"
            f"{comment_text}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Одобрить", callback_data=f"review_approve_{review_id}"),
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"review_reject_{review_id}"),
                ]
            ])
        )
    except Exception as e:
        logger.error(f"Ошибка отправки отзыва админу: {e}")


# ═══════════════════════════════════════════════
# CALLBACK HANDLERS
# ═══════════════════════════════════════════════

async def handle_review_rate(query, context):
    parts = query.data.replace("review_rate_", "").split("_", 1)
    rating = int(parts[0])
    order_num = parts[1]
    user_id = query.from_user.id
    AWAITING_REVIEW_COMMENT[user_id] = {"order_num": order_num, "rating": rating}
    stars = "⭐" * rating
    system_comment = SYSTEM_REVIEW_COMMENTS.get(rating, "")
    await query.edit_message_text(
        f"📦 <b>Заказ {order_num}</b>\n\n"
        f"Ваша оценка: {stars}\n\n"
        f"💬 <i>«{system_comment}»</i>\n\n"
        f"Отправить с этим комментарием или написать свой?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Отправить", callback_data=f"review_system_{order_num}_{rating}")],
            [InlineKeyboardButton("✍️ Написать свой отзыв", callback_data=f"review_custom_{order_num}_{rating}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"review_back_{order_num}")],
        ]),
        parse_mode="HTML"
    )


async def handle_review_back(query, context):
    order_num = query.data.replace("review_back_", "")
    user_id = query.from_user.id
    if user_id in AWAITING_REVIEW_COMMENT:
        del AWAITING_REVIEW_COMMENT[user_id]
    await query.edit_message_text(
        f"📦 <b>Заказ {order_num}</b>\n\n"
        f"⭐ Оцените качество нашего сервиса:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(rating_keyboard(order_num))
    )


async def handle_review_system(query, context):
    parts = query.data.replace("review_system_", "").rsplit("_", 1)
    order_num = parts[0]
    rating = int(parts[1])
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.full_name or "Аноним"
    if user_id in AWAITING_REVIEW_COMMENT:
        del AWAITING_REVIEW_COMMENT[user_id]
    system_comment = SYSTEM_REVIEW_COMMENTS.get(rating, "")
    review_id = await asyncio.to_thread(db.add_review, user_id, username, order_num, rating, system_comment)
    stars = "⭐" * rating
    if review_id:
        await send_review_for_moderation(context.bot, review_id, user_id, username, order_num, rating, system_comment)
        await query.edit_message_text(
            f"✅ Спасибо за отзыв! {stars}\n\n"
            f"{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            f"ℹ️ Вы уже оставляли отзыв к этому заказу.\n\n{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )


async def handle_review_custom(query, context):
    parts = query.data.replace("review_custom_", "").rsplit("_", 1)
    order_num = parts[0]
    rating = int(parts[1])
    user_id = query.from_user.id
    AWAITING_REVIEW_COMMENT[user_id] = {"order_num": order_num, "rating": rating}
    stars = "⭐" * rating
    await query.edit_message_text(
        f"📦 <b>Заказ {order_num}</b>\n\n"
        f"Ваша оценка: {stars}\n\n"
        f"✍️ Напишите свой комментарий:",
        parse_mode="HTML"
    )


async def handle_review_no_comment(query, context):
    parts = query.data.replace("review_no_comment_", "").rsplit("_", 1)
    order_num = parts[0]
    rating = int(parts[1])
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.full_name or "Аноним"
    if user_id in AWAITING_REVIEW_COMMENT:
        del AWAITING_REVIEW_COMMENT[user_id]
    system_comment = SYSTEM_REVIEW_COMMENTS.get(rating, "")
    review_id = await asyncio.to_thread(db.add_review, user_id, username, order_num, rating, system_comment)
    stars = "⭐" * rating
    if review_id:
        await send_review_for_moderation(context.bot, review_id, user_id, username, order_num, rating, system_comment)
        await query.edit_message_text(
            f"✅ Спасибо за отзыв! {stars}\n\n"
            f"{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )
    else:
        await query.edit_message_text(
            f"ℹ️ Вы уже оставляли отзыв к этому заказу.\n\n{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )


async def handle_review_skip(query, context):
    order_num = query.data.replace("review_skip_", "")
    user_id = query.from_user.id
    if user_id in AWAITING_REVIEW_COMMENT:
        del AWAITING_REVIEW_COMMENT[user_id]
    await query.edit_message_text(
        "✅ Заказ выполнен! Спасибо за покупку.\n\nЕсли возникнут вопросы — мы всегда на связи.",
        parse_mode="HTML"
    )


async def handle_review_approve(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    review_id = int(query.data.replace("review_approve_", ""))
    await asyncio.to_thread(db.update_review_status, review_id, "approved")
    review = await asyncio.to_thread(db.get_review_by_id, review_id)
    if review:
        stars = "⭐" * review["rating"]
        comment = review.get("comment", "")
        comment_text = f"\n\n<i>«{html_escape(comment)}»</i>" if comment else ""
        username = review.get("username", "Клиент")
        date = str(review.get("created_at", ""))[:10]
        try:
            month_names = {
                "01": "январь", "02": "февраль", "03": "март", "04": "апрель",
                "05": "май", "06": "июнь", "07": "июль", "08": "август",
                "09": "сентябрь", "10": "октябрь", "11": "ноябрь", "12": "декабрь"
            }
            parts = date.split("-")
            if len(parts) == 3:
                month_name = month_names.get(parts[1], parts[1])
                date_display = f"{month_name} {parts[0]}"
            else:
                date_display = date
        except Exception:
            date_display = date
        await context.bot.send_message(
            REVIEWS_CHAT_ID,
            f"{stars}{comment_text}\n\n— {html_escape(username)}, {date_display}",
            parse_mode="HTML"
        )
    await query.edit_message_text(
        f"✅ Отзыв #{review_id} одобрен и опубликован в группу.",
        parse_mode="HTML"
    )


async def handle_review_reject(query, context):
    if query.from_user.id != ADMIN_ID:
        return
    review_id = int(query.data.replace("review_reject_", ""))
    await asyncio.to_thread(db.update_review_status, review_id, "rejected")
    await query.edit_message_text(
        f"❌ Отзыв #{review_id} отклонён.",
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════
# TEXT HANDLER: КОММЕНТАРИЙ К ОТЗЫВУ
# ═══════════════════════════════════════════════

async def handle_review_comment_text(update, context):
    """Обработка текстового комментария к отзыву."""
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    if len(text) > MAX_REVIEW_LENGTH:
        await update.message.reply_text(
            f"❌ Комментарий слишком длинный ({len(text)} символов). "
            f"Максимум — {MAX_REVIEW_LENGTH}. Пожалуйста, сократите текст:"
        )
        return True

    review_data = AWAITING_REVIEW_COMMENT.get(user_id)
    order_num = review_data["order_num"]
    rating = review_data["rating"]
    comment = text
    username = update.message.from_user.username or update.message.from_user.full_name or "Аноним"
    del AWAITING_REVIEW_COMMENT[user_id]
    review_id = await asyncio.to_thread(db.add_review, user_id, username, order_num, rating, comment)
    stars = "⭐" * rating
    if review_id:
        await send_review_for_moderation(context.bot, review_id, user_id, username, order_num, rating, comment)
        await update.message.reply_text(
            f"✅ Спасибо за отзыв! {stars}\n\n"
            f"{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"ℹ️ Вы уже оставляли отзыв к этому заказу.\n\n{REVIEW_GROUP_MSG}",
            parse_mode="HTML"
        )
    return True

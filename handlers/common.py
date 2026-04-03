"""
Общие хелперы, команда /start, обработчик ошибок, периодические задачи.
"""

import asyncio
import logging
from html import escape as html_escape

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from config import (
    ADMIN_ID, CRYPTOPAY_TOKEN,
    REF_THRESHOLD, FIXED_PARTNER_BONUS,
    BONUS_EXPIRY_MONTHS, BONUS_EXPIRY_WARN_DAYS,
    REGION_DISPLAY,
)
from utils import (
    fmt, esc, get_referral_rates, cleanup_memory,
    ORDER_INFO_MAP,
)
from database import db
from cryptopay import CryptoPay

logger = logging.getLogger(__name__)

# CryptoPay singleton (None если токен не задан)
_cryptopay = CryptoPay(CRYPTOPAY_TOKEN) if CRYPTOPAY_TOKEN else None


# ═══════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════

async def _safe_edit(query, text, reply_markup=None, parse_mode=None):
    """edit_message_text с fallback на edit_message_caption для фото-сообщений."""
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        if "There is no text in the message to edit" in str(e):
            try:
                await query.edit_message_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception:
                await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        else:
            raise


async def _get_user_orders_msg(telegram_id: int) -> tuple:
    """Возвращает (ok, msg) с заказами пользователя из SQLite"""
    orders = await asyncio.to_thread(db.get_user_orders_by_telegram_id, telegram_id)
    if not orders:
        return False, "📋 У вас пока нет заказов."
    msg = "📋 <b>Ваши заказы:</b>\n\n"
    for o in orders[:10]:
        status = o.get('status', '—')
        tariff = o.get('tariff', '—')
        rub = o.get('amount_rub', 0)
        date = str(o.get('created_at', ''))[:16]
        msg += (
            f"🔹 <b>{esc(o['order_number'])}</b>\n"
            f"   Тариф: {esc(tariff)}\n"
            f"   Сумма: {fmt(rub)} ₽\n"
            f"   Статус: {esc(status)}\n"
            f"   Дата: {esc(date)}\n\n"
        )
    if len(orders) > 10:
        msg += f"<i>...и ещё {len(orders) - 10} заказ(ов)</i>"
    return True, msg


def _get_order_data(context, order_number: str) -> dict | None:
    """Gets order data from context.user_data or ORDER_INFO_MAP (restart fallback)."""
    order = context.user_data.get("order")
    if order and order.get("number") == order_number:
        return order
    info = ORDER_INFO_MAP.get(order_number)
    if info:
        return {
            "number": order_number,
            "rub": info["rub"],
            "tariff": info.get("tariff", ""),
            "region": info.get("region", "KZ"),
            "service": info.get("service", ""),
            "rub_original": info.get("rub_original", info["rub"]),
            "commission": info.get("commission", 0),
            "partner_pct": info.get("partner_pct", 0),
            "ref_discount": info.get("ref_discount", 0),
        }
    return None


async def _calc_referral_discount(user_id: int, rub: int, commission: float) -> dict:
    """Считает реферальную скидку для пользователя."""
    referrer_id = await asyncio.to_thread(db.get_referrer, user_id)
    if not referrer_id:
        return {"is_referred": False, "discount_pct": 0, "discount_rub": 0,
                "rub_discounted": rub, "partner_pct": 0}
    completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
    if completed > 0:
        partner_pct, _ = get_referral_rates(commission)
        return {"is_referred": True, "discount_pct": 0, "discount_rub": 0,
                "rub_discounted": rub, "partner_pct": partner_pct}
    partner_pct, discount_pct = get_referral_rates(commission)
    discount_rub = int(rub * discount_pct)
    rub_discounted = rub - discount_rub
    return {
        "is_referred": True,
        "discount_pct": discount_pct,
        "discount_rub": discount_rub,
        "rub_discounted": rub_discounted,
        "partner_pct": partner_pct,
    }


async def _credit_partner_bonus(bot, order_number: str, buyer_id: int):
    """Начисляет бонус партнёру после завершения заказа. Отправляет уведомление."""
    referrer_id = await asyncio.to_thread(db.get_referrer, buyer_id)
    if not referrer_id:
        return

    order_info = ORDER_INFO_MAP.get(order_number)
    if not order_info:
        order_db = await asyncio.to_thread(db.get_order, order_number)
        if not order_db:
            return
        amount_rub = order_db.get("amount_rub", 0)
        partner_pct = 0.02
    else:
        amount_rub = order_info.get("rub", 0) + order_info.get("bonus_used", 0)
        partner_pct = order_info.get("partner_pct", 0)

    if partner_pct <= 0:
        commission = order_info.get("commission", 1.15) if order_info else 1.15
        partner_pct, _ = get_referral_rates(commission)

    if amount_rub >= REF_THRESHOLD:
        bonus = round(amount_rub * partner_pct, 2)
    else:
        bonus = FIXED_PARTNER_BONUS

    if bonus <= 0:
        return

    ok = await asyncio.to_thread(
        db.add_bonus, referrer_id, bonus, "referral_bonus",
        order_number, f"Бонус за покупку друга ({order_number})"
    )
    if ok:
        new_balance = await asyncio.to_thread(db.get_bonus_balance, referrer_id)
        try:
            await bot.send_message(
                referrer_id,
                f"🎉 <b>Твой друг совершил покупку!</b>\n\n"
                f"Начислено: <b>+{fmt(int(bonus))} баллов</b>\n"
                f"Баланс: <b>{fmt(int(new_balance))} баллов</b>\n\n"
                f"<i>1 балл = 1 ₽ • Баллами можно оплатить до 50% заказа</i>",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления партнёру {referrer_id}: {e}")
        logger.info(f"Партнёру {referrer_id} начислено {bonus} баллов за заказ {order_number}")


# ═══════════════════════════════════════════════
# КОМАНДА /start
# ═══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню + обработка реферальной ссылки"""
    try:
        user = update.message.from_user
        await asyncio.to_thread(db.add_user, user.id, user.username, user.first_name)

        if context.args:
            arg = context.args[0]
            if arg.startswith("ref_"):
                try:
                    referrer_id = int(arg[4:])
                    if referrer_id != user.id:
                        saved = await asyncio.to_thread(db.add_referral, referrer_id, user.id)
                        if saved:
                            logger.info(f"Реферал: {user.id} приглашён пользователем {referrer_id}")
                except (ValueError, TypeError):
                    pass

        reply_keyboard = ReplyKeyboardMarkup(
            [
                [KeyboardButton("🍏 Пополнить Apple ID")],
                [KeyboardButton("👤 Личный кабинет"), KeyboardButton("❓ FAQ")],
            ],
            resize_keyboard=True
        )
        await update.message.reply_text(
            "Я готов помогать 🙂",
            reply_markup=reply_keyboard
        )
        logger.info(f"Пользователь {user.id} запустил бот")
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")
        await update.message.reply_text("❌ Ошибка. Попробуйте позже.")


# ═══════════════════════════════════════════════
# ПЕРИОДИЧЕСКИЕ ЗАДАЧИ
# ═══════════════════════════════════════════════

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Периодическая очистка устаревших данных из памяти"""
    cleanup_memory()
    logger.info("⏰ Периодическая очистка памяти выполнена")


async def periodic_bonus_expiry(context: ContextTypes.DEFAULT_TYPE):
    """Сжигание просроченных бонусов + уведомление о скором сгорании (раз в сутки)."""
    expired = await asyncio.to_thread(db.expire_bonuses)
    for entry in expired:
        try:
            await context.bot.send_message(
                entry["user_id"],
                f"⏳ <b>Бонусы сгорели</b>\n\n"
                f"Списано: <b>{fmt(int(entry['expired_amount']))} баллов</b>\n"
                f"Срок действия ({BONUS_EXPIRY_MONTHS} мес.) истёк.\n\n"
                f"Приглашайте друзей, чтобы заработать новые баллы! 🤝",
                parse_mode="HTML",
            )
        except Exception:
            pass

    expiring = await asyncio.to_thread(db.get_expiring_soon, BONUS_EXPIRY_WARN_DAYS)
    for entry in expiring:
        try:
            exp_date = str(entry["earliest_expires"])[:10]
            await context.bot.send_message(
                entry["user_id"],
                f"⚠️ <b>Баллы скоро сгорят!</b>\n\n"
                f"У вас <b>{fmt(int(entry['expiring_amount']))} баллов</b>, "
                f"которые сгорят <b>{exp_date}</b>.\n\n"
                f"Используйте их при следующем заказе! 🍏",
                parse_mode="HTML",
            )
        except Exception:
            pass

    if expired or expiring:
        logger.info(f"⏰ Бонусы: сожжено у {len(expired)} польз., предупреждено {len(expiring)} польз.")


# ═══════════════════════════════════════════════
# ОБРАБОТЧИК ОШИБОК
# ═══════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    logger.error(f"Ошибка при обработке запроса: {context.error}", exc_info=context.error)
    try:
        if isinstance(update, Update):
            chat = update.effective_chat
            if chat:
                await context.bot.send_message(
                    chat.id,
                    "⚠️ Произошла ошибка. Попробуйте ещё раз или напишите /start",
                )
    except Exception:
        logger.debug("Не удалось отправить уведомление пользователю")
    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"⚠️ <b>Ошибка бота</b>\n\n<code>{html_escape(str(context.error)[:500])}</code>",
            parse_mode="HTML"
        )
    except Exception:
        logger.debug("Не удалось отправить уведомление об ошибке админу")

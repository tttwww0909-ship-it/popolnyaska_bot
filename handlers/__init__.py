"""
handlers — пакет обработчиков бота.

Экспортирует те же имена, что и старый handlers.py, чтобы
bot.py продолжал работать без изменений:

    from handlers import (
        start, admin, reviews_command, buttons,
        photo_handler, text_handler, periodic_cleanup,
        periodic_bonus_expiry, error_handler,
        handle_cryptopay_webhook,
    )
"""

import logging

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from config import ADMIN_ID, PRICES
from utils import (
    AWAITING_CODE, AWAITING_EMAIL, AWAITING_REVIEW_COMMENT,
)

# Субмодули
from handlers import order, payment, admin as admin_mod, review, cabinet, faq
from handlers.common import (
    _safe_edit,
    start,
    periodic_cleanup,
    periodic_bonus_expiry,
    error_handler,
)
from handlers.crypto_webhook import handle_cryptopay_webhook  # noqa: F401

logger = logging.getLogger(__name__)

# Re-export команд, чтобы bot.py видел admin / reviews_command
admin = admin_mod.admin
reviews_command = admin_mod.reviews_command
# Re-export photo_handler
photo_handler = payment.photo_handler


# ═══════════════════════════════════════════════
# CALLBACK DISPATCHER (buttons)
# ═══════════════════════════════════════════════

# Точные callback_data → обработчик
_EXACT_ROUTES: dict = {
    # Cabinet
    "my_orders": cabinet.handle_my_orders,
    "cabinet": cabinet.handle_cabinet,
    "ref_program": cabinet.handle_ref_program,
    "bonuses": cabinet.handle_bonuses,
    "bonus_history": cabinet.handle_bonus_history,
    "my_reviews": cabinet.handle_my_reviews,
    # Navigation
    "back_to_start": order.handle_back_to_start,
    "new_order": order.handle_back_to_start,
    # FAQ
    "faq_menu": faq.handle_faq_menu,
    "faq_how": faq.handle_faq_how,
    "faq_time": faq.handle_faq_time,
    "faq_payment": faq.handle_faq_payment,
    "faq_commission": faq.handle_faq_commission,
    "faq_problems": faq.handle_faq_problems,
    "faq_safety": faq.handle_faq_safety,
    "faq_usdt_guide": faq.handle_faq_usdt_guide,
    "vip_usdt_guide": faq.handle_vip_usdt_guide,
    "faq_guide": faq.handle_faq_guide,
    "back_to_faq": faq.handle_back_to_faq,
    # Payment
    "back_to_vip_promo": payment.handle_back_to_vip_promo,
    "help_payment": payment.handle_help_payment,
    "back_to_payment": payment.handle_back_to_payment,
    "contact_manager": payment.handle_contact_manager,
    # Order
    "apple_topup": order.handle_apple_topup,
    "apple_custom": order.handle_apple_custom,
    "region_KZ": order.handle_region_kz,
    # Admin
    "admin_orders": admin_mod.handle_admin_orders,
    "stats_general": admin_mod.handle_stats_general,
    "admin_manage_orders": admin_mod.handle_admin_manage_orders,
    "back_to_admin": admin_mod.handle_back_to_admin,
    "admin_broadcast": admin_mod.handle_admin_broadcast,
    "admin_bonus": admin_mod.handle_admin_bonus,
}


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех inline-кнопок — тонкий диспетчер."""
    query = update.callback_query

    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Ошибка при ответе на callback: {e}")
        return

    try:
        data = query.data

        # 1. Точные маршруты
        handler = _EXACT_ROUTES.get(data)
        if handler:
            return await handler(query, context)

        # 2. Регионы (не KZ)
        if data.startswith("region_"):
            return await order.handle_region_gc(query, context)

        # 3. Gift Card тарифы
        if data.startswith("gc_"):
            return await order.handle_gc_tariff(query, context)

        # 4. KZ тарифы (apple_5000, apple_10000, ...)
        if data.startswith("apple_") and data in PRICES:
            return await order.handle_apple_tariff(query, context)

        # 5. Подтверждение заказа
        if data.startswith("confirm_"):
            return await order.handle_confirm(query, context)

        # 6. Списание бонусов (обычный заказ)
        if data.startswith("use_bonus_") and not data.startswith("use_bonus_vip_"):
            return await order.handle_use_bonus(query, context)

        # 7. Оплата
        if data.startswith("pay_yoomoney_"):
            return await payment.handle_pay_yoomoney(query, context)
        if data.startswith("pay_ozon_"):
            return await payment.handle_pay_ozon(query, context)

        # 8. VIP
        if data.startswith("vip_decline_"):
            return await payment.handle_vip_decline(query, context)
        if data.startswith("vip_crypto_"):
            return await payment.handle_vip_crypto(query, context)
        if data.startswith("use_bonus_vip_"):
            return await payment.handle_use_bonus_vip(query, context)
        if data.startswith("skip_bonus_vip_"):
            return await payment.handle_skip_bonus_vip(query, context)

        # 9. Крипта
        if data.startswith("pay_crypto_manual_"):
            return await payment.handle_pay_crypto_manual(query, context)
        if data.startswith("pay_crypto_"):
            return await payment.handle_pay_crypto(query, context)

        # 10. Подтверждение оплаты (paid_*)
        if data.startswith("paid_"):
            return await payment.handle_paid(query, context)

        # 11. Переотправка скриншота
        if data.startswith("resend_screenshot_"):
            return await payment.handle_resend_screenshot(query, context)

        # 12. Админ: управление заказами
        if data.startswith("admin_select_order_"):
            return await admin_mod.handle_admin_select_order(query, context)
        if data.startswith("admin_set_status_"):
            return await admin_mod.handle_admin_set_status(query, context)
        if data.startswith("send_code_"):
            return await admin_mod.handle_send_code(query, context)
        if data.startswith("topup_done_"):
            return await admin_mod.handle_topup_done(query, context)
        if data.startswith("open_client_dm_"):
            return await admin_mod.handle_open_client_dm(query, context)

        # 13. Админ: бонусы
        if data.startswith("admin_bonus_add_"):
            return await admin_mod.handle_admin_bonus_add(query, context)
        if data.startswith("admin_bonus_deduct_"):
            return await admin_mod.handle_admin_bonus_deduct(query, context)

        # 14. Отзывы
        if data.startswith("review_rate_"):
            return await review.handle_review_rate(query, context)
        if data.startswith("review_back_"):
            return await review.handle_review_back(query, context)
        if data.startswith("review_system_"):
            return await review.handle_review_system(query, context)
        if data.startswith("review_custom_"):
            return await review.handle_review_custom(query, context)
        if data.startswith("review_no_comment_"):
            return await review.handle_review_no_comment(query, context)
        if data.startswith("review_skip_"):
            return await review.handle_review_skip(query, context)
        if data.startswith("review_approve_"):
            return await review.handle_review_approve(query, context)
        if data.startswith("review_reject_"):
            return await review.handle_review_reject(query, context)

        logger.warning(f"Неизвестный callback_data: {data}")

    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass
        else:
            logger.error(f"BadRequest в buttons: {e}")
            try:
                await _safe_edit(
                    query,
                    "⚠️ Произошла ошибка. Попробуйте ещё раз или напишите /start",
                    parse_mode="HTML"
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Ошибка в buttons: {e}")
        try:
            await _safe_edit(
                query,
                "⚠️ Произошла ошибка. Попробуйте ещё раз или напишите /start",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════
# TEXT DISPATCHER (text_handler)
# ═══════════════════════════════════════════════

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений — тонкий диспетчер."""
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    try:
        # 1. Код Gift Card от админа
        if user_id == ADMIN_ID and ADMIN_ID in AWAITING_CODE:
            return await admin_mod.handle_admin_code_text(update, context)

        # 2. Рассылка от админа
        if user_id == ADMIN_ID and context.user_data.get("admin_awaiting_broadcast"):
            return await admin_mod.handle_broadcast_text(update, context)

        # 3. Управление бонусами: ввод user_id
        if user_id == ADMIN_ID and context.user_data.get("admin_awaiting_bonus_uid"):
            return await admin_mod.handle_bonus_uid_text(update, context)

        # 4. Управление бонусами: ввод суммы
        if user_id == ADMIN_ID and context.user_data.get("admin_awaiting_bonus_amount"):
            return await admin_mod.handle_bonus_amount_text(update, context)

        # 5. Email Apple ID
        if user_id in AWAITING_EMAIL:
            return await order.handle_email_text(update, context)

        # 6. Комментарий к отзыву
        if user_id in AWAITING_REVIEW_COMMENT:
            return await review.handle_review_comment_text(update, context)

        # 7. Reply-клавиатура
        if text == "🍏 Пополнить Apple ID":
            return await cabinet.handle_topup_text(update, context)
        if text == "❓ FAQ":
            return await cabinet.handle_faq_text(update, context)
        if text in ("📋 Заказы", "👤 Личный кабинет"):
            return await cabinet.handle_cabinet_text(update, context)

        # 8. Кастомная сумма KZT
        if context.user_data.get("awaiting_apple", False):
            return await order.handle_custom_amount_text(update, context)

    except Exception as e:
        logger.error(f"Ошибка в text_handler: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")

"""
Telegram-хэндлеры: команды, кнопки, обработка сообщений и фото.
"""

import asyncio
import logging
from html import escape as html_escape

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from config import (
    ADMIN_ID, PRICES, GIFT_CARD_TARIFFS, REGION_DISPLAY, REGION_COMMISSION,
    ORDER_STATUSES, FAQ_KEYBOARD, YOOMONEY_WALLET, OZON_PAY_URL,
    CRYPTOPAY_TOKEN, REVIEWS_CHAT_ID,
    GIFT_CARD_LABELS, REGION_DESCRIPTIONS, GIFT_CARD_HINTS,
    REF_THRESHOLD, FIXED_PARTNER_BONUS, MAX_BONUS_PAYMENT, REFERRAL_RATES,
    VIP_DISCOUNT, VIP_THRESHOLD, MAX_EMAIL_LENGTH, MAX_REVIEW_LENGTH,
)
from utils import (
    fmt, esc, get_rate, get_usdt_rate, get_kz_commission, get_us_commission, smart_round, check_spam, mark_order_created, generate_order,
    cleanup_memory, validate_email, get_referral_rates, ORDER_USER_MAP, ORDER_INFO_MAP, ORDER_LOCK,
    AWAITING_SCREENSHOT, AWAITING_EMAIL, AWAITING_CODE, AWAITING_REVIEW_COMMENT,
)
from keyboards import (
    region_selection_keyboard, admin_panel_keyboard, rating_keyboard,
    payment_buttons, crypto_payment_text, vip_promo_text, vip_promo_keyboard,
    crypto_payment_buttons, cryptopay_invoice_text,
)
from sheets import add_order_to_sheet, update_payment_method, update_order_status, update_order_amount_in_sheet, find_order_user_in_sheets
from database import db
from cryptopay import CryptoPay

logger = logging.getLogger(__name__)

# CryptoPay singleton (None если токен не задан)
_cryptopay = CryptoPay(CRYPTOPAY_TOKEN) if CRYPTOPAY_TOKEN else None


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


async def _proceed_vip_crypto(query, context, order_number: str, rub_final: int):
    """Общий финал VIP крипто-оплаты: обновляет суммы, создаёт invoice, показывает экран оплаты."""
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


async def _calc_referral_discount(user_id: int, rub: int, commission: float) -> dict:
    """Считает реферальную скидку для пользователя.

    Возвращает dict:
      - is_referred: bool
      - discount_pct: float (0.02 = 2%)
      - discount_rub: int (абсолютная скидка)
      - rub_discounted: int (итого после скидки)
      - partner_pct: float
    """
    referrer_id = await asyncio.to_thread(db.get_referrer, user_id)
    if not referrer_id:
        return {"is_referred": False, "discount_pct": 0, "discount_rub": 0,
                "rub_discounted": rub, "partner_pct": 0}
    # Скидка только на первый заказ
    completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
    if completed > 0:
        # Уже покупал — скидки нет, но партнёр всё ещё получает бонус
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
        # Пробуем из БД
        order_db = await asyncio.to_thread(db.get_order, order_number)
        if not order_db:
            return
        amount_rub = order_db.get("amount_rub", 0)
        partner_pct = 0.02  # fallback
    else:
        # Бонус партнёру считается от полной стоимости заказа (до списания баллов)
        amount_rub = order_info.get("rub", 0) + order_info.get("bonus_used", 0)
        partner_pct = order_info.get("partner_pct", 0)

    if partner_pct <= 0:
        # Получаем ставку по комиссии
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
# КОМАНДЫ
# ═══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню + обработка реферальной ссылки"""
    try:
        user = update.message.from_user
        # Регистрируем/обновляем пользователя
        await asyncio.to_thread(db.add_user, user.id, user.username, user.first_name)

        # Deep link: /start ref_<telegram_id>
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
# КНОПКИ (callback_query)
# ═══════════════════════════════════════════════

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка всех кнопок"""
    query = update.callback_query

    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Ошибка при ответе на callback: {e}")
        return

    try:
        # === МОИ ЗАКАЗЫ ===
        if query.data == "my_orders":
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
            return

        # === ЛИЧНЫЙ КАБИНЕТ (inline) ===
        if query.data == "cabinet":
            user_id = query.from_user.id
            completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
            keyboard = [
                [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
                [InlineKeyboardButton("📝 Мои отзывы", callback_data="my_reviews")],
            ]
            if completed >= 1:
                keyboard.append([InlineKeyboardButton("🤝 Реферальная программа", callback_data="ref_program")])
            keyboard.append([InlineKeyboardButton("🎁 Акции и бонусы", callback_data="bonuses")])
            await _safe_edit(
                query,
                "👤 <b>Личный кабинет</b>\n\nВыберите раздел:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )
            return

        # === РЕФЕРАЛЬНАЯ ПРОГРАММА ===
        if query.data == "ref_program":
            user_id = query.from_user.id
            completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
            if completed < 1:
                await query.answer("Реферальная программа доступна после первого пополнения", show_alert=True)
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
                f"<i>1 балл = 1 ₽</i>"
            )
            keyboard = [
                [InlineKeyboardButton("📊 История бонусов", callback_data="bonus_history")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
            ]
            await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return

        # === АКЦИИ И БОНУСЫ ===
        if query.data == "bonuses":
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
                f"💎 Крупные заказы = больше бонусов партнёру"
            )
            keyboard = [
                [InlineKeyboardButton("📊 История бонусов", callback_data="bonus_history")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="cabinet")],
            ]
            await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return

        # === ИСТОРИЯ БОНУСОВ ===
        if query.data == "bonus_history":
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
                    text += f"{'🟢' if tx['amount'] > 0 else '🔴'} {sign}{fmt(int(tx['amount']))} ₽ — {esc(desc)}\n   {esc(date)}\n\n"
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад", callback_data="ref_program")],
            ]
            await _safe_edit(query, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return

        # === НАЗАД В ГЛАВНОЕ МЕНЮ ===
        if query.data in ("back_to_start", "new_order"):
            keyboard = [
                [InlineKeyboardButton("🍏 Пополнить Apple ID", callback_data="apple_topup")],
                [InlineKeyboardButton("� Личный кабинет", callback_data="cabinet")],
                [InlineKeyboardButton("❓ FAQ", callback_data="faq_menu")],
            ]
            await query.edit_message_text(
                "🍏 Главное меню\n\nВыберите действие или используйте кнопки внизу:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === FAQ ===
        if query.data == "faq_menu":
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        if query.data == "faq_how":
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
            return

        if query.data == "faq_time":
            await query.edit_message_text(
                "🔹 Сколько времени занимает?\n\n"
                "🇰🇿 Пополнение Apple ID (Казахстан) — до 30 минут после подтверждения оплаты.\n\n"
                "🎁 Gift Card (США, ОАЭ, Турция, Сауд. Аравия) — до 15 минут. "
                "Бот отправит вам код для пополнения Apple ID.\n\n"
                "В редких случаях может занять больше времени из-за высокой нагрузки.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_payment":
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
            return

        if query.data == "faq_commission":
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
            return

        if query.data == "faq_problems":
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
            return

        if query.data == "faq_safety":
            await query.edit_message_text(
                "🔹 Безопасно ли это?\n\n"
                "Да! Это абсолютно безопасно. Мы используем минимум личной информации "
                "(UserID, username и информацию о заказе) для статистики, а вашу почту Apple ID "
                "используем только для пополнения и не сохраняем!\n\n"
                "Ваши данные в безопасности!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_usdt_guide":
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
            return

        if query.data == "vip_usdt_guide":
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
            return

        if query.data == "faq_guide":
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
            return

        if query.data == "back_to_faq":
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        if query.data == "back_to_vip_promo":
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
            return

        # === ПОПОЛНЕНИЕ APPLE ID — ВЫБОР РЕГИОНА ===
        if query.data == "apple_topup":
            keyboard = region_selection_keyboard()
            context.user_data.pop("awaiting_apple", None)
            await query.edit_message_text(
                "🍏 Пополнение Apple ID\n\nВыберите регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === РЕГИОН КАЗАХСТАН ===
        elif query.data == "region_KZ":
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
            return

        # === РЕГИОНЫ С ГИФТ-КАРТАМИ ===
        elif query.data in ("region_TR", "region_AE", "region_SA", "region_US"):
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
            return

        # === ВЫБОР ТАРИФА ГИФТ-КАРТЫ ===
        elif query.data.startswith("gc_"):
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

            # Реферальная скидка
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
            return

        elif query.data == "apple_custom":
            context.user_data["awaiting_apple"] = True
            await query.edit_message_text(
                "Введите сумму пополнения Apple ID (5 000–45 000 KZT)",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="region_KZ")]])
            )

        # === ВЫБОР ТАРИФА APPLE ID (KZ) ===
        elif query.data.startswith("apple_") and query.data in PRICES:
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

            # Реферальная скидка
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

        # === ПОДТВЕРЖДЕНИЕ ЗАКАЗА ===
        elif query.data.startswith("confirm_"):
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
                    # Промо-экран для крупных заказов (Вариант Б)
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
                    # Предложение списания баллов
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

        # === СПИСАНИЕ БАЛЛОВ ===
        elif query.data.startswith("use_bonus_"):
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

            # Списываем баллы
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
            # Обновляем сумму в Sheets и БД
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

        # === ОПЛАТА ЮMONEY ===
        elif query.data.startswith("pay_yoomoney_"):
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

        # === ОПЛАТА OZON ===
        elif query.data.startswith("pay_ozon_"):
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

        # === ОТКАЗ ОТ VIP-ЗАКАЗА ===
        elif query.data.startswith("vip_decline_"):
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

        # === ОПЛАТА КРИПТОЙ ===
        elif query.data.startswith("vip_crypto_"):
            order_number = query.data.replace("vip_crypto_", "")
            order = _get_order_data(context, order_number)
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return
            # Применяем VIP-скидку к сумме заказа
            rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
            context.user_data["rub_discounted"] = rub_discounted

            # Проверяем бонусные баллы
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

            # Нет баллов — сразу к оплате
            await _proceed_vip_crypto(query, context, order_number, rub_discounted)

        elif query.data.startswith("use_bonus_vip_"):
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

        elif query.data.startswith("skip_bonus_vip_"):
            order_number = query.data.replace("skip_bonus_vip_", "")
            order = _get_order_data(context, order_number)
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return
            rub_discounted = context.user_data.get("rub_discounted", round(order["rub"] * (1 - VIP_DISCOUNT)))
            await _proceed_vip_crypto(query, context, order_number, rub_discounted)

        elif query.data.startswith("pay_crypto_") and not query.data.startswith("pay_crypto_manual_"):
            order_number = query.data.replace("pay_crypto_", "")
            order = _get_order_data(context, order_number)
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return
            amount_usdt = context.user_data.get("amount_usdt", 0)

            # CryptoPay: создаём invoice если токен задан
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

        # === РУЧНАЯ КРИПТО-ОПЛАТА (Bybit UID) — fallback от CryptoPay ===
        elif query.data.startswith("pay_crypto_manual_"):
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

        # === ПОДТВЕРЖДЕНИЕ ОПЛАТЫ (все методы) ===
        elif query.data.startswith("paid_crypto_") or query.data.startswith("paid_yoomoney_") or query.data.startswith("paid_ozon_"):
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

        # === FAQ ОПЛАТЫ ===
        elif query.data == "help_payment":
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

        # === ВЕРНУТЬСЯ К ОПЛАТЕ ===
        elif query.data == "back_to_payment":
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

        # === КОНТАКТ С МЕНЕДЖЕРОМ ===
        elif query.data == "contact_manager":
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

        # === КНОПКА ДЛЯ АДМИНА — ОТКРЫТЬ ЛС ===
        elif query.data.startswith("open_client_dm_"):
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

        # === АДМИН-ПАНЕЛЬ: ЗАКАЗЫ ===
        elif query.data == "admin_orders":
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

        # === ОБЩАЯ СТАТИСТИКА ===
        elif query.data == "stats_general":
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

        # === АДМИН: УПРАВЛЕНИЕ СТАТУСАМИ ===
        elif query.data == "admin_manage_orders":
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

        # === АДМИН: ВЫБОР НОВОГО СТАТУСА ===
        elif query.data.startswith("admin_select_order_"):
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

        # === АДМИН: ПРИМЕНЕНИЕ НОВОГО СТАТУСА ===
        elif query.data.startswith("admin_set_status_"):
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
                            # Начисляем бонус партнёру
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

                # Формируем детали заказа для админа
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

        # === ОТПРАВКА КОДА КЛИЕНТУ (Gift Card) ===
        elif query.data.startswith("send_code_"):
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

        # === ПОПОЛНЕНИЕ ПРОИЗВЕДЕНО ===
        elif query.data.startswith("topup_done_"):
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
                # Начисляем бонус партнёру
                await _credit_partner_bonus(context.bot, order_num, client_id)

            await query.edit_message_text(
                f"✅ Заказ <b>{order_num}</b> выполнен!\n\nКлиент уведомлён о пополнении.",
                parse_mode="HTML"
            )
            logger.info(f"Админ отметил пополнение выполненным: {order_num}")

        # === ОТЗЫВЫ ===
        elif query.data.startswith("review_rate_"):
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

        elif query.data.startswith("review_back_"):
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

        elif query.data.startswith("review_system_"):
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

        elif query.data.startswith("review_custom_"):
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

        elif query.data.startswith("review_no_comment_"):
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

        elif query.data.startswith("review_skip_"):
            order_num = query.data.replace("review_skip_", "")
            user_id = query.from_user.id
            if user_id in AWAITING_REVIEW_COMMENT:
                del AWAITING_REVIEW_COMMENT[user_id]
            await query.edit_message_text(
                "✅ Заказ выполнен! Спасибо за покупку.\n\nЕсли возникнут вопросы — мы всегда на связи.",
                parse_mode="HTML"
            )

        # === МОДЕРАЦИЯ ОТЗЫВОВ (АДМИН) ===
        elif query.data.startswith("review_approve_"):
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

        elif query.data.startswith("review_reject_"):
            if query.from_user.id != ADMIN_ID:
                return
            review_id = int(query.data.replace("review_reject_", ""))
            await asyncio.to_thread(db.update_review_status, review_id, "rejected")
            await query.edit_message_text(
                f"❌ Отзыв #{review_id} отклонён.",
                parse_mode="HTML"
            )

        # === МОИ ОТЗЫВЫ ===
        elif query.data == "my_reviews":
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

        # === ПЕРЕОТПРАВКА СКРИНШОТА ===
        elif query.data.startswith("resend_screenshot_"):
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

        # === НАЗАД В АДМИН-ПАНЕЛЬ ===
        elif query.data == "back_to_admin":
            if query.from_user.id != ADMIN_ID:
                return
            keyboard = admin_panel_keyboard()
            await query.edit_message_text("⚙️ Админ-панель", reply_markup=InlineKeyboardMarkup(keyboard))

    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка в buttons: {e}")
            try:
                await _safe_edit(query, "❌ Произошла ошибка. Попробуйте позже.")
            except Exception:
                logger.debug("Не удалось отправить сообщение об ошибке (BadRequest fallback)")
    except Exception as e:
        logger.error(f"Ошибка в buttons: {e}")
        try:
            await _safe_edit(query, "❌ Произошла ошибка. Попробуйте позже.")
        except Exception:
            logger.debug("Не удалось отправить сообщение об ошибке (general fallback)")


# ═══════════════════════════════════════════════
# ОБРАБОТКА ФОТО И ТЕКСТА
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


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.message.from_user.id
    text = update.message.text.strip()

    try:
        # === КОД ОТ АДМИНА (Gift Card) ===
        if user_id == ADMIN_ID and ADMIN_ID in AWAITING_CODE:
            code_data = AWAITING_CODE[ADMIN_ID]
            code_order = code_data["order_num"]
            code_client = code_data["client_id"]
            gift_code = text
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
                # Начисляем бонус партнёру
                await _credit_partner_bonus(context.bot, code_order, code_client)
            except Exception as e:
                logger.error(f"Ошибка отправки кода клиенту: {e}")
                await update.message.reply_text(f"❌ Не удалось отправить код клиенту. Ошибка: {esc(str(e))}")
                return

            await update.message.reply_text(
                f"✅ Код отправлен клиенту!\n\n📦 Заказ: <b>{code_order}</b>\n📊 Статус: {ORDER_STATUSES['completed']}",
                parse_mode="HTML"
            )
            return

        # === ПОЧТА APPLE ID ===
        if user_id in AWAITING_EMAIL:
            order_number = AWAITING_EMAIL.get(user_id)
            if not order_number:
                del AWAITING_EMAIL[user_id]
                return
            email = text.lower()

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
                return
            else:
                await update.message.reply_text("❌ Некорректный email. Пожалуйста, отправьте правильную почту Apple ID:")
                return

        # === КОММЕНТАРИЙ К ОТЗЫВУ ===
        if user_id in AWAITING_REVIEW_COMMENT:
            if len(text) > MAX_REVIEW_LENGTH:
                await update.message.reply_text(
                    f"❌ Комментарий слишком длинный ({len(text)} символов). "
                    f"Максимум — {MAX_REVIEW_LENGTH}. Пожалуйста, сократите текст:"
                )
                return
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
            return

        # === REPLY KEYBOARD КНОПКИ ===
        if text == "🍏 Пополнить Apple ID":
            keyboard = region_selection_keyboard()
            await update.message.reply_text(
                "🍏 Пополнение Apple ID\n\nВыберите регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if text == "❓ FAQ":
            await update.message.reply_text(
                "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        if text == "📋 Заказы" or text == "👤 Личный кабинет":
            user_id = update.message.from_user.id
            completed = await asyncio.to_thread(db.count_user_completed_orders, user_id)
            keyboard = [
                [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
                [InlineKeyboardButton("📝 Мои отзывы", callback_data="my_reviews")],
            ]
            if completed >= 1:
                keyboard.append([InlineKeyboardButton("🤝 Реферальная программа", callback_data="ref_program")])
            keyboard.append([InlineKeyboardButton("🎁 Акции и бонусы", callback_data="bonuses")])
            await update.message.reply_text(
                "👤 <b>Личный кабинет</b>\n\nВыберите раздел:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        # === ВВОД КАСТОМНОЙ СУММЫ ===
        if context.user_data.get("awaiting_apple", False):
            try:
                amount = int(text)
                if not (5000 <= amount <= 45000):
                    await update.message.reply_text("❌ Неверный диапазон.\n\nВведите сумму от 5 000 до 45 000 KZT (шаг 500):")
                    return
                if amount % 500 != 0:
                    await update.message.reply_text("❌ Сумма должна быть кратна 500 KZT.\n\nНапример: 5 000, 5 500, 6 000 и т.д.")
                    return

                can_create, spam_msg = check_spam(user_id)
                if not can_create:
                    await update.message.reply_text(spam_msg)
                    return

                rate = await asyncio.to_thread(get_rate)
                if not rate:
                    await update.message.reply_text("❌ Ошибка получения курса. Попробуйте позже.")
                    return

                commission = get_kz_commission(amount)
                commission_pct = round((commission - 1) * 100)
                rub = smart_round(int(amount * rate * commission))
                order_number = await asyncio.to_thread(generate_order)
                if not order_number:
                    await update.message.reply_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                    return

                user = update.message.from_user
                tariff_name = f"{fmt(amount)} KZT"

                # Реферальная скидка
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
                return
            except ValueError:
                await update.message.reply_text("❌ Введите только число.\n\nПовторите ввод суммы (5 000–45 000 KZT):")
                return

    except Exception as e:
        logger.error(f"Ошибка в text_handler: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")


async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Периодическая очистка устаревших данных из памяти"""
    cleanup_memory()
    logger.info("⏰ Периодическая очистка памяти выполнена")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    logger.error(f"Ошибка при обработке запроса: {context.error}", exc_info=context.error)
    # Уведомить пользователя
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
    # Уведомить админа
    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"⚠️ <b>Ошибка бота</b>\n\n<code>{html_escape(str(context.error)[:500])}</code>",
            parse_mode="HTML"
        )
    except Exception:
        logger.debug("Не удалось отправить уведомление об ошибке админу")


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

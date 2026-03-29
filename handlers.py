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
    BYBIT_UID, BSC_ADDRESS, TRC20_ADDRESS,
)
from utils import (
    fmt, get_rate, get_usdt_rate, get_kz_commission, check_spam, mark_order_created, generate_order,
    cleanup_memory, validate_email, ORDER_USER_MAP, ORDER_INFO_MAP, ORDER_LOCK,
    AWAITING_SCREENSHOT, AWAITING_EMAIL, AWAITING_CODE, AWAITING_REVIEW_COMMENT,
)
from sheets import add_order_to_sheet, update_payment_method, update_order_status, find_order_user_in_sheets
from database import db

logger = logging.getLogger(__name__)


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
            f"🔹 <b>{o['order_number']}</b>\n"
            f"   Тариф: {tariff}\n"
            f"   Сумма: {fmt(rub)} ₽\n"
            f"   Статус: {status}\n"
            f"   Дата: {date}\n\n"
        )
    if len(orders) > 10:
        msg += f"<i>...и ещё {len(orders) - 10} заказ(ов)</i>"
    return True, msg


async def send_review_for_moderation(bot, review_id: int, user_id: int, username: str,
                                      order_num: str, rating: int, comment: str | None):
    """Отправляет отзыв админу для информации"""
    stars = "⭐" * rating
    comment_text = f"\n💬 Комментарий: <i>{comment}</i>" if comment else ""
    try:
        await bot.send_message(
            ADMIN_ID,
            f"⭐ <b>Новый отзыв</b>\n\n"
            f"📦 Заказ: <b>{order_num}</b>\n"
            f"👤 Клиент: @{username} (ID: <code>{user_id}</code>)\n"
            f"Оценка: {stars}"
            f"{comment_text}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки отзыва админу: {e}")


# ═══════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню"""
    try:
        reply_keyboard = ReplyKeyboardMarkup(
            [
                [KeyboardButton("🍏 Пополнить Apple ID")],
                [KeyboardButton("📋 Заказы"), KeyboardButton("❓ FAQ")],
            ],
            resize_keyboard=True
        )
        await update.message.reply_text(
            "Рад видеть тебя! Я готов помочь с пополнением твоего Apple ID\n\n"
            "Что для этого нужно?\n\n"
            "1️⃣ Нажми \"🍏 Пополнить Apple ID\"\n"
            "2️⃣ Выбери регион своего Apple ID и тариф\n"
            "   (для Казахстана можно ввести свою сумму)\n"
            "3️⃣ Выбери способ оплаты и оплати\n"
            "4️⃣ Отправь данные менеджеру и жди пополнения\n\n"
            "Всё проще, чем кажется! 😉",
            reply_markup=reply_keyboard
        )
        logger.info(f"Пользователь {update.message.from_user.id} запустил бот")
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

        keyboard = [
            [InlineKeyboardButton("📊 Общая статистика", callback_data="stats_general")],
            [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
            [InlineKeyboardButton("🔄 Изменить статус заказа", callback_data="admin_manage_orders")]
        ]
        await update.message.reply_text(
            "⚙️ Админ панель",
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
            comment = f"\n💬 <i>{r['comment']}</i>" if r.get("comment") else ""
            date = str(r.get("created_at", ""))[:10]
            text += (
                f"<b>{r['order_number']}</b> · @{r['username']} · {date}\n"
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
            await query.edit_message_text(msg, parse_mode="HTML")
            logger.info(f"Пользователь {user_id} просмотрел заказы")
            return

        # === НАЗАД В ГЛАВНОЕ МЕНЮ ===
        if query.data in ("back_to_start", "new_order"):
            keyboard = [
                [InlineKeyboardButton("🍏 Пополнить Apple ID", callback_data="apple_topup")],
                [InlineKeyboardButton("📋 Мои заказы", callback_data="my_orders")],
                [InlineKeyboardButton("❓ FAQ", callback_data="faq_menu")],
            ]
            await query.edit_message_text(
                "🍏 Главное меню\n\nВыбери действие или используй кнопки внизу:",
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
                "Мы помогаем пополнить Apple ID граждан РФ методом смены региона.\n\n"
                "🇰🇿 <b>Казахстан</b> — пополнение напрямую на ваш Apple ID. "
                "Вы отправляете почту, привязанную к Apple ID, а мы отправляем "
                "подарочный код для пополнения Apple ID.\n\n"
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
                "🇰🇿 Пополнение Apple ID (Казахстан) — 15-30 минут после подтверждения оплаты.\n\n"
                "🎁 Gift Card (США, ОАЭ, Турция, СА) — до 15 минут. "
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
                "  — Bybit (адрес, USDT BSC/BEP20)\n"
                "  — Телеграм кошелёк (USDT TRC20)\n\n"
                "⚠️ Для заказов свыше 8 500 ₽ доступна только оплата криптой.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "faq_commission":
            await query.edit_message_text(
                "🔹 Какая комиссия сервиса?\n\n"
                "Комиссия зависит от региона:\n\n"
                "🇰🇿 Казахстан — 15%\n"
                "🇺🇸 США — 15%\n"
                "🇦🇪 ОАЭ — 15%\n"
                "🇸🇦 Саудовская Аравия — 15%\n"
                "🇹🇷 Турция — 10%",
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
                "(UserID, username и информацию о заказе) для статистики, а вашу Apple почту "
                "используем непосредственно для пополнения Apple ID напрямую и не сохраняем!\n\n"
                "Ваши данные в безопасности!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к FAQ", callback_data="back_to_faq")]])
            )
            return

        if query.data == "back_to_faq":
            await query.edit_message_text(
                "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        # === ПОПОЛНЕНИЕ APPLE ID — ВЫБОР РЕГИОНА ===
        if query.data == "apple_topup":
            keyboard = [
                [InlineKeyboardButton("🇺🇸 США", callback_data="region_US")],
                [InlineKeyboardButton("🇦🇪 ОАЭ", callback_data="region_AE")],
                [InlineKeyboardButton("🇹🇷 Турция", callback_data="region_TR")],
                [InlineKeyboardButton("🇰🇿 Казахстан", callback_data="region_KZ")],
                [InlineKeyboardButton("🇸🇦 Саудовская Аравия", callback_data="region_SA")]
            ]
            await query.edit_message_text(
                "🍏 Пополнение Apple ID\n\nВыбери регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === РЕГИОН КАЗАХСТАН ===
        elif query.data == "region_KZ":
            keyboard = [
                [InlineKeyboardButton("🍏 5 000 KZT", callback_data="apple_5000")],
                [InlineKeyboardButton("🍏 10 000 KZT", callback_data="apple_10000")],
                [InlineKeyboardButton("🍏 15 000 KZT", callback_data="apple_15000")],
                [InlineKeyboardButton("✏️ Ввести свою сумму", callback_data="apple_custom")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="apple_topup")]
            ]
            await query.edit_message_text(
                "🇰🇿 Казахстан — Пополнение Apple ID\n\nВыбери сумму пополнения:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # === РЕГИОНЫ С ГИФТ-КАРТАМИ ===
        elif query.data in ("region_TR", "region_AE", "region_SA", "region_US"):
            region_code = query.data.replace("region_", "")
            tariffs = GIFT_CARD_TARIFFS[region_code]
            region_name = REGION_DISPLAY[region_code]
            keyboard = []
            for amount, currency, usdt_cost in tariffs:
                keyboard.append([InlineKeyboardButton(
                    f"🍏 {fmt(amount)} {currency}",
                    callback_data=f"gc_{region_code}_{amount}"
                )])
            keyboard.append([InlineKeyboardButton("⬅️ Назад к регионам", callback_data="apple_topup")])
            await query.edit_message_text(
                f"{region_name} — Gift Card Apple\n\nВыбери номинал гифт-карты:",
                reply_markup=InlineKeyboardMarkup(keyboard)
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
            commission = REGION_COMMISSION.get(region_code, 1.15)
            commission_pct = round((commission - 1) * 100)
            rub = int(t_usdt * usdt_rate * commission)
            order_number = await asyncio.to_thread(generate_order)
            if not order_number:
                await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                return

            region_name = REGION_DISPLAY[region_code]
            tariff_name = f"{fmt(t_amount)} {t_currency}"
            context.user_data["order"] = {
                "number": order_number,
                "service": f"Gift Card ({region_name})",
                "tariff": tariff_name,
                "kzt": 0,
                "rub": rub,
                "region": region_code,
                "user": user
            }
            keyboard = [
                [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"region_{region_code}")]
            ]
            await query.edit_message_text(
                f"📦 Информация о заказе\n\n"
                f"Номер заказа: <b>{order_number}</b>\n"
                f"Регион: <b>{region_name}</b>\n"
                f"Тариф: <b>{tariff_name} Gift Card</b>\n"
                f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (комиссия {commission_pct}%)",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            logger.info(f"Пользователь {user.id} создал заказ {order_number} (Gift Card {region_code} {tariff_name})")
            return

        elif query.data == "apple_custom":
            context.user_data["awaiting_apple"] = True
            await query.edit_message_text("Введите сумму пополнения Apple ID (5 000–45 000 KZT)")

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
            rub = int(amount * rate * commission)
            order_number = await asyncio.to_thread(generate_order)
            if not order_number:
                await query.edit_message_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                return

            tariff_name = f"{fmt(amount)} KZT"
            context.user_data["order"] = {
                "number": order_number,
                "service": "Apple ID",
                "tariff": tariff_name,
                "kzt": amount,
                "rub": rub,
                "region": "KZ",
                "user": user
            }
            keyboard = [
                [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data="apple_topup")]
            ]
            await query.edit_message_text(
                f"📦 Информация о заказе\n\n"
                f"Номер заказа: <b>{order_number}</b>\n"
                f"Тариф: <b>{tariff_name}</b>\n"
                f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (сервисный сбор {commission_pct}%)",
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
                    "kzt": order["kzt"],
                    "rub": order["rub"],
                    "region": order.get("region", "KZ")
                }

                if not await asyncio.to_thread(add_order_to_sheet, order_data):
                    await query.edit_message_text("❌ Ошибка сохранения заказа. Попробуйте позже.")
                    return

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
                    "kzt": order["kzt"],
                    "rub": order["rub"],
                    "usdt": amount_usdt or 0,
                    "region": order.get("region", "KZ")
                }
                context.user_data["current_order_number"] = order_number

                if order["rub"] > 8500:
                    pay_buttons = [
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]
                else:
                    pay_buttons = [
                        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]

                usdt_suffix = f" (~{amount_usdt} USDT)" if amount_usdt else ""
                await query.edit_message_text(
                    f"✅ Заявка сформирована!\n\n"
                    f"Номер заказа: <b>{order['number']}</b>\n"
                    f"Тариф: <b>{order['tariff']}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b>{usdt_suffix}\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup(pay_buttons),
                    parse_mode="HTML"
                )
                logger.info(f"Заказ {order_number} — выбор способа оплаты")
            finally:
                ORDER_LOCK.pop(order_number, None)

        # === ОПЛАТА ЮMONEY ===
        elif query.data.startswith("pay_yoomoney_"):
            order_number = query.data.replace("pay_yoomoney_", "")
            order = context.user_data.get("order")
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
            order = context.user_data.get("order")
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

        # === ОПЛАТА КРИПТОЙ ===
        elif query.data.startswith("pay_crypto_"):
            order_number = query.data.replace("pay_crypto_", "")
            order = context.user_data.get("order")
            if not order:
                await query.edit_message_text("⚠️ Данные заказа потеряны. Создайте новый заказ.")
                return
            amount_usdt = context.user_data.get("amount_usdt", 0)
            await query.edit_message_text(
                f"💎 Оплата криптой (USDT)\n\n"
                f"📦 Заказ: <b>{order_number}</b>\n"
                f"💰 К оплате: <b>{amount_usdt} USDT</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📲 <b>Способ 1: Bybit (перевод по UID)</b>\n"
                f"UID: <code>{BYBIT_UID}</code>\n"
                f"Сумма: <b>{amount_usdt} USDT</b>\n\n"
                f"📲 <b>Способ 2: Bybit (адрес)</b>\n"
                f"Адрес: <code>{BSC_ADDRESS}</code>\n"
                f"Сеть: <b>BSC (BEP20)</b> | Монета: <b>USDT</b>\n\n"
                f"📲 <b>Способ 3: Телеграм кошелёк</b>\n"
                f"Адрес: <code>{TRC20_ADDRESS}</code>\n"
                f"Сеть: <b>Tron (TRC20)</b> | Монета: <b>USDT</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"После перевода нажмите «✅ Я оплатил» и отправьте скриншот подтверждения.",
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
            order = context.user_data.get("order")
            AWAITING_SCREENSHOT[user_id] = order_number

            order_info = ORDER_INFO_MAP.get(order_number, {})
            region_display = REGION_DISPLAY.get(order_info.get('region', ''), order_info.get('region', '—'))

            if pay_label == "Crypto":
                amount_usdt = context.user_data.get("amount_usdt", 0)
                sum_display = f"<b>{amount_usdt} USDT</b>"
            else:
                amount_rub = order["rub"] if order else 0
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
                "  — Bybit (адрес, USDT BSC/BEP20)\n"
                "  — Телеграм кошелёк (USDT TRC20)\n\n"
                "⚠️ Для заказов свыше 8 500 ₽ доступна только оплата криптой.\n\n"
                "⏱ <b>Сроки:</b>\n"
                "🇰🇿 Казахстан — 15-30 минут | 🎁 Gift Card — до 15 минут\n\n"
                "💰 <b>Комиссия:</b> 15% (🇹🇷 Турция — 10%)\n\n"
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
            order = context.user_data.get("order")
            amount_usdt = context.user_data.get("amount_usdt", 0)
            if order:
                if order['rub'] > 8500:
                    pay_buttons = [
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]
                else:
                    pay_buttons = [
                        [InlineKeyboardButton("💳 ЮMoney", callback_data=f"pay_yoomoney_{order_number}")],
                        [InlineKeyboardButton("💳 OZON банк", callback_data=f"pay_ozon_{order_number}")],
                        [InlineKeyboardButton("💎 Криптой (USDT)", callback_data=f"pay_crypto_{order_number}")],
                        [InlineKeyboardButton("❓ FAQ", callback_data="help_payment")]
                    ]
                usdt_suffix = f" (~{amount_usdt} USDT)" if amount_usdt else ""
                await query.edit_message_text(
                    f"✅ Заявка сформирована!\n\n"
                    f"Номер заказа: <b>{order_number}</b>\n"
                    f"Тариф: <b>{order['tariff']}</b>\n"
                    f"Сумма: <b>{fmt(order['rub'])} ₽</b>{usdt_suffix}\n\n"
                    f"Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup(pay_buttons),
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

            username_link = f'<a href="https://t.me/{username}">@{username}</a>' if query.from_user.username else first_name
            client_info = f'Имя: <a href="tg://user?id={user_id}">{first_name}</a>\n' \
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
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"Клиент запросил связь с менеджером.\n"
                    f"Если у него есть заказ:\n"
                    f"  • Регион: {client_region or 'уточнить'}\n"
                    f"  • Тариф: {client_tariff or 'уточнить'}\n\n"
                    f"Напишите ему в личку и помогите!"
                )
            else:
                msg = (
                    f'💬 Откройте ЛС с клиентом <a href="tg://user?id={client_id}">{client_name}</a> (ID: <code>{client_id}</code>)\n\n'
                    f"<b>📦 Заказ:</b>\n"
                    f"  • Номер: {reason}\n"
                    f"  • Регион: {client_region}\n"
                    f"  • Тариф: {client_tariff}\n\n"
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
                    "📦 Нет заказов пока.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]])
                )
                return

            msg = "📦 Последние заказы (последние 10):\n\n"
            for o in orders:
                order_info_cached = ORDER_INFO_MAP.get(o['order_number'], {})
                region_code = order_info_cached.get('region', '')
                if region_code:
                    region_display = REGION_DISPLAY.get(region_code, region_code)
                else:
                    region_display = o.get('service', '—')
                msg += f"🔹 <b>{o['order_number']}</b>\n"
                msg += f"   Статус: {o.get('status', '—')}\n"
                msg += f"   Сервис: {region_display}\n"
                msg += f"   Тариф: {o.get('tariff', '—')}\n"
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
                            "completed": "✅ Ваш заказ выполнен! Спасибо за покупку.",
                            "cancelled": "❌ Ваш заказ отменён. Если есть вопросы — свяжитесь с поддержкой."
                        }
                        client_message = status_messages.get(new_status, f"Статус заказа изменён на: {status_name}")

                    try:
                        if new_status == "completed":
                            rating_keyboard = [
                                [
                                    InlineKeyboardButton("1⭐️", callback_data=f"review_rate_1_{order_num}"),
                                    InlineKeyboardButton("2⭐️", callback_data=f"review_rate_2_{order_num}"),
                                    InlineKeyboardButton("3⭐️", callback_data=f"review_rate_3_{order_num}"),
                                    InlineKeyboardButton("4⭐️", callback_data=f"review_rate_4_{order_num}"),
                                    InlineKeyboardButton("5⭐️", callback_data=f"review_rate_5_{order_num}"),
                                ],
                                [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"review_skip_{order_num}")]
                            ]
                            await context.bot.send_message(
                                user_id,
                                f"📦 <b>Заказ {order_num} выполнен!</b>\n\n"
                                f"✅ Спасибо за покупку!\n\n"
                                f"⭐ Оцените качество нашего сервиса:",
                                parse_mode="HTML",
                                reply_markup=InlineKeyboardMarkup(rating_keyboard)
                            )
                        else:
                            await context.bot.send_message(
                                user_id,
                                f"📦 <b>Заказ {order_num}</b>\n\n{client_message}",
                                parse_mode="HTML"
                            )
                        logger.info(f"Клиент {user_id} уведомлён о статусе {status_name}")
                    except Exception as e:
                        logger.error(f"Ошибка уведомления клиента о статусе: {e}")

                await query.edit_message_text(
                    f"✅ Статус заказа <b>{order_num}</b> изменён на: <b>{status_name}</b>\n\n"
                    f"{'✉️ Клиент уведомлён.' if user_id else '⚠️ Не удалось уведомить клиента (ID не найден).'}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 К списку заказов", callback_data="admin_manage_orders")],
                        [InlineKeyboardButton("⬅️ В админ-панель", callback_data="back_to_admin")]
                    ]),
                    parse_mode="HTML"
                )
                logger.info(f"Админ изменил статус {order_num} на {status_name}")
            else:
                await query.edit_message_text(
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
                    rating_keyboard = [
                        [
                            InlineKeyboardButton("1⭐️", callback_data=f"review_rate_1_{order_num}"),
                            InlineKeyboardButton("2⭐️", callback_data=f"review_rate_2_{order_num}"),
                            InlineKeyboardButton("3⭐️", callback_data=f"review_rate_3_{order_num}"),
                            InlineKeyboardButton("4⭐️", callback_data=f"review_rate_4_{order_num}"),
                            InlineKeyboardButton("5⭐️", callback_data=f"review_rate_5_{order_num}"),
                        ],
                        [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"review_skip_{order_num}")]
                    ]
                    await context.bot.send_message(
                        client_id,
                        f"🎉 <b>Пополнение выполнено!</b>\n\n"
                        f"📦 Заказ: <b>{order_num}</b>\n\n"
                        f"✅ Ваш Apple ID успешно пополнен!\n"
                        f"Спасибо, что воспользовались нашим сервисом! 🍏\n\n"
                        f"⭐ Оцените качество нашего сервиса:",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(rating_keyboard)
                    )
                except Exception as e:
                    logger.error(f"Ошибка уведомления клиента о пополнении: {e}")

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
            await query.edit_message_text(
                f"📦 <b>Заказ {order_num}</b>\n\n"
                f"Ваша оценка: {stars}\n\n"
                f"✍️ Напишите комментарий или нажмите «Пропустить»:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"review_no_comment_{order_num}_{rating}")]
                ]),
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
            review_id = await asyncio.to_thread(db.add_review, user_id, username, order_num, rating, None)
            stars = "⭐" * rating
            await send_review_for_moderation(context.bot, review_id, user_id, username, order_num, rating, None)
            await query.edit_message_text(
                f"✅ Спасибо за отзыв! {stars}\n\nВаше мнение помогает нам становиться лучше.",
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

        # === ПЕРЕОТПРАВКА СКРИНШОТА ===
        elif query.data.startswith("resend_screenshot_"):
            order_number = query.data.replace("resend_screenshot_", "")
            user_id = query.from_user.id
            AWAITING_SCREENSHOT[user_id] = order_number
            await query.edit_message_text(
                f"📸 <b>Отправьте новый скриншот</b>\n\n"
                f"Заказ: <b>{order_number}</b>\n\n"
                f"Отправьте скриншот подтверждения оплаты прямо в этот чат.",
                parse_mode="HTML"
            )
            logger.info(f"Клиент {user_id} запросил переотправку скриншота для {order_number}")

        # === НАЗАД В АДМИН-ПАНЕЛЬ ===
        elif query.data == "back_to_admin":
            if query.from_user.id != ADMIN_ID:
                return
            keyboard = [
                [InlineKeyboardButton("📊 Общая статистика", callback_data="stats_general")],
                [InlineKeyboardButton("📦 Последние заказы", callback_data="admin_orders")],
                [InlineKeyboardButton("🔄 Изменить статус заказа", callback_data="admin_manage_orders")]
            ]
            await query.edit_message_text("⚙️ Админ панель", reply_markup=InlineKeyboardMarkup(keyboard))

    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка в buttons: {e}")
            try:
                await query.edit_message_text("❌ Произошла ошибка. Попробуйте позже.")
            except:
                pass
    except Exception as e:
        logger.error(f"Ошибка в buttons: {e}")
        try:
            await query.edit_message_text("❌ Произошла ошибка. Попробуйте позже.")
        except:
            pass


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
                    f"Имя: {update.message.from_user.first_name or 'Неизвестно'}\n"
                    f"Ник: @{update.message.from_user.username if update.message.from_user.username else 'нет'}\n"
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
                    f"Активируйте код в App Store / iTunes.\n"
                    f"Спасибо, что воспользовались нашим сервисом! 🍏",
                    parse_mode="HTML"
                )
                logger.info(f"Код отправлен клиенту {code_client} для заказа {code_order}")
                try:
                    rating_keyboard = [
                        [
                            InlineKeyboardButton("1⭐️", callback_data=f"review_rate_1_{code_order}"),
                            InlineKeyboardButton("2⭐️", callback_data=f"review_rate_2_{code_order}"),
                            InlineKeyboardButton("3⭐️", callback_data=f"review_rate_3_{code_order}"),
                            InlineKeyboardButton("4⭐️", callback_data=f"review_rate_4_{code_order}"),
                            InlineKeyboardButton("5⭐️", callback_data=f"review_rate_5_{code_order}"),
                        ],
                        [InlineKeyboardButton("⏭️ Пропустить", callback_data=f"review_skip_{code_order}")]
                    ]
                    await context.bot.send_message(
                        code_client,
                        f"⭐ Оцените качество нашего сервиса:",
                        reply_markup=InlineKeyboardMarkup(rating_keyboard)
                    )
                except Exception as re:
                    logger.error(f"Ошибка отправки запроса отзыва: {re}")
            except Exception as e:
                logger.error(f"Ошибка отправки кода клиенту: {e}")
                await update.message.reply_text(f"❌ Не удалось отправить код клиенту. Ошибка: {e}")
                return

            await update.message.reply_text(
                f"✅ Код отправлен клиенту!\n\n📦 Заказ: <b>{code_order}</b>\n📊 Статус: Выполнен",
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
                        f"<code>{email}</code>\n\n"
                        f"👤 Клиент:\nИмя: {user_name}\nНик: {username}\nID: <code>{user_id}</code>",
                        reply_markup=InlineKeyboardMarkup(admin_keyboard),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки почты админу: {e}")

                await update.message.reply_text(
                    f"✅ Почта <b>{email}</b> получена!\n\n"
                    f"📦 Заказ: <b>{order_number}</b>\n\n"
                    f"Мы пополним ваш Apple ID в ближайшее время. Ожидайте уведомления!",
                    parse_mode="HTML"
                )
                logger.info(f"Клиент {user_id} отправил почту {email} для заказа {order_number}")
                return
            else:
                await update.message.reply_text("❌ Некорректный email. Пожалуйста, отправьте правильную почту Apple ID:")
                return

        # === КОММЕНТАРИЙ К ОТЗЫВУ ===
        if user_id in AWAITING_REVIEW_COMMENT:
            review_data = AWAITING_REVIEW_COMMENT.get(user_id)
            order_num = review_data["order_num"]
            rating = review_data["rating"]
            comment = text
            username = update.message.from_user.username or update.message.from_user.full_name or "Аноним"
            del AWAITING_REVIEW_COMMENT[user_id]
            review_id = await asyncio.to_thread(db.add_review, user_id, username, order_num, rating, comment)
            stars = "⭐" * rating
            await send_review_for_moderation(context.bot, review_id, user_id, username, order_num, rating, comment)
            await update.message.reply_text(
                f"✅ Спасибо за отзыв! {stars}\n\nВаше мнение помогает нам становиться лучше.",
                parse_mode="HTML"
            )
            return

        # === REPLY KEYBOARD КНОПКИ ===
        if text == "🍏 Пополнить Apple ID":
            keyboard = [
                [InlineKeyboardButton("🇺🇸 США", callback_data="region_US")],
                [InlineKeyboardButton("🇦🇪 ОАЭ", callback_data="region_AE")],
                [InlineKeyboardButton("🇹🇷 Турция", callback_data="region_TR")],
                [InlineKeyboardButton("🇰🇿 Казахстан", callback_data="region_KZ")],
                [InlineKeyboardButton("🇸🇦 Саудовская Аравия", callback_data="region_SA")]
            ]
            await update.message.reply_text(
                "🍏 Пополнение Apple ID\n\nВыбери регион своего Apple ID:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        if text == "❓ FAQ":
            await update.message.reply_text(
                "❓ Часто задаваемые вопросы\n\nВыберите интересующий вопрос:",
                reply_markup=InlineKeyboardMarkup(FAQ_KEYBOARD)
            )
            return

        if text == "📋 Заказы":
            ok, msg = await _get_user_orders_msg(user_id)
            await update.message.reply_text(msg, parse_mode="HTML")
            logger.info(f"Пользователь {user_id} просмотрел заказы")
            return

        # === ВВОД КАСТОМНОЙ СУММЫ ===
        if context.user_data.get("awaiting_apple", False):
            try:
                amount = int(text)
                if not (5000 <= amount <= 45000):
                    await update.message.reply_text("❌ Неверный диапазон.\n\nВведите сумму от 5 000 до 45 000 KZT:")
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
                rub = int(amount * rate * commission)
                order_number = await asyncio.to_thread(generate_order)
                if not order_number:
                    await update.message.reply_text("❌ Ошибка генерации заказа. Попробуйте позже.")
                    return

                user = update.message.from_user
                tariff_name = f"{fmt(amount)} KZT"

                context.user_data["order"] = {
                    "number": order_number,
                    "service": "Apple ID",
                    "tariff": tariff_name,
                    "kzt": amount,
                    "rub": rub,
                    "region": "KZ",
                    "user": user
                }
                keyboard = [
                    [InlineKeyboardButton("✅ Продолжить", callback_data=f"confirm_{order_number}")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="apple_topup")]
                ]
                await update.message.reply_text(
                    f"📦 Информация о заказе\n\n"
                    f"Номер заказа: <b>{order_number}</b>\n"
                    f"Тариф: <b>{tariff_name}</b>\n"
                    f"Сумма к оплате: <b>{fmt(rub)} ₽</b> (сервисный сбор {commission_pct}%)",
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
    logger.error(f"Ошибка при обработке запроса: {context.error}")

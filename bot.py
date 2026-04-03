"""
ПополняшкаBot — точка входа.
"""

import asyncio
import json

from aiohttp import web
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters,
)

from config import TOKEN, CRYPTOPAY_TOKEN, CRYPTOPAY_WEBHOOK_PATH, CRYPTOPAY_WEBHOOK_PORT, logger
from cryptopay import CryptoPay
from handlers import (
    start, admin, reviews_command, buttons,
    photo_handler, text_handler, periodic_cleanup, error_handler,
    handle_cryptopay_webhook,
)
from utils import (
    AWAITING_SCREENSHOT, AWAITING_EMAIL, AWAITING_CODE, AWAITING_REVIEW_COMMENT,
)

_cryptopay = CryptoPay(CRYPTOPAY_TOKEN) if CRYPTOPAY_TOKEN else None


def _build_app():
    """Собираем Telegram Application."""
    for store in (AWAITING_SCREENSHOT, AWAITING_EMAIL, AWAITING_CODE, AWAITING_REVIEW_COMMENT):
        store.load()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("reviews", reviews_command))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(periodic_cleanup, interval=3600, first=60)
    return app


_MAX_WEBHOOK_BODY = 65536  # 64 KB максимум для webhook-запроса


async def _cryptopay_webhook_handler(request: web.Request) -> web.Response:
    """HTTP-обработчик вебхуков от CryptoPay."""
    try:
        if request.content_length and request.content_length > _MAX_WEBHOOK_BODY:
            logger.warning("CryptoPay webhook: body too large (%s bytes)", request.content_length)
            return web.Response(status=413, text="Payload too large")
        body = await request.read()
        if len(body) > _MAX_WEBHOOK_BODY:
            logger.warning("CryptoPay webhook: body too large (%s bytes)", len(body))
            return web.Response(status=413, text="Payload too large")
        signature = request.headers.get("crypto-pay-api-signature", "")

        if _cryptopay and not _cryptopay.verify_webhook(body, signature):
            logger.warning("CryptoPay webhook: invalid signature")
            return web.Response(status=403, text="Invalid signature")

        payload = json.loads(body)
        bot = request.app["telegram_bot"]
        await handle_cryptopay_webhook(bot, payload)
        return web.Response(status=200, text="OK")
    except Exception as e:
        logger.error("CryptoPay webhook error: %s", e)
        return web.Response(status=500, text="Internal error")


async def _run_with_webhook():
    """Запуск бота (long-polling) + CryptoPay webhook-сервера (aiohttp) параллельно."""
    tg_app = _build_app()

    # aiohttp для CryptoPay вебхуков
    web_app = web.Application()
    web_app.router.add_post(CRYPTOPAY_WEBHOOK_PATH, _cryptopay_webhook_handler)

    # Стартуем Telegram app чтобы получить bot instance
    await tg_app.initialize()
    await tg_app.start()
    web_app["telegram_bot"] = tg_app.bot

    # Запускаем polling
    updater = tg_app.updater
    await updater.start_polling(drop_pending_updates=False)

    # Запускаем HTTP-сервер
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CRYPTOPAY_WEBHOOK_PORT)
    await site.start()
    logger.info("CryptoPay webhook server started on port %s%s", CRYPTOPAY_WEBHOOK_PORT, CRYPTOPAY_WEBHOOK_PATH)

    # Ждём бесконечно (Ctrl+C для остановки)
    try:
        await asyncio.Event().wait()
    finally:
        if _cryptopay:
            await _cryptopay.close()
        await updater.stop()
        await tg_app.stop()
        await tg_app.shutdown()
        await runner.cleanup()


def main():
    if CRYPTOPAY_TOKEN:
        logger.info("Бот запускается с CryptoPay webhook-сервером…")
        asyncio.run(_run_with_webhook())
    else:
        # Без CryptoPay — простой polling как раньше
        tg_app = _build_app()
        logger.info("Бот запускается (без CryptoPay)…")
        tg_app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()

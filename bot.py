"""
ПополняшкаBot — точка входа.
"""

from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters,
)

from config import TOKEN, logger
from database import db
from handlers import (
    start, admin, reviews_command, buttons,
    photo_handler, text_handler, periodic_cleanup, error_handler,
)


def main():
    db.init_db()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("reviews", reviews_command))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(periodic_cleanup, interval=3600, first=60)

    logger.info("Бот запускается…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

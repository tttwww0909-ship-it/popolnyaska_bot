# Copilot Instructions — popolnyaska_bot

## Проект
Telegram-бот "Пополняшка" для пополнения Apple ID. Python + aiogram-совместимый python-telegram-bot.

## Стек
- Python 3.11+
- python-telegram-bot (telegram.ext)
- Google Sheets (gspread + oauth2client) — основная таблица заказов
- SQLite (orders.db) — локальная БД через database.py
- dotenv для конфигурации (.env)

## Структура
- `bot.py` — основной файл бота (хендлеры, логика)
- `database.py` — модуль БД (класс Database, экземпляр db)
- `service_account.json` — ключ Google Sheets (не коммитить)
- `.env` — токены и реквизиты (не коммитить)

## Деплой
- Сервер: Linux VPS, путь `/opt/popolnyaska-bot/`
- Сервис: `systemd` → `popolnyaska-bot.service`
- Деплой: `git push` → на сервере `cd /opt/popolnyaska-bot && git pull && sudo systemctl restart popolnyaska-bot`

## Правила
- Язык интерфейса бота: русский
- Валюта: KZT (основная), RUB (конвертация через ЦБ РФ + 15% комиссия)
- Тарифы: 5 000 / 10 000 / 15 000 KZT, кастом 2 000–45 000 KZT
- Google Sheets колонки: Номер ордера, User_ID, Username, Услуга, Тариф, Цена KZT, Цена RUB, Способ оплаты, Дата создания, Статус заявки
- ADMIN_ID — единственный админ, проверять через `update.message.from_user.id != ADMIN_ID`
- Не выводить токены и секреты в логи/stdout

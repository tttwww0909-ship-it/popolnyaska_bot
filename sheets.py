"""
Интеграция с Google Sheets: добавление заказов, обновление статусов, статистика.
"""

import time
import logging
import threading
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import ORDER_STATUSES, REGION_DISPLAY, MONTH_NAMES
from utils import fmt, ORDER_INFO_MAP
from database import db

logger = logging.getLogger(__name__)

# === КЭШИ ===
_sheet_cache = {"sheet": None, "time": 0}
_SHEET_CACHE_TTL = 300

_last_stats_update = 0
_STATS_UPDATE_MIN_INTERVAL = 60


def get_sheet():
    """Получает объект таблицы с кэшированием (5 мин)"""
    now = time.time()
    if _sheet_cache["sheet"] and now - _sheet_cache["time"] < _SHEET_CACHE_TTL:
        return _sheet_cache["sheet"]
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open("popolnyaska_bot").sheet1
        _sheet_cache["sheet"] = sheet
        _sheet_cache["time"] = now
        logger.debug("Подключение к Google Sheets успешно")
        return sheet
    except Exception as e:
        logger.error(f"Ошибка подключения к Google Sheets: {e}")
        return None


def _run_stats_update():
    """Запускает обновление статистики в фоновом потоке (не чаще раза в минуту)"""
    try:
        global _last_stats_update
        now = time.time()
        if now - _last_stats_update < _STATS_UPDATE_MIN_INTERVAL:
            return
        _last_stats_update = now
        threading.Thread(target=update_stats_sheet, daemon=True).start()
    except Exception as e:
        logger.error(f"Ошибка запуска обновления статистики: {e}")


def update_stats_sheet():
    """Полностью обновляет лист 'Статистика' в Google Sheets"""
    try:
        main_sheet = get_sheet()
        if not main_sheet:
            logger.warning("⚠️ Не удалось получить основной лист для статистики")
            return
        spreadsheet = main_sheet.spreadsheet

        try:
            stats_ws = spreadsheet.worksheet("Статистика")
        except gspread.exceptions.WorksheetNotFound:
            stats_ws = spreadsheet.add_worksheet(title="Статистика", rows=100, cols=6)

        records = main_sheet.get_all_records()
        if not records:
            stats_ws.clear()
            stats_ws.update("A1", [["Нет данных для статистики"]])
            return

        today_str = datetime.now().strftime("%d.%m.%Y")

        total = len(records)
        unique_users = len(set(str(r.get("User_ID", "")) for r in records if r.get("User_ID")))

        statuses = {}
        for r in records:
            s = r.get("Статус", "—")
            statuses[s] = statuses.get(s, 0) + 1

        completed_records = [r for r in records if r.get("Статус") == "Выполнен"]
        revenue = sum(int(r.get("Сумма RUB", 0) or 0) for r in completed_records)
        avg_check = int(revenue / len(completed_records)) if completed_records else 0
        paid_count = statuses.get("Оплачен", 0) + statuses.get("Выполнен", 0)
        conversion = int(paid_count / total * 100) if total > 0 else 0

        today_records = [r for r in records if str(r.get("Дата", "")).startswith(today_str)]
        today_orders = len(today_records)
        today_completed = [r for r in today_records if r.get("Статус") == "Выполнен"]
        today_revenue = sum(int(r.get("Сумма RUB", 0) or 0) for r in today_completed)
        today_users = len(set(str(r.get("User_ID", "")) for r in today_records if r.get("User_ID")))

        months_data = {}
        for r in records:
            date_str = str(r.get("Дата", ""))
            if not date_str:
                continue
            try:
                parts = date_str.split(" ")[0].split(".")
                month_key = (int(parts[2]), int(parts[1]))
            except (IndexError, ValueError):
                continue
            if month_key not in months_data:
                months_data[month_key] = {"orders": 0, "users": set(), "revenue": 0, "paid": 0}
            months_data[month_key]["orders"] += 1
            months_data[month_key]["users"].add(str(r.get("User_ID", "")))
            if r.get("Статус") in ("Оплачен", "Выполнен"):
                months_data[month_key]["paid"] += 1
            if r.get("Статус") == "Выполнен":
                months_data[month_key]["revenue"] += int(r.get("Сумма RUB", 0) or 0)

        regions_data = {}
        for r in records:
            reg = r.get("Регион", "—") or "—"
            if reg not in regions_data:
                regions_data[reg] = {"orders": 0, "users": set(), "revenue": 0, "paid": 0}
            regions_data[reg]["orders"] += 1
            regions_data[reg]["users"].add(str(r.get("User_ID", "")))
            if r.get("Статус") in ("Оплачен", "Выполнен"):
                regions_data[reg]["paid"] += 1
            if r.get("Статус") == "Выполнен":
                regions_data[reg]["revenue"] += int(r.get("Сумма RUB", 0) or 0)

        payment_methods = {}
        for r in records:
            pm = r.get("Способ оплаты", "") or ""
            if pm:
                payment_methods[pm] = payment_methods.get(pm, 0) + 1

        rows = []
        rows.append(["═══ ЗАКАЗЫ ═══", ""])
        rows.append(["Всего заказов:", total])
        rows.append(["Уникальных клиентов:", unique_users])
        rows.append(["Ожидает оплаты:", statuses.get("Ожидает оплаты", 0)])
        rows.append(["Оплачено:", statuses.get("Оплачен", 0)])
        rows.append(["Выполнено:", statuses.get("Выполнен", 0)])
        rows.append(["Отменено:", statuses.get("Отменён", 0)])
        rows.append(["", ""])

        rows.append(["═══ ФИНАНСЫ ═══", ""])
        rows.append(["Выручка (₽):", fmt(revenue)])
        rows.append(["Средний чек (₽):", fmt(avg_check)])
        rows.append(["Конверсия (%):", conversion])
        rows.append(["", ""])

        rows.append(["═══ СЕГОДНЯ ═══", ""])
        rows.append(["Заказов сегодня:", today_orders])
        rows.append(["Выручка сегодня (₽):", fmt(today_revenue)])
        rows.append(["Клиентов сегодня:", today_users])
        rows.append(["", ""])

        rows.append(["═══ ПО МЕСЯЦАМ ═══", "", "", "", ""])
        rows.append(["Месяц", "Заказов", "Клиентов", "Выручка (₽)", "Конверсия (%)"])
        for key in sorted(months_data.keys()):
            year, month = key
            m = months_data[key]
            m_conv = int(m["paid"] / m["orders"] * 100) if m["orders"] > 0 else 0
            month_name = f"{MONTH_NAMES.get(month, month)} {year}"
            rows.append([month_name, m["orders"], len(m["users"]), fmt(m["revenue"]), m_conv])
        rows.append(["", ""])

        rows.append(["═══ ПО РЕГИОНАМ ═══", "", "", "", ""])
        rows.append(["Регион", "Заказов", "Клиентов", "Выручка (₽)", "Конверсия (%)"])
        for reg_code in ["US", "AE", "TR", "KZ", "SA"]:
            if reg_code in regions_data:
                rd = regions_data[reg_code]
                r_conv = int(rd["paid"] / rd["orders"] * 100) if rd["orders"] > 0 else 0
                reg_name = REGION_DISPLAY.get(reg_code, reg_code)
                rows.append([reg_name, rd["orders"], len(rd["users"]), fmt(rd["revenue"]), r_conv])
        for reg_code, rd in regions_data.items():
            if reg_code not in ["US", "AE", "TR", "KZ", "SA"]:
                r_conv = int(rd["paid"] / rd["orders"] * 100) if rd["orders"] > 0 else 0
                rows.append([reg_code, rd["orders"], len(rd["users"]), fmt(rd["revenue"]), r_conv])
        rows.append(["", ""])

        rows.append(["═══ СПОСОБЫ ОПЛАТЫ ═══", ""])
        for pm_name in ["ЮMoney", "OZON банк", "Crypto"]:
            rows.append([f"{pm_name}:", payment_methods.get(pm_name, 0)])
        for pm_name, count in payment_methods.items():
            if pm_name not in ["ЮMoney", "OZON банк", "Crypto"]:
                rows.append([f"{pm_name}:", count])

        stats_ws.clear()
        stats_ws.update(f"A1:E{len(rows)}", rows, value_input_option="RAW")
        logger.info("✅ Лист 'Статистика' обновлён")

    except Exception as e:
        logger.error(f"⚠️ Ошибка обновления листа Статистика: {e}")


def add_order_to_sheet(order_data):
    """Добавляет заказ в таблицу и в БД"""
    try:
        current_sheet = get_sheet()
        if current_sheet:
            for _attempt in range(3):
                try:
                    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
                    current_sheet.append_row([
                        order_data["number"],
                        order_data["user_id"],
                        order_data["username"],
                        order_data.get("region", "KZ"),
                        order_data["tariff"],
                        order_data["rub"],
                        "",
                        current_date,
                        ORDER_STATUSES["pending"]
                    ])
                    added_cell = current_sheet.find(order_data["number"])
                    if added_cell:
                        db.set_order_sheets_row(order_data["number"], added_cell.row)
                    logger.info(f"Заказ {order_data['number']} добавлен в Google Sheets")
                    break
                except gspread.exceptions.APIError as e:
                    if _attempt < 2:
                        logger.warning(f"⚠️ gspread APIError (попытка {_attempt+1}/3): {e}")
                        import time as _time
                        _time.sleep(2 ** _attempt)
                        _sheet_cache["sheet"] = None
                        _sheet_cache["time"] = 0
                        current_sheet = get_sheet()
                    else:
                        logger.error(f"❌ gspread APIError после 3 попыток: {e}")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка добавления в Google Sheets: {e}")
                    break

        user_id = db.add_user(
            telegram_id=order_data["user_id"],
            username=order_data["username"],
            first_name=order_data.get("first_name", "Клиент")
        )
        if not user_id:
            logger.error("Ошибка добавления пользователя в БД")
            return False

        order_id = db.add_order(
            order_number=order_data["number"],
            user_id=user_id,
            service=order_data["service"],
            tariff=order_data["tariff"],
            amount_kzt=order_data["kzt"],
            amount_rub=order_data["rub"],
            payment_id=None
        )
        if not order_id:
            logger.error("Ошибка добавления заказа в БД")
            return False

        logger.info(f"✅ Заказ {order_data['number']} добавлен в БД")
        _run_stats_update()
        return True

    except Exception as e:
        logger.error(f"Ошибка при добавлении заказа: {e}")
        return False


def update_payment_method(order_number, payment_method):
    """Записывает способ оплаты в Google Sheets (колонка G)"""
    try:
        current_sheet = get_sheet()
        if current_sheet:
            row = db.get_order_sheets_row(order_number)
            if row:
                current_sheet.update_cell(row, 7, payment_method)
            else:
                cell = current_sheet.find(order_number)
                if cell:
                    current_sheet.update_cell(cell.row, 7, payment_method)
                    db.set_order_sheets_row(order_number, cell.row)
            logger.info(f"✅ Способ оплаты {payment_method} записан для {order_number}")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка записи способа оплаты: {e}")


def update_order_status(order_number, new_status):
    """Обновляет статус заказа в БД и Google Sheets"""
    try:
        success = db.update_order_status(order_number, new_status)
        if not success:
            logger.warning(f"Заказ {order_number} не найден в БД")
            return False

        current_sheet = get_sheet()
        if current_sheet:
            try:
                row = db.get_order_sheets_row(order_number)
                if row:
                    current_sheet.update_cell(row, 9, new_status)
                else:
                    cell = current_sheet.find(order_number)
                    if cell:
                        current_sheet.update_cell(cell.row, 9, new_status)
                        db.set_order_sheets_row(order_number, cell.row)
                logger.info(f"✅ Статус {order_number} обновлён в Google Sheets")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обновления Google Sheets: {e}")

        logger.info(f"✅ Статус {order_number} изменён на {new_status}")
        _run_stats_update()
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении статуса: {e}")
        return False


def find_order_user_in_sheets(order_num):
    """Ищет telegram_id и region заказа в Google Sheets (для fallback)"""
    try:
        current_sheet = get_sheet()
        if current_sheet:
            cell = current_sheet.find(order_num)
            if cell:
                row_vals = current_sheet.row_values(cell.row)
                if len(row_vals) >= 4:
                    return int(row_vals[1]), row_vals[3]
    except Exception as e:
        logger.error(f"Ошибка получения user_id из Sheets: {e}")
    return None, ""

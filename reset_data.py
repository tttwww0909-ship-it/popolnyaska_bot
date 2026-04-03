"""
Скрипт полной очистки данных: SQLite + Google Sheets.
Сбрасывает счётчик ордеров на 1000 (первый ордер будет ORD-1001).
"""

import sqlite3
import gspread

DATABASE_FILE = "orders.db"

TABLES_TO_CLEAR = [
    "orders",
    "users",
    "payments",
    "action_log",
    "reviews",
    "pending_states",
    "referrals",
    "bonus_balance",
    "bonus_transactions",
]


def reset_sqlite():
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    for table in TABLES_TO_CLEAR:
        c.execute(f"DELETE FROM {table}")
        print(f"  ✅ {table}: удалено {c.rowcount} строк")
    # Сброс счётчика — следующий ордер будет ORD-1001
    c.execute("UPDATE counters SET value = 1000 WHERE name = 'order_number'")
    print(f"  ✅ counters: order_number = 1000")
    # Сброс автоинкрементов
    c.execute("DELETE FROM sqlite_sequence")
    print(f"  ✅ sqlite_sequence сброшены")
    conn.commit()
    conn.close()
    print("SQLite очищен.\n")


def reset_sheets():
    client = gspread.service_account(filename="service_account.json")
    sheet = client.open("popolnyaska_bot").sheet1
    all_values = sheet.get_all_values()
    data_rows = len(all_values) - 1  # минус заголовок
    if data_rows > 0:
        # Очищаем содержимое строк 2..N (заголовок остаётся)
        last_row = len(all_values)
        last_col = len(all_values[0]) if all_values[0] else 9
        from gspread.utils import rowcol_to_a1
        range_str = f"A2:{rowcol_to_a1(last_row, last_col)}"
        sheet.batch_clear([range_str])
        # Удаляем лишние строки (оставляем заголовок + 1 пустую)
        if last_row > 2:
            sheet.delete_rows(3, last_row)
        print(f"  ✅ Google Sheets: очищено {data_rows} строк данных (заголовок сохранён)")
    else:
        print("  ✅ Google Sheets: данных нет")
    print("Google Sheets очищен.\n")


if __name__ == "__main__":
    print("=== СБРОС ДАННЫХ ===\n")
    print("[1/2] SQLite...")
    reset_sqlite()
    print("[2/2] Google Sheets...")
    reset_sheets()
    print("=== ГОТОВО. Следующий ордер: ORD-1001 ===")

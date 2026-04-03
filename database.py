"""
Модуль для работы с базой данных SQLite.
Здесь хранятся все пользователи и заказы.
"""

import json
import sqlite3
import logging
from typing import Optional, List, Dict

from config import ORDER_STATUSES

logger = logging.getLogger(__name__)

DATABASE_FILE = "orders.db"


class Database:
    """Класс для работы с БД"""

    def __init__(self):
        self.db_file = DATABASE_FILE
        self.init_db()

    def _connect(self, row_factory=False):
        conn = sqlite3.connect(self.db_file, timeout=5)
        if row_factory:
            conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Инициализирует БД и создаёт таблицы"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        telegram_id INTEGER UNIQUE NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_number TEXT UNIQUE NOT NULL,
                        user_id INTEGER NOT NULL,
                        service TEXT NOT NULL,
                        tariff TEXT NOT NULL,
                        amount_kzt INTEGER NOT NULL,
                        amount_rub INTEGER NOT NULL,
                        status TEXT DEFAULT 'new',
                        payment_id TEXT UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(user_id) REFERENCES users(id)
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS payments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        payment_id TEXT UNIQUE NOT NULL,
                        order_id INTEGER NOT NULL,
                        amount REAL NOT NULL,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(order_id) REFERENCES orders(id)
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        action TEXT NOT NULL,
                        details TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS reviews (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        username TEXT,
                        order_number TEXT,
                        rating INTEGER NOT NULL,
                        comment TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS pending_states (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        state_type TEXT NOT NULL,
                        key_id INTEGER NOT NULL,
                        value_json TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(state_type, key_id)
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS counters (
                        name TEXT PRIMARY KEY,
                        value INTEGER NOT NULL DEFAULT 0
                    )
                ''')
                c.execute('''
                    INSERT OR IGNORE INTO counters (name, value)
                    VALUES ('order_number', 1000)
                ''')
                # === РЕФЕРАЛЬНАЯ ПРОГРАММА ===
                c.execute('''
                    CREATE TABLE IF NOT EXISTS referrals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        referrer_id INTEGER NOT NULL,
                        referred_id INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(referred_id)
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS bonus_balance (
                        user_id INTEGER PRIMARY KEY,
                        balance REAL NOT NULL DEFAULT 0,
                        total_earned REAL NOT NULL DEFAULT 0,
                        total_spent REAL NOT NULL DEFAULT 0
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS bonus_transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        amount REAL NOT NULL,
                        tx_type TEXT NOT NULL,
                        order_number TEXT,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # Миграции для существующих БД
                for migration in [
                    "ALTER TABLE reviews ADD COLUMN status TEXT DEFAULT 'pending'",
                    "ALTER TABLE orders ADD COLUMN sheets_row INTEGER",
                ]:
                    try:
                        c.execute(migration)
                    except Exception:
                        pass
                conn.commit()
            logger.info("✅ База данных инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
        finally:
            conn.close()

    def add_user(self, telegram_id: int, username: str = None, first_name: str = None) -> Optional[int]:
        """Добавляет или обновляет пользователя"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
                result = c.fetchone()
                if result:
                    c.execute("UPDATE users SET username = ?, first_name = ? WHERE telegram_id = ?",
                              (username, first_name, telegram_id))
                    return result[0]
                c.execute("INSERT INTO users (telegram_id, username, first_name) VALUES (?, ?, ?)",
                          (telegram_id, username, first_name))
                logger.info(f"Добавлен новый пользователь: {telegram_id}")
                return c.lastrowid
        except Exception as e:
            logger.error(f"❌ Ошибка добавления пользователя: {e}")
            return None
        finally:
            conn.close()

    def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получает пользователя по telegram_id"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            result = c.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователя: {e}")
            return None
        finally:
            conn.close()

    def add_order(self, order_number: str, user_id: int, service: str, tariff: str,
                  amount_kzt: int, amount_rub: int, payment_id: str = None) -> Optional[int]:
        """Добавляет новый заказ в БД"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute('''
                    INSERT INTO orders
                    (order_number, user_id, service, tariff, amount_kzt, amount_rub, payment_id, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'new')
                ''', (order_number, user_id, service, tariff, amount_kzt, amount_rub, payment_id))
                logger.info(f"✅ Заказ {order_number} добавлен в БД")
                return c.lastrowid
        except Exception as e:
            logger.error(f"❌ Ошибка добавления заказа: {e}")
            return None
        finally:
            conn.close()

    def get_order(self, order_number: str) -> Optional[Dict]:
        """Получает заказ по номеру"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM orders WHERE order_number = ?", (order_number,))
            result = c.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа: {e}")
            return None
        finally:
            conn.close()

    def get_user_orders(self, user_id: int) -> List[Dict]:
        """Получает все заказы пользователя"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов пользователя: {e}")
            return []
        finally:
            conn.close()

    def set_order_sheets_row(self, order_number: str, sheets_row: int) -> None:
        """Сохраняет номер строки в Google Sheets"""
        conn = self._connect()
        try:
            with conn:
                conn.execute("UPDATE orders SET sheets_row = ? WHERE order_number = ?",
                             (sheets_row, order_number))
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения sheets_row: {e}")
        finally:
            conn.close()

    def get_order_sheets_row(self, order_number: str) -> Optional[int]:
        """Возвращает кэшированный номер строки в Google Sheets"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT sheets_row FROM orders WHERE order_number = ?", (order_number,))
            result = c.fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения sheets_row: {e}")
            return None
        finally:
            conn.close()

    def update_order_amount(self, order_number: str, new_amount_rub: int) -> bool:
        """Обновляет сумму заказа в рублях (например, при VIP-скидке)"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute('''UPDATE orders SET amount_rub = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE order_number = ?''', (new_amount_rub, order_number))
                if c.rowcount == 0:
                    return False
                logger.info(f"✅ Сумма {order_number} обновлена на {new_amount_rub} ₽")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления суммы: {e}")
            return False
        finally:
            conn.close()

    def update_order_status(self, order_number: str, new_status: str) -> bool:
        """Обновляет статус заказа"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute('''
                    UPDATE orders SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE order_number = ?
                ''', (new_status, order_number))
                if c.rowcount == 0:
                    logger.warning(f"Заказ {order_number} не найден")
                    return False
                logger.info(f"✅ Статус {order_number} изменён на {new_status}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса: {e}")
            return False
        finally:
            conn.close()

    def get_order_by_payment_id(self, payment_id: str) -> Optional[Dict]:
        """Получает заказ по payment_id"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM orders WHERE payment_id = ?", (payment_id,))
            result = c.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа по payment_id: {e}")
            return None
        finally:
            conn.close()

    def add_payment(self, payment_id: str, order_id: int, amount: float) -> bool:
        """Добавляет платёж"""
        conn = self._connect()
        try:
            with conn:
                conn.execute('''
                    INSERT INTO payments (payment_id, order_id, amount, status)
                    VALUES (?, ?, ?, 'pending')
                ''', (payment_id, order_id, amount))
                logger.info(f"✅ Платёж {payment_id} добавлен")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления платежа: {e}")
            return False
        finally:
            conn.close()

    def update_payment_status(self, payment_id: str, status: str) -> bool:
        """Обновляет статус платежа"""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute("UPDATE payments SET status = ? WHERE payment_id = ?", (status, payment_id))
                if c.rowcount == 0:
                    logger.warning(f"Платёж {payment_id} не найден")
                    return False
                logger.info(f"✅ Платёж {payment_id} обновлён: {status}")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления платежа: {e}")
            return False
        finally:
            conn.close()

    def log_action(self, user_id: int, action: str, details: str = None) -> bool:
        """Логирует действие пользователя"""
        conn = self._connect()
        try:
            with conn:
                conn.execute("INSERT INTO action_log (user_id, action, details) VALUES (?, ?, ?)",
                             (user_id, action, details))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка логирования действия: {e}")
            return False
        finally:
            conn.close()

    def add_review(self, user_id: int, username: str, order_number: str, rating: int, comment: str = None) -> int:
        """Добавляет отзыв клиента. Возвращает ID отзыва или 0.
        Защита от дублей: один заказ = один отзыв."""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                # Проверка: уже есть отзыв на этот заказ?
                c.execute("SELECT id FROM reviews WHERE order_number = ?", (order_number,))
                if c.fetchone():
                    logger.warning(f"⚠️ Отзыв на заказ {order_number} уже существует")
                    return 0
                c.execute('''
                    INSERT INTO reviews (user_id, username, order_number, rating, comment, status)
                    VALUES (?, ?, ?, ?, ?, 'pending')
                ''', (user_id, username, order_number, rating, comment))
                review_id = c.lastrowid
                logger.info(f"✅ Отзыв от {user_id} сохранён (рейтинг {rating}, id {review_id})")
                return review_id
        except Exception as e:
            logger.error(f"❌ Ошибка добавления отзыва: {e}")
            return 0
        finally:
            conn.close()

    def update_review_status(self, review_id: int, status: str) -> bool:
        """Обновляет статус отзыва: pending / approved / rejected"""
        conn = self._connect()
        try:
            with conn:
                conn.execute("UPDATE reviews SET status = ? WHERE id = ?", (status, review_id))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса отзыва: {e}")
            return False
        finally:
            conn.close()

    def get_review_by_id(self, review_id: int) -> dict:
        """Возвращает отзыв по ID"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM reviews WHERE id = ?", (review_id,))
            row = c.fetchone()
            return dict(row) if row else {}
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзыва по ID: {e}")
            return {}
        finally:
            conn.close()

    def get_recent_reviews(self, limit: int = 5) -> List[Dict]:
        """Возвращает последние одобренные отзывы"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM reviews WHERE status = 'approved' ORDER BY created_at DESC LIMIT ?",
                      (limit,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзывов: {e}")
            return []
        finally:
            conn.close()

    def get_all_reviews(self) -> List[Dict]:
        """Возвращает все отзывы для админа"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM reviews ORDER BY created_at DESC")
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения всех отзывов: {e}")
            return []
        finally:
            conn.close()

    def get_user_reviews(self, telegram_id: int) -> List[Dict]:
        """Возвращает отзывы пользователя по telegram_id"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM reviews WHERE user_id = ? ORDER BY created_at DESC", (telegram_id,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзывов пользователя: {e}")
            return []
        finally:
            conn.close()

    def get_user_orders_by_telegram_id(self, telegram_id: int) -> List[Dict]:
        """Получает все заказы пользователя по telegram_id"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute('''
                SELECT o.* FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE u.telegram_id = ?
                ORDER BY o.created_at DESC
            ''', (telegram_id,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов по telegram_id: {e}")
            return []
        finally:
            conn.close()

    def get_recent_orders(self, limit: int = 10) -> List[Dict]:
        """Получает последние N заказов"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute('''
                SELECT o.*, u.telegram_id, u.username
                FROM orders o JOIN users u ON o.user_id = u.id
                ORDER BY o.created_at DESC LIMIT ?
            ''', (limit,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения последних заказов: {e}")
            return []
        finally:
            conn.close()

    def get_active_orders(self, limit: int = 20) -> List[Dict]:
        """Получает активные заказы (не выполненные/отменённые)"""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute('''
                SELECT o.*, u.telegram_id, u.username
                FROM orders o JOIN users u ON o.user_id = u.id
                WHERE o.status NOT IN (?, ?)
                ORDER BY o.created_at DESC LIMIT ?
            ''', (ORDER_STATUSES["completed"], ORDER_STATUSES["cancelled"], limit,))
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных заказов: {e}")
            return []
        finally:
            conn.close()

    def get_admin_stats(self) -> Dict:
        """Получает расширенную статистику"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(DISTINCT telegram_id) FROM users")
            unique_users = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM orders")
            total_orders = c.fetchone()[0]
            c.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
            raw_statuses = dict(c.fetchall())
            # Маппинг 'new' → 'Ожидает оплаты' для отображения
            statuses = {}
            for k, v in raw_statuses.items():
                display_key = ORDER_STATUSES["pending"] if k == 'new' else k
                statuses[display_key] = statuses.get(display_key, 0) + v
            c.execute("SELECT SUM(amount_rub) FROM orders WHERE status = ?",
                      (ORDER_STATUSES["completed"],))
            revenue = c.fetchone()[0] or 0
            c.execute("SELECT COUNT(*) FROM orders WHERE status = ?",
                      (ORDER_STATUSES["completed"],))
            completed_count = c.fetchone()[0]
            avg_check = int(revenue / completed_count) if completed_count else 0
            paid_count = statuses.get(ORDER_STATUSES["paid"], 0) + statuses.get(ORDER_STATUSES["completed"], 0)
            conversion = int(paid_count / total_orders * 100) if total_orders > 0 else 0
            return {
                "unique_users": unique_users,
                "total_orders": total_orders,
                "statuses": statuses,
                "revenue": revenue,
                "avg_check": avg_check,
                "conversion": conversion,
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения админ-статистики: {e}")
            return {}
        finally:
            conn.close()

    def get_stats(self) -> Dict:
        """Получает общую статистику"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            users_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM orders")
            orders_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM orders WHERE status = ?", (ORDER_STATUSES["paid"],))
            paid_orders = c.fetchone()[0]
            return {"users": users_count, "orders": orders_count, "paid_orders": paid_orders}
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
        finally:
            conn.close()

    def get_telegram_id_for_order(self, order_number: str) -> Optional[int]:
        """Получает telegram_id клиента по номеру заказа"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute('''
                SELECT u.telegram_id FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE o.order_number = ?
            ''', (order_number,))
            result = c.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения telegram_id для заказа: {e}")
            return None
        finally:
            conn.close()

    def set_pending_state(self, state_type: str, key_id: int, value: dict) -> bool:
        """Сохраняет или обновляет pending state"""
        conn = self._connect()
        try:
            with conn:
                conn.execute('''
                    INSERT INTO pending_states (state_type, key_id, value_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT(state_type, key_id) DO UPDATE SET
                        value_json = excluded.value_json,
                        created_at = CURRENT_TIMESTAMP
                ''', (state_type, key_id, json.dumps(value, ensure_ascii=False)))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения pending_state: {e}")
            return False
        finally:
            conn.close()

    def get_pending_state(self, state_type: str, key_id: int) -> Optional[dict]:
        """Получает pending state"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT value_json FROM pending_states WHERE state_type = ? AND key_id = ?",
                      (state_type, key_id))
            result = c.fetchone()
            return json.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения pending_state: {e}")
            return None
        finally:
            conn.close()

    def delete_pending_state(self, state_type: str, key_id: int) -> bool:
        """Удаляет pending state"""
        conn = self._connect()
        try:
            with conn:
                conn.execute("DELETE FROM pending_states WHERE state_type = ? AND key_id = ?",
                             (state_type, key_id))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления pending_state: {e}")
            return False
        finally:
            conn.close()

    def get_all_pending_states(self, state_type: str) -> list:
        """Получает все pending states данного типа"""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT key_id, value_json, created_at FROM pending_states WHERE state_type = ?",
                      (state_type,))
            return [(row[0], json.loads(row[1]), row[2]) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения pending_states: {e}")
            return []
        finally:
            conn.close()

    def cleanup_expired_states(self, max_age_seconds: int) -> int:
        """Удаляет устаревшие pending states. Возвращает кол-во удалённых."""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute("DELETE FROM pending_states WHERE created_at < datetime('now', ? || ' seconds')",
                          (f"-{max_age_seconds}",))
                deleted = c.rowcount
                if deleted:
                    logger.debug(f"Очищено {deleted} устаревших pending_states")
                return deleted
        except Exception as e:
            logger.error(f"❌ Ошибка очистки pending_states: {e}")
            return 0
        finally:
            conn.close()

    def generate_order_number(self) -> str:
        """Атомарная генерация номера ордера через таблицу counters.

        Счётчик хранится отдельно от orders — не сбрасывается при
        очистке заказов и не ломается от fallback-значений.
        """
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute(
                    "UPDATE counters SET value = value + 1 WHERE name = 'order_number'"
                )
                c.execute(
                    "SELECT value FROM counters WHERE name = 'order_number'"
                )
                row = c.fetchone()
                if row:
                    return f"ORD-{row[0]}"
                # Таблица counters пуста (не должно случиться) — создаём запись
                c.execute(
                    "INSERT INTO counters (name, value) VALUES ('order_number', 1001)"
                )
                return "ORD-1001"
        except Exception as e:
            logger.error(f"❌ Ошибка генерации номера ордера: {e}")
            raise
        finally:
            conn.close()

    # ── Реферальная программа ───────────────────────────────────

    def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        """Сохраняет связку «кто пригласил – кого».
        Возвращает False, если referred_id уже привязан к кому-то."""
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                    (referrer_id, referred_id),
                )
                return conn.total_changes > 0
        except Exception as e:
            logger.error(f"❌ Ошибка добавления реферала: {e}")
            return False
        finally:
            conn.close()

    def get_referrer(self, referred_id: int) -> Optional[int]:
        """Возвращает telegram_id пригласившего или None."""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT referrer_id FROM referrals WHERE referred_id = ?", (referred_id,))
            row = c.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения реферера: {e}")
            return None
        finally:
            conn.close()

    def get_referral_count(self, referrer_id: int) -> int:
        """Количество приглашённых пользователем."""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (referrer_id,))
            return c.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Ошибка подсчёта рефералов: {e}")
            return 0
        finally:
            conn.close()

    def get_bonus_balance(self, user_id: int) -> float:
        """Текущий баланс бонусных баллов."""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT balance FROM bonus_balance WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            return row[0] if row else 0.0
        except Exception as e:
            logger.error(f"❌ Ошибка получения баланса: {e}")
            return 0.0
        finally:
            conn.close()

    def get_bonus_info(self, user_id: int) -> Dict:
        """Возвращает balance, total_earned, total_spent."""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute("SELECT balance, total_earned, total_spent FROM bonus_balance WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            if row:
                return {"balance": row[0], "total_earned": row[1], "total_spent": row[2]}
            return {"balance": 0.0, "total_earned": 0.0, "total_spent": 0.0}
        except Exception as e:
            logger.error(f"❌ Ошибка получения бонусной информации: {e}")
            return {"balance": 0.0, "total_earned": 0.0, "total_spent": 0.0}
        finally:
            conn.close()

    def add_bonus(self, user_id: int, amount: float, tx_type: str,
                  order_number: str = None, description: str = None) -> bool:
        """Начисляет бонус (amount > 0). Создаёт запись balance при необходимости."""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute(
                    """INSERT INTO bonus_balance (user_id, balance, total_earned, total_spent)
                       VALUES (?, ?, ?, 0)
                       ON CONFLICT(user_id) DO UPDATE SET
                           balance = balance + excluded.balance,
                           total_earned = total_earned + excluded.balance""",
                    (user_id, amount, amount),
                )
                c.execute(
                    "INSERT INTO bonus_transactions (user_id, amount, tx_type, order_number, description) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (user_id, amount, tx_type, order_number, description),
                )
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка начисления бонуса: {e}")
            return False
        finally:
            conn.close()

    def spend_bonus(self, user_id: int, amount: float, order_number: str = None,
                    description: str = None) -> bool:
        """Списывает бонус. Проверяет достаточность баланса."""
        conn = self._connect()
        try:
            with conn:
                c = conn.cursor()
                c.execute("SELECT balance FROM bonus_balance WHERE user_id = ?", (user_id,))
                row = c.fetchone()
                if not row or row[0] < amount:
                    return False
                c.execute(
                    "UPDATE bonus_balance SET balance = balance - ?, total_spent = total_spent + ? WHERE user_id = ?",
                    (amount, amount, user_id),
                )
                c.execute(
                    "INSERT INTO bonus_transactions (user_id, amount, tx_type, order_number, description) "
                    "VALUES (?, ?, 'payment', ?, ?)",
                    (user_id, -amount, order_number, description),
                )
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка списания бонуса: {e}")
            return False
        finally:
            conn.close()

    def get_bonus_history(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Последние бонусные транзакции."""
        conn = self._connect(row_factory=True)
        try:
            c = conn.cursor()
            c.execute(
                "SELECT * FROM bonus_transactions WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            )
            return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения истории бонусов: {e}")
            return []
        finally:
            conn.close()

    def count_user_completed_orders(self, telegram_id: int) -> int:
        """Количество выполненных заказов пользователя."""
        conn = self._connect()
        try:
            c = conn.cursor()
            c.execute('''
                SELECT COUNT(*) FROM orders o
                JOIN users u ON o.user_id = u.id
                WHERE u.telegram_id = ? AND o.status = ?
            ''', (telegram_id, ORDER_STATUSES["completed"]))
            return c.fetchone()[0]
        except Exception as e:
            logger.error(f"❌ Ошибка подсчёта заказов: {e}")
            return 0
        finally:
            conn.close()


# Создаём глобальный объект БД
db = Database()

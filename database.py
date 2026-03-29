"""
Модуль для работы с базой данных SQLite.
Здесь хранятся все пользователи и заказы.
"""

import sqlite3
import logging
from typing import Optional, List, Dict

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
        try:
            with self._connect() as conn:
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

    def add_user(self, telegram_id: int, username: str = None, first_name: str = None) -> Optional[int]:
        """Добавляет или обновляет пользователя"""
        try:
            with self._connect() as conn:
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

    def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получает пользователя по telegram_id"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
                result = c.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователя: {e}")
            return None

    def add_order(self, order_number: str, user_id: int, service: str, tariff: str,
                  amount_kzt: int, amount_rub: int, payment_id: str = None) -> Optional[int]:
        """Добавляет новый заказ в БД"""
        try:
            with self._connect() as conn:
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

    def get_order(self, order_number: str) -> Optional[Dict]:
        """Получает заказ по номеру"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM orders WHERE order_number = ?", (order_number,))
                result = c.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа: {e}")
            return None

    def get_user_orders(self, user_id: int) -> List[Dict]:
        """Получает все заказы пользователя"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
                return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов пользователя: {e}")
            return []

    def set_order_sheets_row(self, order_number: str, sheets_row: int) -> None:
        """Сохраняет номер строки в Google Sheets"""
        try:
            with self._connect() as conn:
                conn.execute("UPDATE orders SET sheets_row = ? WHERE order_number = ?",
                             (sheets_row, order_number))
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения sheets_row: {e}")

    def get_order_sheets_row(self, order_number: str) -> Optional[int]:
        """Возвращает кэшированный номер строки в Google Sheets"""
        try:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("SELECT sheets_row FROM orders WHERE order_number = ?", (order_number,))
                result = c.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения sheets_row: {e}")
            return None

    def update_order_status(self, order_number: str, new_status: str) -> bool:
        """Обновляет статус заказа"""
        try:
            with self._connect() as conn:
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

    def get_order_by_payment_id(self, payment_id: str) -> Optional[Dict]:
        """Получает заказ по payment_id"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM orders WHERE payment_id = ?", (payment_id,))
                result = c.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа по payment_id: {e}")
            return None

    def add_payment(self, payment_id: str, order_id: int, amount: float) -> bool:
        """Добавляет платёж"""
        try:
            with self._connect() as conn:
                conn.execute('''
                    INSERT INTO payments (payment_id, order_id, amount, status)
                    VALUES (?, ?, ?, 'pending')
                ''', (payment_id, order_id, amount))
                logger.info(f"✅ Платёж {payment_id} добавлен")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления платежа: {e}")
            return False

    def update_payment_status(self, payment_id: str, status: str) -> bool:
        """Обновляет статус платежа"""
        try:
            with self._connect() as conn:
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

    def log_action(self, user_id: int, action: str, details: str = None) -> bool:
        """Логирует действие пользователя"""
        try:
            with self._connect() as conn:
                conn.execute("INSERT INTO action_log (user_id, action, details) VALUES (?, ?, ?)",
                             (user_id, action, details))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка логирования действия: {e}")
            return False

    def add_review(self, user_id: int, username: str, order_number: str, rating: int, comment: str = None) -> int:
        """Добавляет отзыв клиента. Возвращает ID отзыва или 0."""
        try:
            with self._connect() as conn:
                c = conn.cursor()
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

    def update_review_status(self, review_id: int, status: str) -> bool:
        """Обновляет статус отзыва: pending / approved / rejected"""
        try:
            with self._connect() as conn:
                conn.execute("UPDATE reviews SET status = ? WHERE id = ?", (status, review_id))
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса отзыва: {e}")
            return False

    def get_review_by_id(self, review_id: int) -> dict:
        """Возвращает отзыв по ID"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM reviews WHERE id = ?", (review_id,))
                row = c.fetchone()
                return dict(row) if row else {}
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзыва по ID: {e}")
            return {}

    def get_recent_reviews(self, limit: int = 5) -> List[Dict]:
        """Возвращает последние одобренные отзывы"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM reviews WHERE status = 'approved' ORDER BY created_at DESC LIMIT ?",
                          (limit,))
                return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзывов: {e}")
            return []

    def get_all_reviews(self) -> List[Dict]:
        """Возвращает все отзывы для админа"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM reviews ORDER BY created_at DESC")
                return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения всех отзывов: {e}")
            return []

    def get_user_orders_by_telegram_id(self, telegram_id: int) -> List[Dict]:
        """Получает все заказы пользователя по telegram_id"""
        try:
            with self._connect(row_factory=True) as conn:
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

    def get_recent_orders(self, limit: int = 10) -> List[Dict]:
        """Получает последние N заказов"""
        try:
            with self._connect(row_factory=True) as conn:
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

    def get_active_orders(self, limit: int = 20) -> List[Dict]:
        """Получает активные заказы (не выполненные/отменённые)"""
        try:
            with self._connect(row_factory=True) as conn:
                c = conn.cursor()
                c.execute('''
                    SELECT o.*, u.telegram_id, u.username
                    FROM orders o JOIN users u ON o.user_id = u.id
                    WHERE o.status NOT IN ('Выполнен', 'Отменён')
                    ORDER BY o.created_at DESC LIMIT ?
                ''', (limit,))
                return [dict(row) for row in c.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных заказов: {e}")
            return []

    def get_admin_stats(self) -> Dict:
        """Получает расширенную статистику"""
        try:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(DISTINCT telegram_id) FROM users")
                unique_users = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM orders")
                total_orders = c.fetchone()[0]
                c.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
                statuses = dict(c.fetchall())
                c.execute("SELECT SUM(amount_rub) FROM orders WHERE status = 'Выполнен'")
                revenue = c.fetchone()[0] or 0
                c.execute("SELECT COUNT(*) FROM orders WHERE status = 'Выполнен'")
                completed_count = c.fetchone()[0]
                avg_check = int(revenue / completed_count) if completed_count else 0
                paid_count = statuses.get("Оплачен", 0) + statuses.get("Выполнен", 0)
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

    def get_stats(self) -> Dict:
        """Получает общую статистику"""
        try:
            with self._connect() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM users")
                users_count = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM orders")
                orders_count = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM orders WHERE status = 'paid'")
                paid_orders = c.fetchone()[0]
                return {"users": users_count, "orders": orders_count, "paid_orders": paid_orders}
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}

    def get_telegram_id_for_order(self, order_number: str) -> Optional[int]:
        """Получает telegram_id клиента по номеру заказа"""
        try:
            with self._connect() as conn:
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


# Создаём глобальный объект БД
db = Database()

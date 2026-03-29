"""
Модуль для работы с базой данных SQLite.
Здесь хранятся все пользователи и заказы.
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

DATABASE_FILE = "orders.db"


class Database:
    """Класс для работы с БД"""
    
    def __init__(self):
        self.db_file = DATABASE_FILE
        self.init_db()
    
    def init_db(self):
        """Инициализирует БД и создаёт таблицы"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            # Таблица пользователей
            c.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица заказов
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
            
            # Таблица платежей
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
            
            # Таблица лога действий
            c.execute('''
                CREATE TABLE IF NOT EXISTS action_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица отзывов
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
            # Миграция: добавить колонку status если её нет (для существующих БД)
            try:
                c.execute("ALTER TABLE reviews ADD COLUMN status TEXT DEFAULT 'pending'")
                conn.commit()
            except Exception:
                pass  # Колонка уже существует
            
            conn.commit()
            logger.info("✅ База данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
        finally:
            if conn:
                conn.close()
    
    def add_user(self, telegram_id: int, username: str = None, first_name: str = None) -> int:
        """Добавляет или обновляет пользователя"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
            result = c.fetchone()
            
            if result:
                c.execute('''
                    UPDATE users 
                    SET username = ?, first_name = ?
                    WHERE telegram_id = ?
                ''', (username, first_name, telegram_id))
                user_id = result[0]
            else:
                c.execute('''
                    INSERT INTO users (telegram_id, username, first_name)
                    VALUES (?, ?, ?)
                ''', (telegram_id, username, first_name))
                user_id = c.lastrowid
                logger.info(f"Добавлен новый пользователь: {telegram_id}")
            
            conn.commit()
            return user_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления пользователя: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def get_user(self, telegram_id: int) -> Optional[Dict]:
        """Получает пользователя по telegram_id"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            result = c.fetchone()
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователя: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def add_order(self, order_number: str, user_id: int, service: str, tariff: str,
                  amount_kzt: int, amount_rub: int, payment_id: str = None) -> Optional[int]:
        """Добавляет новый заказ в БД"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                INSERT INTO orders 
                (order_number, user_id, service, tariff, amount_kzt, amount_rub, payment_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'new')
            ''', (order_number, user_id, service, tariff, amount_kzt, amount_rub, payment_id))
            
            order_id = c.lastrowid
            conn.commit()
            
            logger.info(f"✅ Заказ {order_number} добавлен в БД")
            return order_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления заказа: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def get_order(self, order_number: str) -> Optional[Dict]:
        """Получает заказ по номеру"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute("SELECT * FROM orders WHERE order_number = ?", (order_number,))
            result = c.fetchone()
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def get_user_orders(self, user_id: int) -> List[Dict]:
        """Получает все заказы пользователя"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute("SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
            results = c.fetchall()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказов пользователя: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def update_order_status(self, order_number: str, new_status: str) -> bool:
        """Обновляет статус заказа"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                UPDATE orders 
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_number = ?
            ''', (new_status, order_number))
            
            if c.rowcount == 0:
                logger.warning(f"Заказ {order_number} не найден")
                return False
            
            conn.commit()
            
            logger.info(f"✅ Статус {order_number} изменён на {new_status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_order_by_payment_id(self, payment_id: str) -> Optional[Dict]:
        """Получает заказ по payment_id"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            c.execute("SELECT * FROM orders WHERE payment_id = ?", (payment_id,))
            result = c.fetchone()
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения заказа по payment_id: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def add_payment(self, payment_id: str, order_id: int, amount: float) -> bool:
        """Добавляет платёж"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                INSERT INTO payments (payment_id, order_id, amount, status)
                VALUES (?, ?, ?, 'pending')
            ''', (payment_id, order_id, amount))
            
            conn.commit()
            logger.info(f"✅ Платёж {payment_id} добавлен")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления платежа: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_payment_status(self, payment_id: str, status: str) -> bool:
        """Обновляет статус платежа"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                UPDATE payments 
                SET status = ?
                WHERE payment_id = ?
            ''', (status, payment_id))
            
            if c.rowcount == 0:
                logger.warning(f"Платёж {payment_id} не найден")
                return False
            
            conn.commit()
            
            logger.info(f"✅ Платёж {payment_id} обновлён: {status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления платежа: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def log_action(self, user_id: int, action: str, details: str = None) -> bool:
        """Логирует действие пользователя"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                INSERT INTO action_log (user_id, action, details)
                VALUES (?, ?, ?)
            ''', (user_id, action, details))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка логирования действия: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def add_review(self, user_id: int, username: str, order_number: str, rating: int, comment: str = None) -> int:
        """Добавляет отзыв клиента со статусом pending. Возвращает ID отзыва или 0 при ошибке."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute('''
                INSERT INTO reviews (user_id, username, order_number, rating, comment, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
            ''', (user_id, username, order_number, rating, comment))
            conn.commit()
            review_id = c.lastrowid
            logger.info(f"✅ Отзыв от {user_id} сохранён (рейтинг {rating}, id {review_id})")
            return review_id
        except Exception as e:
            logger.error(f"❌ Ошибка добавления отзыва: {e}")
            return 0
        finally:
            if conn:
                conn.close()

    def update_review_status(self, review_id: int, status: str) -> bool:
        """Обновляет статус отзыва: pending / approved / rejected"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("UPDATE reviews SET status = ? WHERE id = ?", (status, review_id))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления статуса отзыва: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_review_by_id(self, review_id: int) -> dict:
        """Возвращает отзыв по ID"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT * FROM reviews WHERE id = ?", (review_id,))
            row = c.fetchone()
            return dict(row) if row else {}
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзыва по ID: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def get_recent_reviews(self, limit: int = 5) -> List[Dict]:
        """Возвращает последние одобренные отзывы"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute('''
                SELECT * FROM reviews WHERE status = 'approved' ORDER BY created_at DESC LIMIT ?
            ''', (limit,))
            results = c.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"❌ Ошибка получения отзывов: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_stats(self) -> Dict:
        """Получает статистику"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM users")
            users_count = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM orders")
            orders_count = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM orders WHERE status = 'paid'")
            paid_orders = c.fetchone()[0]
            
            return {
                "users": users_count,
                "orders": orders_count,
                "paid_orders": paid_orders
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
        finally:
            if conn:
                conn.close()


# Создаём глобальный объект БД
db = Database()
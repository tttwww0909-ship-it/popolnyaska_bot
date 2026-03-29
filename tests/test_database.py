"""
Тесты для database.py: CRUD пользователей, заказов, отзывов, статистика.
"""

import os
import pytest
from database import Database


@pytest.fixture
def db(tmp_path):
    """Создаёт временную БД для каждого теста"""
    db_path = str(tmp_path / "test_orders.db")
    d = Database.__new__(Database)
    d.db_file = db_path
    d.init_db()
    return d


class TestUsers:
    def test_add_user(self, db):
        uid = db.add_user(111, "testuser", "Test")
        assert uid is not None
        assert uid > 0

    def test_add_user_duplicate_updates(self, db):
        uid1 = db.add_user(111, "old_name", "Old")
        uid2 = db.add_user(111, "new_name", "New")
        assert uid1 == uid2
        user = db.get_user(111)
        assert user["username"] == "new_name"
        assert user["first_name"] == "New"

    def test_get_user_not_found(self, db):
        assert db.get_user(9999) is None


class TestOrders:
    def test_add_and_get_order(self, db):
        uid = db.add_user(222, "buyer", "Buyer")
        oid = db.add_order("ORD-1001", uid, "steam", "1000 KZT", 1000, 185)
        assert oid is not None

        order = db.get_order("ORD-1001")
        assert order is not None
        assert order["order_number"] == "ORD-1001"
        assert order["service"] == "steam"
        assert order["amount_kzt"] == 1000
        assert order["status"] == "new"

    def test_get_order_not_found(self, db):
        assert db.get_order("ORD-0000") is None

    def test_get_user_orders(self, db):
        uid = db.add_user(333, "multi", "Multi")
        db.add_order("ORD-2001", uid, "steam", "500", 500, 93)
        db.add_order("ORD-2002", uid, "steam", "1000", 1000, 185)
        orders = db.get_user_orders(uid)
        assert len(orders) == 2

    def test_update_order_status(self, db):
        uid = db.add_user(444, "s", "S")
        db.add_order("ORD-3001", uid, "steam", "500", 500, 93)
        ok = db.update_order_status("ORD-3001", "Оплачен")
        assert ok is True
        order = db.get_order("ORD-3001")
        assert order["status"] == "Оплачен"

    def test_update_order_status_not_found(self, db):
        ok = db.update_order_status("ORD-FAKE", "Оплачен")
        assert ok is False

    def test_sheets_row(self, db):
        uid = db.add_user(555, "sr", "SR")
        db.add_order("ORD-4001", uid, "steam", "1000", 1000, 185)
        assert db.get_order_sheets_row("ORD-4001") is None
        db.set_order_sheets_row("ORD-4001", 42)
        assert db.get_order_sheets_row("ORD-4001") == 42

    def test_get_user_orders_by_telegram_id(self, db):
        uid = db.add_user(666, "tg", "TG")
        db.add_order("ORD-5001", uid, "steam", "500", 500, 93)
        orders = db.get_user_orders_by_telegram_id(666)
        assert len(orders) == 1
        assert orders[0]["order_number"] == "ORD-5001"

    def test_get_telegram_id_for_order(self, db):
        uid = db.add_user(777, "tid", "TID")
        db.add_order("ORD-6001", uid, "steam", "500", 500, 93)
        tid = db.get_telegram_id_for_order("ORD-6001")
        assert tid == 777

    def test_get_telegram_id_for_order_not_found(self, db):
        assert db.get_telegram_id_for_order("ORD-NOPE") is None


class TestReviews:
    def test_add_review(self, db):
        rid = db.add_review(111, "user1", "ORD-1001", 5, "Отлично!")
        assert rid > 0

    def test_review_status_flow(self, db):
        rid = db.add_review(222, "user2", "ORD-2001", 4, "Хорошо")
        review = db.get_review_by_id(rid)
        assert review["status"] == "pending"

        db.update_review_status(rid, "approved")
        review = db.get_review_by_id(rid)
        assert review["status"] == "approved"

    def test_recent_reviews(self, db):
        db.add_review(1, "u1", "O-1", 5, "a")
        db.add_review(2, "u2", "O-2", 4, "b")

        # Ни один не approved — список пуст
        assert db.get_recent_reviews() == []

        # Одобряем первый
        db.update_review_status(1, "approved")
        recent = db.get_recent_reviews()
        assert len(recent) == 1


class TestStats:
    def test_admin_stats_empty(self, db):
        stats = db.get_admin_stats()
        assert stats["unique_users"] == 0
        assert stats["total_orders"] == 0
        assert stats["revenue"] == 0

    def test_admin_stats_with_data(self, db):
        uid = db.add_user(888, "stat", "Stat")
        db.add_order("ORD-S1", uid, "steam", "1000", 1000, 185)
        db.update_order_status("ORD-S1", "Выполнен")

        stats = db.get_admin_stats()
        assert stats["unique_users"] == 1
        assert stats["total_orders"] == 1
        assert stats["revenue"] == 185
        assert stats["avg_check"] == 185

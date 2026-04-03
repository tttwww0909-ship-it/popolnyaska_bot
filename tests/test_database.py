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

    def test_update_order_amount(self, db):
        uid = db.add_user(888, "amt", "Amt")
        db.add_order("ORD-7001", uid, "steam", "500", 500, 93)
        ok = db.update_order_amount("ORD-7001", 200)
        assert ok is True
        order = db.get_order("ORD-7001")
        assert order["amount_rub"] == 200

    def test_update_order_amount_not_found(self, db):
        ok = db.update_order_amount("ORD-FAKE", 200)
        assert ok is False


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

    def test_get_all_reviews(self, db):
        db.add_review(1, "u1", "O-1", 5, "a")
        db.add_review(2, "u2", "O-2", 4, "b")
        all_reviews = db.get_all_reviews()
        assert len(all_reviews) == 2


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

    def test_get_stats(self, db):
        stats = db.get_stats()
        assert stats["users"] == 0
        assert stats["orders"] == 0
        assert stats["paid_orders"] == 0


class TestGenerateOrderNumber:
    def test_first_order(self, db):
        num = db.generate_order_number()
        assert num == "ORD-1001"

    def test_sequential(self, db):
        """Счётчик инкрементируется независимо от содержимого orders"""
        first = db.generate_order_number()
        second = db.generate_order_number()
        assert first == "ORD-1001"
        assert second == "ORD-1002"

    def test_counter_independent_of_orders(self, db):
        """Ручная вставка ORD-1005 не влияет на счётчик"""
        uid = db.add_user(111, "u", "U")
        db.add_order("ORD-1005", uid, "s", "t", 100, 50)
        num = db.generate_order_number()
        assert num == "ORD-1001"
        num2 = db.generate_order_number()
        assert num2 == "ORD-1002"


class TestPendingStates:
    def test_set_and_get(self, db):
        db.set_pending_state("screenshot", 123, {"order": "ORD-1001"})
        result = db.get_pending_state("screenshot", 123)
        assert result == {"order": "ORD-1001"}

    def test_get_not_found(self, db):
        assert db.get_pending_state("screenshot", 999) is None

    def test_delete(self, db):
        db.set_pending_state("email", 123, {"order": "ORD-1001"})
        db.delete_pending_state("email", 123)
        assert db.get_pending_state("email", 123) is None

    def test_upsert(self, db):
        db.set_pending_state("code", 123, {"v": "old"})
        db.set_pending_state("code", 123, {"v": "new"})
        result = db.get_pending_state("code", 123)
        assert result == {"v": "new"}

    def test_get_all(self, db):
        db.set_pending_state("screenshot", 1, {"a": 1})
        db.set_pending_state("screenshot", 2, {"a": 2})
        db.set_pending_state("email", 3, {"a": 3})
        results = db.get_all_pending_states("screenshot")
        assert len(results) == 2

    def test_cleanup_expired(self, db):
        db.set_pending_state("test", 1, {"v": 1})
        # Всё свежее — ничего не удалится
        deleted = db.cleanup_expired_states(3600)
        assert deleted == 0

    def test_log_action(self, db):
        ok = db.log_action(123, "test_action", "details")
        assert ok is True


class TestReferrals:
    def test_add_referral(self, db):
        db.add_user(100, "partner", "Partner")
        db.add_user(200, "friend", "Friend")
        assert db.add_referral(100, 200) is True

    def test_add_referral_duplicate(self, db):
        db.add_user(100, "partner", "Partner")
        db.add_user(200, "friend", "Friend")
        db.add_referral(100, 200)
        # Повторная привязка того же referred_id — ignored
        assert db.add_referral(999, 200) is False

    def test_get_referrer(self, db):
        db.add_user(100, "p", "P")
        db.add_user(200, "f", "F")
        db.add_referral(100, 200)
        assert db.get_referrer(200) == 100

    def test_get_referrer_not_found(self, db):
        assert db.get_referrer(999) is None

    def test_get_referral_count(self, db):
        db.add_user(100, "p", "P")
        db.add_user(201, "f1", "F1")
        db.add_user(202, "f2", "F2")
        db.add_referral(100, 201)
        db.add_referral(100, 202)
        assert db.get_referral_count(100) == 2

    def test_get_referral_count_empty(self, db):
        assert db.get_referral_count(999) == 0


class TestBonusBalance:
    def test_add_bonus(self, db):
        db.add_user(100, "p", "P")
        ok = db.add_bonus(100, 50.0, "referral_bonus", "ORD-1001", "Тестовый бонус")
        assert ok is True
        assert db.get_bonus_balance(100) == 50.0

    def test_add_bonus_accumulates(self, db):
        db.add_user(100, "p", "P")
        db.add_bonus(100, 30.0, "referral_bonus")
        db.add_bonus(100, 20.0, "referral_bonus")
        assert db.get_bonus_balance(100) == 50.0
        info = db.get_bonus_info(100)
        assert info["total_earned"] == 50.0
        assert info["total_spent"] == 0.0

    def test_spend_bonus(self, db):
        db.add_user(100, "p", "P")
        db.add_bonus(100, 100.0, "referral_bonus")
        ok = db.spend_bonus(100, 40.0, "ORD-1001", "Оплата")
        assert ok is True
        assert db.get_bonus_balance(100) == 60.0
        info = db.get_bonus_info(100)
        assert info["total_spent"] == 40.0

    def test_spend_bonus_insufficient(self, db):
        db.add_user(100, "p", "P")
        db.add_bonus(100, 10.0, "referral_bonus")
        ok = db.spend_bonus(100, 50.0, "ORD-1001")
        assert ok is False
        assert db.get_bonus_balance(100) == 10.0

    def test_spend_bonus_no_record(self, db):
        ok = db.spend_bonus(999, 10.0)
        assert ok is False

    def test_get_bonus_balance_no_record(self, db):
        assert db.get_bonus_balance(999) == 0.0

    def test_get_bonus_info_no_record(self, db):
        info = db.get_bonus_info(999)
        assert info["balance"] == 0.0
        assert info["total_earned"] == 0.0

    def test_bonus_history(self, db):
        db.add_user(100, "p", "P")
        db.add_bonus(100, 30.0, "referral_bonus", "ORD-1001", "Бонус 1")
        db.add_bonus(100, 20.0, "referral_bonus", "ORD-1002", "Бонус 2")
        db.spend_bonus(100, 10.0, "ORD-1003", "Оплата")
        history = db.get_bonus_history(100, limit=10)
        assert len(history) == 3
        # Проверяем, что все типы транзакций присутствуют
        types = [h["tx_type"] for h in history]
        assert "referral_bonus" in types
        assert "payment" in types
        amounts = sorted([h["amount"] for h in history])
        assert amounts == [-10.0, 20.0, 30.0]

    def test_bonus_history_empty(self, db):
        assert db.get_bonus_history(999) == []

    def test_count_user_completed_orders(self, db):
        uid = db.add_user(100, "u", "U")
        db.add_order("ORD-1001", uid, "Apple ID", "5000 KZT", 5000, 1000)
        db.update_order_status("ORD-1001", "Выполнен")
        db.add_order("ORD-1002", uid, "Apple ID", "10000 KZT", 10000, 2000)
        db.update_order_status("ORD-1002", "Ожидает оплаты")
        assert db.count_user_completed_orders(100) == 1

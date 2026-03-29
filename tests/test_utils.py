"""
Тесты для utils.py: TimedDict, fmt, check_spam, generate_order.
"""

import time
import pytest
from utils import TimedDict, fmt, check_spam, mark_order_created, USER_ORDER_TIMES, ORDER_COOLDOWN


class TestTimedDict:
    def test_set_and_get(self):
        d = TimedDict(max_age_seconds=60)
        d["key1"] = "value1"
        assert d["key1"] == "value1"

    def test_get_default(self):
        d = TimedDict(max_age_seconds=60)
        assert d.get("missing") is None
        assert d.get("missing", 42) == 42

    def test_contains(self):
        d = TimedDict(max_age_seconds=60)
        d["a"] = 1
        assert "a" in d
        assert "b" not in d

    def test_expiration(self):
        d = TimedDict(max_age_seconds=1)
        d["temp"] = "data"
        assert "temp" in d

        # Подменяем timestamp, чтобы запись считалась устаревшей
        d.timestamps["temp"] = time.time() - 2
        assert "temp" not in d
        assert d.get("temp") is None

    def test_getitem_expired_raises(self):
        d = TimedDict(max_age_seconds=1)
        d["x"] = 10
        d.timestamps["x"] = time.time() - 5
        with pytest.raises(KeyError):
            _ = d["x"]

    def test_cleanup(self):
        d = TimedDict(max_age_seconds=1)
        d["a"] = 1
        d["b"] = 2
        d.timestamps["a"] = time.time() - 5  # устарела
        d.timestamps["b"] = time.time()       # свежая
        d.cleanup()
        assert "a" not in d
        assert "b" in d


class TestFmt:
    def test_basic(self):
        assert fmt(25000) == "25 000"

    def test_small(self):
        assert fmt(100) == "100"

    def test_million(self):
        assert fmt(1000000) == "1 000 000"

    def test_zero(self):
        assert fmt(0) == "0"

    def test_float_truncation(self):
        assert fmt(25999.7) == "25 999"


class TestCheckSpam:
    def setup_method(self):
        USER_ORDER_TIMES.clear()

    def test_first_order_allowed(self):
        ok, msg = check_spam(12345)
        assert ok is True
        assert msg == ""

    def test_cooldown_blocks(self):
        mark_order_created(12345)
        ok, msg = check_spam(12345)
        assert ok is False
        assert "Подождите" in msg

    def test_after_cooldown_ok(self):
        uid = 99999
        USER_ORDER_TIMES[uid] = [time.time() - ORDER_COOLDOWN - 1]
        ok, msg = check_spam(uid)
        assert ok is True

    def test_rate_limit(self):
        uid = 77777
        now = time.time()
        # 3 заказа недавно — но после кулдауна
        USER_ORDER_TIMES[uid] = [now - ORDER_COOLDOWN - 3, now - ORDER_COOLDOWN - 2, now - ORDER_COOLDOWN - 1]
        ok, msg = check_spam(uid)
        assert ok is False
        assert "Лимит" in msg

"""
Тесты для utils.py: TimedDict, fmt, check_spam, generate_order.
"""

import time
import pytest
from utils import (
    TimedDict, fmt, check_spam, mark_order_created, USER_ORDER_TIMES, ORDER_COOLDOWN,
    smart_round, validate_email, esc, get_us_commission, get_kz_commission, cleanup_memory,
    ORDER_USER_MAP, ORDER_INFO_MAP, ORDER_LOCK,
)


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

    def test_thread_safety_lock_exists(self):
        d = TimedDict(max_age_seconds=60)
        assert hasattr(d, '_lock')


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


class TestSmartRound:
    def test_small_amount(self):
        assert smart_round(1134) == 1140

    def test_large_amount_50(self):
        assert smart_round(2134) == 2150

    def test_large_amount_90(self):
        assert smart_round(2160) == 2190

    def test_exact_boundary(self):
        assert smart_round(2000) == 2000

    def test_above_boundary_exact_50(self):
        assert smart_round(2050) == 2050

    def test_small_ceil(self):
        assert smart_round(503) == 510


class TestValidateEmail:
    def test_valid(self):
        assert validate_email("user@example.com") is True

    def test_valid_with_dots(self):
        assert validate_email("first.last@mail.ru") is True

    def test_invalid_no_at(self):
        assert validate_email("userexample.com") is False

    def test_invalid_no_domain(self):
        assert validate_email("user@") is False

    def test_invalid_spaces(self):
        assert validate_email("user @example.com") is False

    def test_empty(self):
        assert validate_email("") is False


class TestEsc:
    def test_html_escape(self):
        assert esc("<b>test</b>") == "&lt;b&gt;test&lt;/b&gt;"

    def test_none(self):
        assert esc(None) == ""

    def test_empty(self):
        assert esc("") == ""

    def test_safe_string(self):
        assert esc("hello") == "hello"

    def test_ampersand(self):
        assert esc("A & B") == "A &amp; B"


class TestCommissions:
    def test_us_small(self):
        assert get_us_commission(25) == 1.15

    def test_us_medium(self):
        assert get_us_commission(100) == 1.12

    def test_us_large(self):
        assert get_us_commission(500) == 1.11

    def test_us_boundary_50(self):
        assert get_us_commission(50) == 1.15

    def test_kz_small(self):
        assert get_kz_commission(5000) == 1.20

    def test_kz_medium(self):
        assert get_kz_commission(15000) == 1.15

    def test_kz_large(self):
        assert get_kz_commission(50000) == 1.12

    def test_kz_boundary(self):
        assert get_kz_commission(10000) == 1.15


class TestCleanupMemory:
    def setup_method(self):
        ORDER_USER_MAP.clear()
        ORDER_USER_MAP.timestamps.clear()
        ORDER_INFO_MAP.clear()
        ORDER_INFO_MAP.timestamps.clear()
        ORDER_LOCK.clear()
        ORDER_LOCK.timestamps.clear()
        USER_ORDER_TIMES.clear()

    def test_cleanup_runs_without_error(self):
        ORDER_USER_MAP["test"] = 1
        ORDER_USER_MAP.timestamps["test"] = time.time() - 100000
        cleanup_memory()
        assert "test" not in ORDER_USER_MAP

    def test_cleanup_user_order_times(self):
        USER_ORDER_TIMES[123] = [time.time() - 100000]
        cleanup_memory()
        assert 123 not in USER_ORDER_TIMES


class TestGetReferralRates:
    def test_exact_20(self):
        from utils import get_referral_rates
        partner, discount = get_referral_rates(1.20)
        assert partner == 0.03
        assert discount == 0.02

    def test_exact_15(self):
        from utils import get_referral_rates
        partner, discount = get_referral_rates(1.15)
        assert partner == 0.03
        assert discount == 0.02

    def test_exact_12(self):
        from utils import get_referral_rates
        partner, discount = get_referral_rates(1.12)
        assert partner == 0.02
        assert discount == 0.02

    def test_exact_11(self):
        from utils import get_referral_rates
        partner, discount = get_referral_rates(1.11)
        assert partner == 0.015
        assert discount == 0.01

    def test_unknown_commission_fallback(self):
        from utils import get_referral_rates
        # 1.18 not in table — should fall back to nearest ≤ (1.15)
        partner, discount = get_referral_rates(1.18)
        assert partner == 0.03
        assert discount == 0.02

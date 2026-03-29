"""
Конфигурация тестового окружения.
Устанавливает mock-переменные до импорта модулей, чтобы config.py не падал в CI.
"""

import os

os.environ.setdefault("TELEGRAM_TOKEN", "TEST_TOKEN_FOR_CI")
os.environ.setdefault("ADMIN_ID", "123456789")
os.environ.setdefault("YOOMONEY_WALLET", "test_wallet")
os.environ.setdefault("OZON_PAY_URL", "https://example.com")
os.environ.setdefault("BYBIT_UID", "test_uid")
os.environ.setdefault("BSC_ADDRESS", "test_bsc")
os.environ.setdefault("TRC20_ADDRESS", "test_trc20")

"""
Интеграция с CryptoPay (@CryptoBot) — автоматический приём крипто-платежей.

API docs: https://help.crypt.bot/crypto-pay-api
"""

import hashlib
import hmac
import logging

import aiohttp

logger = logging.getLogger(__name__)

API_URL = "https://pay.crypt.bot/api"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)


class CryptoPay:
    def __init__(self, token: str):
        self.token = token
        self._headers = {"Crypto-Pay-API-Token": token}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Ленивая инициализация persistent-сессии."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self._headers,
                timeout=_REQUEST_TIMEOUT,
            )
        return self._session

    async def close(self):
        """Закрывает HTTP-сессию (вызывать при остановке бота)."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, params: dict | None = None) -> dict:
        url = f"{API_URL}/{method}"
        session = await self._get_session()
        async with session.post(url, json=params or {}) as resp:
            data = await resp.json()
            if not data.get("ok"):
                logger.error("CryptoPay API error: %s", data)
                raise RuntimeError(data.get("error", {}).get("message", "Unknown error"))
            return data["result"]

    async def create_invoice(
        self,
        amount: float,
        order_number: str,
        currency: str = "USDT",
        description: str | None = None,
        expires_in: int = 3600,
    ) -> dict:
        """Создаёт invoice для оплаты.

        Returns dict с ключами: invoice_id, mini_app_invoice_url, amount, status и др.
        """
        params = {
            "asset": currency,
            "amount": str(amount),
            "description": description or f"Заказ {order_number}",
            "payload": order_number,
            "expires_in": expires_in,
        }
        invoice = await self._request("createInvoice", params)
        logger.info("CryptoPay invoice created: %s for order %s", invoice.get("invoice_id"), order_number)
        return invoice

    async def get_invoices(self, invoice_ids: list[int] | None = None, status: str | None = None) -> list[dict]:
        params = {}
        if invoice_ids:
            params["invoice_ids"] = ",".join(str(i) for i in invoice_ids)
        if status:
            params["status"] = status
        return await self._request("getInvoices", params)

    def verify_webhook(self, body: bytes, signature: str) -> bool:
        """Проверяет подпись вебхука от CryptoPay."""
        secret = hashlib.sha256(self.token.encode()).digest()
        check_string = body if isinstance(body, bytes) else body.encode()
        expected = hmac.new(secret, check_string, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature)

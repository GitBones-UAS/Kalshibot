import base64
import time
import threading
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from logger import log_error


class RateLimiter:
    def __init__(self, max_per_second=10):
        self._interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last = time.monotonic()


class KalshiAPI:
    def __init__(self, api_key_id="", private_key_path="", base_url="https://demo-api.kalshi.co/trade-api/v2"):
        self.api_key_id = api_key_id
        self.base_url = base_url.rstrip("/")
        self._private_key = None
        if private_key_path:
            with open(private_key_path, "rb") as f:
                self._private_key = serialization.load_pem_private_key(f.read(), password=None)
        self._rate_limiter = RateLimiter(max_per_second=10)
        self._session = requests.Session()

    def _sign_request(self, timestamp_ms, method, path):
        message = str(timestamp_ms) + method + path
        signature = self._private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _get_headers(self, method, path):
        timestamp_ms = int(time.time() * 1000)
        signature = self._sign_request(timestamp_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
            "Content-Type": "application/json",
        }

    def _request(self, method, path, params=None, data=None):
        if not self._private_key:
            log_error(f"KalshiAPI {method} {path}: no private key loaded, cannot sign request")
            return {}
        self._rate_limiter.wait()
        url = self.base_url + path
        headers = self._get_headers(method, path)
        try:
            resp = self._session.request(method, url, headers=headers, params=params, json=data, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log_error(f"KalshiAPI {method} {path}: {e}")
            return {}

    def get(self, path, params=None):
        return self._request("GET", path, params=params)

    def post(self, path, data=None):
        return self._request("POST", path, data=data)

    def delete(self, path):
        return self._request("DELETE", path)

    def get_balance(self) -> float:
        data = self.get("/portfolio/balance")
        balance_cents = data.get("balance", 0)
        return balance_cents / 100.0

    def get_positions(self) -> list[dict]:
        data = self.get("/portfolio/positions")
        return data.get("market_positions", [])

    def get_orders(self, status: str = "resting") -> list[dict]:
        data = self.get("/portfolio/orders", params={"status": status})
        return data.get("orders", [])

    def get_order(self, order_id: str) -> dict:
        data = self.get(f"/portfolio/orders/{order_id}")
        return data.get("order", {})

    def get_public(self, path, params=None):
        self._rate_limiter.wait()
        url = self.base_url + path
        try:
            resp = self._session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log_error(f"KalshiAPI GET_PUBLIC {path}: {e}")
            return {}

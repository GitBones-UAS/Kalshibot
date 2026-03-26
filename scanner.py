import time
from dataclasses import dataclass
from kalshi_client import KalshiAPI
from logger import log_error


@dataclass
class KalshiMarket:
    ticker: str
    event_ticker: str
    title: str
    yes_price_cents: int
    no_price_cents: int
    volume: int
    status: str
    close_time: str


class MarketScanner:
    def __init__(self, api: KalshiAPI, cache_ttl: float = 30.0):
        self.api = api
        self.cache_ttl = cache_ttl
        self._market_cache: list = []
        self._cache_time: float = 0.0

    def _parse_market(self, m: dict) -> KalshiMarket:
        yes_price = m.get("yes_bid") or m.get("last_price") or 50
        return KalshiMarket(
            ticker=m.get("ticker", ""),
            event_ticker=m.get("event_ticker", ""),
            title=m.get("title", ""),
            yes_price_cents=int(yes_price),
            no_price_cents=100 - int(yes_price),
            volume=int(m.get("volume", 0)),
            status=m.get("status", ""),
            close_time=m.get("close_time", ""),
        )

    def fetch_open_markets(self, limit=200, cursor=None, series_ticker=None):
        params = {"status": "open", "limit": limit}
        if cursor:
            params["cursor"] = cursor
        if series_ticker:
            params["series_ticker"] = series_ticker
        try:
            data = self.api.get_public("/markets", params=params)
            markets = [self._parse_market(m) for m in data.get("markets", [])]
            next_cursor = data.get("cursor", None)
            return markets, next_cursor
        except Exception as e:
            log_error(f"MarketScanner.fetch_open_markets: {e}")
            return [], None

    def fetch_all_open_markets(self):
        now = time.monotonic()
        if self._market_cache and (now - self._cache_time) < self.cache_ttl:
            return self._market_cache

        all_markets = []
        cursor = None
        while True:
            markets, cursor = self.fetch_open_markets(cursor=cursor)
            all_markets.extend(markets)
            if not cursor or not markets:
                break

        self._market_cache = all_markets
        self._cache_time = time.monotonic()
        return all_markets

    def invalidate_cache(self):
        self._market_cache = []
        self._cache_time = 0.0

    def fetch_events(self, limit=200, status="open"):
        try:
            data = self.api.get_public("/events", params={"status": status, "limit": limit})
            return data.get("events", [])
        except Exception as e:
            log_error(f"MarketScanner.fetch_events: {e}")
            return []

    def fetch_event(self, event_ticker):
        try:
            data = self.api.get_public(f"/events/{event_ticker}")
            return data.get("event", {})
        except Exception as e:
            log_error(f"MarketScanner.fetch_event({event_ticker}): {e}")
            return {}

    def fetch_orderbook(self, market_ticker):
        try:
            return self.api.get_public(f"/markets/{market_ticker}/orderbook")
        except Exception as e:
            log_error(f"MarketScanner.fetch_orderbook({market_ticker}): {e}")
            return {}

    def validate_orderbook_depth(self, market_ticker: str, side: str,
                                  price_cents: int, min_contracts: int = 1) -> bool:
        if not isinstance(market_ticker, str) or not market_ticker:
            return True  # fail-open
        try:
            orderbook = self.fetch_orderbook(market_ticker)
            if not isinstance(orderbook, dict) or not orderbook:
                return True  # fail-open on bad data
            key = "yes" if side == "yes" else "no"
            levels = orderbook.get("orderbook", {}).get(key, [])
            if not isinstance(levels, list):
                return True
            available = 0
            for level in levels:
                if not isinstance(level, (list, tuple)) or len(level) < 2:
                    continue
                level_price, level_qty = int(level[0]), int(level[1])
                if level_price <= price_cents:
                    available += level_qty
            return available >= min_contracts
        except Exception as e:
            log_error(f"MarketScanner.validate_orderbook_depth({market_ticker}): {e}")
            return True  # fail-open

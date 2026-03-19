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
    def __init__(self, api: KalshiAPI):
        self.api = api

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
        all_markets = []
        cursor = None
        while True:
            markets, cursor = self.fetch_open_markets(cursor=cursor)
            all_markets.extend(markets)
            if not cursor or not markets:
                break
            time.sleep(0.2)
        return all_markets

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

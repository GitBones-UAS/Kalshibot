import time
import threading
import requests
from config import config
from arb_engine import ArbitrageOpportunity
from logger import log_error


class AlertManager:
    def __init__(self, token: str = None, chat_id: str = None):
        self.token = token or config.TELEGRAM_TOKEN
        self.chat_id = chat_id or config.TELEGRAM_CHAT_ID
        self.enabled = bool(self.token and self.chat_id)
        self._lock = threading.Lock()
        self._last_sent = 0.0

    def _rate_limit(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_sent
            if elapsed < 2.0:
                time.sleep(2.0 - elapsed)
            self._last_sent = time.monotonic()

    def send_alert(self, message: str) -> bool:
        if not self.enabled:
            return False
        self._rate_limit()
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": message,
            })
            resp.raise_for_status()
            return True
        except Exception as e:
            log_error(f"AlertManager.send_alert: {e}")
            return False

    def send_opportunity_alert(self, opp: ArbitrageOpportunity) -> bool:
        msg = (
            f"ARB OPPORTUNITY\n"
            f"Market: {opp.market.ticker}\n"
            f"{opp.market.title}\n\n"
            f"YES: {opp.yes_price_cents}c (${opp.yes_price_cents / 100:.2f})\n"
            f"NO: {opp.no_price_cents}c (${opp.no_price_cents / 100:.2f})\n"
            f"Total cost: {opp.total_cost_cents}c (${opp.total_cost_cents / 100:.2f})\n\n"
            f"Gross spread: {opp.gross_spread_cents}c\n"
            f"Fee: {opp.fee_cents}c\n"
            f"Net profit: {opp.net_profit_cents}c (${opp.net_profit_cents / 100:.2f})\n"
            f"ROI: {opp.roi_percent}%"
        )
        return self.send_alert(msg)

    def send_trade_result(self, market: str, status: str, details: str = "") -> bool:
        msg = (
            f"TRADE {status.upper()}\n"
            f"Market: {market}\n"
            f"Status: {status}\n"
        )
        if details:
            msg += f"Details: {details}"
        return self.send_alert(msg)

    def send_daily_summary(self, stats: dict) -> bool:
        msg = (
            f"DAILY SUMMARY\n"
            f"Trades: {stats.get('daily_trade_count', 0)}/{stats.get('max_daily_trades', 0)}\n"
            f"PnL: ${stats.get('daily_pnl', 0.0):.2f}\n"
            f"Open positions: {stats.get('open_positions', 0)}\n"
            f"Total exposure: ${stats.get('total_exposure', 0.0):.2f}\n"
            f"Kill switch: {'ON' if stats.get('kill_switch') else 'OFF'}"
        )
        return self.send_alert(msg)

    def send_error_alert(self, error: str) -> bool:
        msg = f"ERROR\n{error}"
        return self.send_alert(msg)

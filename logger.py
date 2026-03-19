import csv
import os
from datetime import datetime, timezone

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")

SIGNAL_CSV = os.path.join(LOGS_DIR, "signals.csv")
SIGNAL_HEADERS = [
    "timestamp", "event_ticker", "market_ticker", "title",
    "yes_price_cents", "no_price_cents", "total_cost_cents",
    "spread_cents", "estimated_profit_cents", "action",
]

TRADE_CSV = os.path.join(LOGS_DIR, "trades.csv")
TRADE_HEADERS = [
    "timestamp", "market_ticker", "title", "side", "price_cents",
    "count", "order_id", "status", "fees_cents", "notes",
]

ERROR_LOG = os.path.join(LOGS_DIR, "errors.log")


def _ensure_csv(path, headers):
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(headers)


def _append_row(path, headers, row):
    _ensure_csv(path, headers)
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)


def _now():
    return datetime.now(timezone.utc).isoformat()


class SignalLogger:
    def log(self, event_ticker, market_ticker, title, yes_price_cents,
            no_price_cents, total_cost_cents, spread_cents,
            estimated_profit_cents, action):
        _append_row(SIGNAL_CSV, SIGNAL_HEADERS, [
            _now(), event_ticker, market_ticker, title,
            yes_price_cents, no_price_cents, total_cost_cents,
            spread_cents, estimated_profit_cents, action,
        ])


class TradeLogger:
    def log(self, market_ticker, title, side, price_cents, count,
            order_id, status, fees_cents, notes=""):
        _append_row(TRADE_CSV, TRADE_HEADERS, [
            _now(), market_ticker, title, side, price_cents,
            count, order_id, status, fees_cents, notes,
        ])


def log_error(message):
    with open(ERROR_LOG, "a") as f:
        f.write(f"[{_now()}] {message}\n")


signal_logger = SignalLogger()
trade_logger = TradeLogger()

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

MULTI_ARB_CSV = os.path.join(LOGS_DIR, "multi_arb_signals.csv")
MULTI_ARB_HEADERS = [
    "timestamp", "event_ticker", "event_title", "num_markets",
    "total_yes_cost_cents", "gross_spread_cents", "fee_cents",
    "net_profit_cents", "roi_percent", "min_volume", "markets",
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


class MultiArbLogger:
    def log(self, event_ticker, event_title, num_markets, total_yes_cost_cents,
            gross_spread_cents, fee_cents, net_profit_cents, roi_percent,
            min_volume, markets_summary):
        _append_row(MULTI_ARB_CSV, MULTI_ARB_HEADERS, [
            _now(), event_ticker, event_title, num_markets,
            total_yes_cost_cents, gross_spread_cents, fee_cents,
            net_profit_cents, roi_percent, min_volume, markets_summary,
        ])


def log_error(message):
    with open(ERROR_LOG, "a") as f:
        f.write(f"[{_now()}] {message}\n")


signal_logger = SignalLogger()
trade_logger = TradeLogger()
multi_arb_logger = MultiArbLogger()

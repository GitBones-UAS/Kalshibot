import csv
import os
import pytest
from unittest.mock import MagicMock, patch

from scanner import MarketScanner, KalshiMarket
from arb_engine import ArbEngine
from executor import TradeExecutor
from risk_manager import RiskManager
from alerts import AlertManager
from main import run_scan_cycle


def _arb_market():
    """YES=40 + NO=55 = 95 -> gross=5, fee=2, net=3."""
    return KalshiMarket(
        ticker="ARB-MKT", event_ticker="EVT-1",
        title="Arb test market",
        yes_price_cents=40, no_price_cents=55,
        volume=500, status="open",
        close_time="2026-12-31T00:00:00Z",
    )


def _fair_market():
    """YES=50 + NO=50 = 100 -> no arb."""
    return KalshiMarket(
        ticker="FAIR-MKT", event_ticker="EVT-2",
        title="Fair market",
        yes_price_cents=50, no_price_cents=50,
        volume=1000, status="open",
        close_time="2026-12-31T00:00:00Z",
    )


@pytest.fixture
def env(tmp_path):
    """Isolate all file I/O to tmp_path and mock config."""
    signals = str(tmp_path / "signals.csv")
    trades = str(tmp_path / "trades.csv")
    errors = str(tmp_path / "errors.log")
    state = str(tmp_path / "risk_state.json")

    with patch("logger.SIGNAL_CSV", signals), \
         patch("logger.TRADE_CSV", trades), \
         patch("logger.ERROR_LOG", errors), \
         patch("risk_manager.STATE_PATH", state), \
         patch("risk_manager.log_error"), \
         patch("risk_manager.config") as rcfg, \
         patch("main.config") as mcfg:

        rcfg.MAX_DAILY_TRADES = 10
        rcfg.MAX_DAILY_LOSS = 10.0
        rcfg.MAX_POSITION_SIZE = 2.0

        mcfg.MIN_SPREAD = 0.03
        mcfg.MAX_POSITION_SIZE = 2.0

        yield {"signals": signals, "trades": trades}


class TestEndToEndDryRun:
    """Full pipeline: scanner -> arb engine -> risk -> executor -> CSV logs."""

    def test_detects_arb_and_logs_to_csv(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)
        markets = [_arb_market(), _fair_market()]

        with patch.object(scanner, "fetch_all_open_markets", return_value=markets):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Signal CSV: header + 1 arb detected
        with open(env["signals"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 2
        assert rows[1][2] == "ARB-MKT"          # market_ticker
        assert rows[1][9] == "ARB_DETECTED"      # action

        # Trades CSV: header + 2 dry-run legs (yes + no)
        with open(env["trades"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3
        assert rows[1][3] == "yes"               # side
        assert rows[2][3] == "no"
        assert rows[1][7] == "dry_run"           # status
        assert rows[2][7] == "dry_run"

        # Risk manager tracked the trade
        assert risk.daily_trade_count == 1

    def test_no_arb_produces_no_csv(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_fair_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        assert not os.path.exists(env["signals"])
        assert not os.path.exists(env["trades"])
        assert risk.daily_trade_count == 0

    def test_risk_blocks_trade_but_signal_still_logged(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            risk.daily_trade_count = 10  # at limit (MAX_DAILY_TRADES=10)
            executor = TradeExecutor(api=api, dry_run=True)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Signal still logged
        with open(env["signals"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 2
        assert rows[1][2] == "ARB-MKT"

        # No trades executed
        assert not os.path.exists(env["trades"])

    def test_multiple_arbs_all_executed(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)

        arb1 = KalshiMarket(
            ticker="ARB-1", event_ticker="EVT-1", title="Arb one",
            yes_price_cents=30, no_price_cents=60,
            volume=200, status="open", close_time="2026-12-31T00:00:00Z",
        )
        arb2 = KalshiMarket(
            ticker="ARB-2", event_ticker="EVT-2", title="Arb two",
            yes_price_cents=35, no_price_cents=58,
            volume=300, status="open", close_time="2026-12-31T00:00:00Z",
        )

        with patch.object(scanner, "fetch_all_open_markets", return_value=[arb1, arb2]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # 2 signals
        with open(env["signals"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3  # header + 2

        # 4 trades (2 arbs x 2 legs each)
        with open(env["trades"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 5  # header + 4

        assert risk.daily_trade_count == 2


class TestLiveBatchExecution:
    """Test live order execution uses the batch endpoint."""

    def test_batch_endpoint_called_with_both_legs(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {
            "orders": [
                {"order": {"order_id": "yes-001", "status": "resting"}},
                {"order": {"order_id": "no-002", "status": "resting"}},
            ]
        }
        api.get_order.return_value = {"status": "filled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Single batch call with both legs
        api.post.assert_called_once()
        path = api.post.call_args[0][0]
        assert path == "/portfolio/orders/batched"
        orders = api.post.call_args[1]["data"]["orders"]
        assert len(orders) == 2
        assert orders[0]["side"] == "yes"
        assert orders[1]["side"] == "no"
        assert orders[0]["count"] == 2  # int(2.0 / 0.95) = 2
        assert orders[1]["count"] == 2

        # Trades CSV logged with API response data
        with open(env["trades"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3
        assert rows[1][6] == "yes-001"    # order_id
        assert rows[1][7] == "resting"    # status
        assert rows[2][6] == "no-002"
        assert rows[2][7] == "resting"

    def test_batch_failure_records_error(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {}  # batch call fails
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Risk manager recorded failure
        assert risk.consecutive_failures == 1

        # Trades logged as errors
        with open(env["trades"]) as f:
            rows = list(csv.reader(f))
        assert len(rows) == 3  # header + 2 error rows
        assert rows[1][7] == "error"
        assert rows[2][7] == "error"


class TestPartialFillMonitoring:
    """Test fill monitoring and unfilled leg cancellation."""

    def test_both_filled_no_cancellation(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {
            "orders": [
                {"order": {"order_id": "y-1", "status": "resting"}},
                {"order": {"order_id": "n-1", "status": "resting"}},
            ]
        }
        api.get_order.return_value = {"status": "filled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        api.delete.assert_not_called()

    def test_unfilled_leg_cancelled_after_timeout(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {
            "orders": [
                {"order": {"order_id": "y-2", "status": "resting"}},
                {"order": {"order_id": "n-2", "status": "resting"}},
            ]
        }
        # get_order always returns "resting" — never fills
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Both legs should be cancelled after timeout
        assert api.delete.call_count == 2

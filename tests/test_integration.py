import csv
import os
from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import MagicMock, patch

from scanner import MarketScanner, KalshiMarket
from arb_engine import ArbEngine
from executor import TradeExecutor
from risk_manager import RiskManager
from alerts import AlertManager
from config import config
from main import run_scan_cycle, run_multi_arb_cycle, run_validate, _build_validation_report, DEMO_BASE_URL
from multi_arb import MultiArbScanner


def _future_close_time(days=30):
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _arb_market():
    """YES=40 + NO=55 = 95 -> gross=5, fee=2, net=3."""
    return KalshiMarket(
        ticker="ARB-MKT", event_ticker="EVT-1",
        title="Arb test market",
        yes_price_cents=40, no_price_cents=55,
        volume=500, status="open",
        close_time=_future_close_time(),
    )


def _fair_market():
    """YES=50 + NO=50 = 100 -> no arb."""
    return KalshiMarket(
        ticker="FAIR-MKT", event_ticker="EVT-2",
        title="Fair market",
        yes_price_cents=50, no_price_cents=50,
        volume=1000, status="open",
        close_time=_future_close_time(),
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
        mcfg.DRY_RUN = True
        mcfg.SCAN_INTERVAL = 120
        mcfg.IS_DEMO = False

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
            volume=200, status="open", close_time=_future_close_time(),
        )
        arb2 = KalshiMarket(
            ticker="ARB-2", event_ticker="EVT-2", title="Arb two",
            yes_price_cents=35, no_price_cents=58,
            volume=300, status="open", close_time=_future_close_time(),
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

    def test_partial_fill_places_sl_tp_orders(self, env):
        """When yes fills but no doesn't, SL/TP resting orders are placed."""
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {
            "orders": [
                {"order": {"order_id": "y-3", "status": "resting"}},
                {"order": {"order_id": "n-3", "status": "resting"}},
            ]
        }
        # yes fills, no stays resting
        def get_order_side(order_id):
            if order_id == "y-3":
                return {"status": "filled"}
            return {"status": "resting"}

        api.get_order.side_effect = get_order_side
        api.delete.return_value = {"order_id": "cancelled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]), \
             patch("executor.TradeExecutor._place_sl_tp_orders") as mock_sl_tp:
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        # Unfilled no leg was cancelled
        api.delete.assert_called_once()
        # SL/TP orders placed for the filled yes leg
        mock_sl_tp.assert_called_once()
        call_args = mock_sl_tp.call_args
        assert call_args[0][2] == "yes"  # filled_side

    def test_no_sl_tp_when_both_unfilled(self, env):
        """When neither leg fills, no SL/TP orders should be placed."""
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {
            "orders": [
                {"order": {"order_id": "y-4", "status": "resting"}},
                {"order": {"order_id": "n-4", "status": "resting"}},
            ]
        }
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]), \
             patch("executor.TradeExecutor._place_sl_tp_orders") as mock_sl_tp:
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=False, fill_timeout=0.1)
            alerts = MagicMock(spec=AlertManager)
            run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        mock_sl_tp.assert_not_called()


class TestStopLossTakeProfit:
    """Test SL/TP resting order placement for uncovered positions."""

    def test_places_both_tp_and_sl_orders(self, env):
        """Both TP and SL sell orders are placed via the API."""
        api = MagicMock()
        api.post.return_value = {"order": {"order_id": "order-1", "status": "resting"}}

        executor = TradeExecutor(api=api, dry_run=False)
        result = executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                               entry_price=40, count=2)

        # Two sell orders: TP then SL
        assert api.post.call_count == 2
        tp_call = api.post.call_args_list[0]
        sl_call = api.post.call_args_list[1]

        tp_data = tp_call[1]["data"]
        assert tp_data["action"] == "sell"
        assert tp_data["side"] == "yes"
        assert tp_data["yes_price"] == 46  # int(40 * 1.15) = 46
        assert tp_data["count"] == 2

        sl_data = sl_call[1]["data"]
        assert sl_data["action"] == "sell"
        assert sl_data["side"] == "yes"
        assert sl_data["yes_price"] == 36  # int(40 * 0.90) = 36
        assert sl_data["count"] == 2

    def test_tp_cancelled_when_sl_fills_immediately(self, env):
        """If SL is a marketable limit and fills immediately, cancel the TP."""
        api = MagicMock()
        call_count = [0]

        def mock_post(path, data=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # TP order placed, resting
                return {"order": {"order_id": "tp-001", "status": "resting"}}
            else:
                # SL order fills immediately (marketable limit)
                return {"order": {"order_id": "sl-001", "status": "filled"}}

        api.post.side_effect = mock_post
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                      entry_price=40, count=2)

        # TP should be cancelled since SL filled
        api.delete.assert_called_once()
        cancel_path = api.delete.call_args[0][0]
        assert "tp-001" in cancel_path

    def test_no_cancel_when_sl_rests(self, env):
        """If SL also rests (no bids above floor), both orders stay active."""
        api = MagicMock()
        api.post.return_value = {"order": {"order_id": "order-1", "status": "resting"}}
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                      entry_price=40, count=2)

        # No cancellations — both resting
        api.delete.assert_not_called()

    def test_no_side_places_correct_price_key(self, env):
        """When the filled side is 'no', use no_price in the order."""
        api = MagicMock()
        api.post.return_value = {"order": {"order_id": "order-1", "status": "resting"}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "no",
                                      entry_price=55, count=3)

        tp_data = api.post.call_args_list[0][1]["data"]
        assert tp_data["side"] == "no"
        assert "no_price" in tp_data
        assert tp_data["no_price"] == 63  # int(55 * 1.15) = 63

        sl_data = api.post.call_args_list[1][1]["data"]
        assert sl_data["side"] == "no"
        assert "no_price" in sl_data
        assert sl_data["no_price"] == 49  # int(55 * 0.90) = 49

    def test_invalid_sl_tp_prices_skips_orders(self, env):
        """When stop_price >= take_price, no orders are placed."""
        api = MagicMock()
        executor = TradeExecutor(api=api, dry_run=False)
        with patch("executor.STOP_LOSS_PCT", 0.50), \
             patch("executor.TAKE_PROFIT_PCT", 0.10):
            # entry=1 -> SL=max(1,0)=1, TP=min(99,1)=1, SL>=TP
            executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                          entry_price=1, count=2)

        api.post.assert_not_called()

    def test_sl_price_clamped_to_1(self, env):
        """Stop price never goes below 1 cent."""
        api = MagicMock()
        api.post.return_value = {"order": {"order_id": "order-1", "status": "resting"}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                      entry_price=5, count=1)

        sl_data = api.post.call_args_list[1][1]["data"]
        assert sl_data["yes_price"] == max(1, int(5 * 0.90))  # 4

    def test_tp_price_clamped_to_99(self, env):
        """Take profit price never exceeds 99 cents."""
        api = MagicMock()
        api.post.return_value = {"order": {"order_id": "order-1", "status": "resting"}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                      entry_price=90, count=1)

        tp_data = api.post.call_args_list[0][1]["data"]
        assert tp_data["yes_price"] == 99  # min(99, int(90 * 1.15)=103) = 99

    def test_both_resting_registers_oco_pair(self, env):
        """When both SL and TP rest, they are tracked for reconciliation."""
        api = MagicMock()
        call_count = [0]

        def mock_post(path, data=None):
            call_count[0] += 1
            oid = f"order-{call_count[0]}"
            return {"order": {"order_id": oid, "status": "resting"}}

        api.post.side_effect = mock_post

        executor = TradeExecutor(api=api, dry_run=False)
        executor._place_sl_tp_orders("MKT-1", "Test", "yes",
                                      entry_price=40, count=2)

        assert len(executor._oco_pairs) == 1
        pair = executor._oco_pairs[0]
        assert pair["ticker"] == "MKT-1"
        assert pair["tp_order_id"] == "order-1"
        assert pair["sl_order_id"] == "order-2"


class TestReconcileSlTpOrders:
    """Test OCO reconciliation: when one fills, the other is cancelled."""

    def test_sl_fills_cancels_tp(self, env):
        """When SL order fills, TP order is cancelled and pair removed."""
        api = MagicMock()

        def get_order_side(order_id):
            if order_id == "sl-1":
                return {"status": "filled"}
            return {"status": "resting"}

        api.get_order.side_effect = get_order_side
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [{
            "ticker": "MKT-1", "title": "Test", "side": "yes",
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": 40, "count": 2,
        }]

        executor.reconcile_sl_tp_orders()

        # TP cancelled
        api.delete.assert_called_once()
        assert "tp-1" in api.delete.call_args[0][0]
        # Pair removed
        assert len(executor._oco_pairs) == 0

    def test_tp_fills_cancels_sl(self, env):
        """When TP order fills, SL order is cancelled and pair removed."""
        api = MagicMock()

        def get_order_side(order_id):
            if order_id == "tp-1":
                return {"status": "filled"}
            return {"status": "resting"}

        api.get_order.side_effect = get_order_side
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [{
            "ticker": "MKT-1", "title": "Test", "side": "yes",
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": 40, "count": 2,
        }]

        executor.reconcile_sl_tp_orders()

        # SL cancelled
        api.delete.assert_called_once()
        assert "sl-1" in api.delete.call_args[0][0]
        # Pair removed
        assert len(executor._oco_pairs) == 0

    def test_both_still_resting_keeps_pair(self, env):
        """When both orders are still resting, pair stays tracked."""
        api = MagicMock()
        api.get_order.return_value = {"status": "resting"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [{
            "ticker": "MKT-1", "title": "Test", "side": "yes",
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": 40, "count": 2,
        }]

        executor.reconcile_sl_tp_orders()

        api.delete.assert_not_called()
        assert len(executor._oco_pairs) == 1

    def test_both_cancelled_externally_removes_pair(self, env):
        """If both orders are cancelled (e.g. kill switch), pair is cleaned up."""
        api = MagicMock()
        api.get_order.return_value = {"status": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [{
            "ticker": "MKT-1", "title": "Test", "side": "yes",
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": 40, "count": 2,
        }]

        executor.reconcile_sl_tp_orders()

        api.delete.assert_not_called()
        assert len(executor._oco_pairs) == 0

    def test_multiple_pairs_reconciled_independently(self, env):
        """Multiple OCO pairs are checked independently."""
        api = MagicMock()

        def get_order_side(order_id):
            # Pair 1: SL filled
            if order_id == "sl-1":
                return {"status": "filled"}
            if order_id == "tp-1":
                return {"status": "resting"}
            # Pair 2: both still resting
            return {"status": "resting"}

        api.get_order.side_effect = get_order_side
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [
            {
                "ticker": "MKT-1", "title": "Test 1", "side": "yes",
                "sl_order_id": "sl-1", "tp_order_id": "tp-1",
                "entry_price": 40, "count": 2,
            },
            {
                "ticker": "MKT-2", "title": "Test 2", "side": "no",
                "sl_order_id": "sl-2", "tp_order_id": "tp-2",
                "entry_price": 55, "count": 3,
            },
        ]

        executor.reconcile_sl_tp_orders()

        # Pair 1 resolved (SL filled, TP cancelled), pair 2 remains
        assert len(executor._oco_pairs) == 1
        assert executor._oco_pairs[0]["ticker"] == "MKT-2"

    def test_api_error_keeps_pair(self, env):
        """If the API call fails, pair stays tracked for next cycle."""
        api = MagicMock()
        api.get_order.side_effect = Exception("API timeout")

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [{
            "ticker": "MKT-1", "title": "Test", "side": "yes",
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": 40, "count": 2,
        }]

        executor.reconcile_sl_tp_orders()

        # Pair kept for retry next cycle
        assert len(executor._oco_pairs) == 1


def _close_time_in_hours(hours):
    """Generate a close_time N hours from now."""
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestExpiringPositions:
    """Test pre-resolution position management."""

    def _make_pair(self, ticker="MKT-1", side="yes", entry_price=40, count=2):
        return {
            "ticker": ticker, "title": "Test", "side": side,
            "sl_order_id": "sl-1", "tp_order_id": "tp-1",
            "entry_price": entry_price, "count": count,
        }

    def test_sells_when_under_60pct_and_within_12h(self, env):
        """Position with <60% chance near expiry is sold at market."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(6),
                "yes_bid": 45,  # we hold YES at 45% — under 60%
            }
        }
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}
        api.post.return_value = {"order": {"order_id": "exit-1"}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair(side="yes")]

        executor.check_expiring_positions()

        # Both SL/TP cancelled + sell order placed
        assert api.delete.call_count == 2  # SL + TP cancelled
        api.post.assert_called_once()
        sell_data = api.post.call_args[1]["data"]
        assert sell_data["action"] == "sell"
        assert sell_data["side"] == "yes"
        assert sell_data["yes_price"] == 45
        assert sell_data["count"] == 2
        # Pair removed
        assert len(executor._oco_pairs) == 0

    def test_holds_when_over_60pct_and_within_12h(self, env):
        """Position with >=60% chance near expiry is held to resolution."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(6),
                "yes_bid": 75,  # we hold YES at 75% — over 60%
            }
        }
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair(side="yes")]

        executor.check_expiring_positions()

        # Both SL/TP cancelled, but NO sell order placed
        assert api.delete.call_count == 2
        api.post.assert_not_called()
        # Pair removed (held to resolution, no more tracking needed)
        assert len(executor._oco_pairs) == 0

    def test_no_side_price_calculated_correctly(self, env):
        """For a NO position, our price = 100 - yes_bid."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(6),
                "yes_bid": 30,  # we hold NO, so our price = 100 - 30 = 70%
            }
        }
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair(side="no")]

        executor.check_expiring_positions()

        # 70% > 60% — should hold, not sell
        api.post.assert_not_called()
        assert len(executor._oco_pairs) == 0

    def test_skips_when_more_than_12h_to_close(self, env):
        """Positions far from expiry are left alone."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(48),
                "yes_bid": 30,
            }
        }

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair()]

        executor.check_expiring_positions()

        # No orders cancelled or placed
        api.delete.assert_not_called()
        api.post.assert_not_called()
        # Pair kept
        assert len(executor._oco_pairs) == 1

    def test_missing_close_time_skips(self, env):
        """Markets with no close_time are left alone."""
        api = MagicMock()
        api.get_public.return_value = {"market": {"yes_bid": 50}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair()]

        executor.check_expiring_positions()

        api.delete.assert_not_called()
        assert len(executor._oco_pairs) == 1

    def test_api_error_keeps_pair(self, env):
        """On API error, pair stays tracked for retry."""
        api = MagicMock()
        api.get_public.side_effect = Exception("timeout")

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair()]

        executor.check_expiring_positions()

        assert len(executor._oco_pairs) == 1

    def test_already_filled_orders_not_cancelled_again(self, env):
        """If SL/TP already filled before expiry check, don't try to cancel."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(6),
                "yes_bid": 45,
            }
        }
        api.get_order.return_value = {"status": "filled"}
        api.delete.return_value = {"order_id": "cancelled"}
        api.post.return_value = {"order": {"order_id": "exit-1"}}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair(side="yes")]

        executor.check_expiring_positions()

        # Already filled — no delete calls
        api.delete.assert_not_called()
        assert len(executor._oco_pairs) == 0

    def test_exactly_60pct_holds(self, env):
        """At exactly 60%, position is held (>= threshold)."""
        api = MagicMock()
        api.get_public.return_value = {
            "market": {
                "close_time": _close_time_in_hours(6),
                "yes_bid": 60,
            }
        }
        api.get_order.return_value = {"status": "resting"}
        api.delete.return_value = {"order_id": "cancelled"}

        executor = TradeExecutor(api=api, dry_run=False)
        executor._oco_pairs = [self._make_pair(side="yes")]

        executor.check_expiring_positions()

        # Held — no sell order
        api.post.assert_not_called()
        assert len(executor._oco_pairs) == 0


class TestScanCycleReturnsStats:
    """run_scan_cycle and run_multi_arb_cycle return stats dicts."""

    def test_binary_scan_returns_stats(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        api.post.return_value = {"orders": [
            {"order": {"order_id": "y-1", "status": "filled"}},
            {"order": {"order_id": "n-1", "status": "filled"}},
        ]}
        api.get_order.return_value = {"status": "filled"}
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[_arb_market()]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True, fill_timeout=0.01)
            alerts = MagicMock(spec=AlertManager)
            stats = run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        assert "markets_scanned" in stats
        assert "opps_found" in stats
        assert "spreads" in stats
        assert "errors" in stats
        assert stats["markets_scanned"] == 1
        assert stats["opps_found"] >= 1

    def test_binary_scan_no_markets(self, env):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[]):
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            alerts = MagicMock(spec=AlertManager)
            stats = run_scan_cycle(scanner, ArbEngine(), executor, risk, alerts, api)

        assert stats["markets_scanned"] == 0
        assert stats["opps_found"] == 0

    def test_multi_scan_returns_stats(self, env):
        api = MagicMock()
        multi_scanner = MultiArbScanner(api)

        with patch.object(multi_scanner, "fetch_multi_outcome_events", return_value=[]), \
             patch.object(multi_scanner, "scan_for_multi_arb", return_value=[]):
            stats = run_multi_arb_cycle(multi_scanner)

        assert "events_scanned" in stats
        assert "opps_found" in stats
        assert "spreads" in stats
        assert stats["events_scanned"] == 0


class TestValidationReport:
    """Test the validation report builder."""

    def test_report_contains_all_sections(self):
        binary = {"markets_scanned": 100, "opps_found": 3, "spreads": [5, 7, 3], "errors": 1}
        multi = {"events_scanned": 20, "opps_found": 1, "spreads": [10], "errors": 0}
        report = _build_validation_report(5, 1800.0, binary, multi)

        assert "VALIDATION REPORT" in report
        assert "OVERVIEW" in report
        assert "BINARY ARBITRAGE" in report
        assert "MULTI-OUTCOME ARBITRAGE" in report
        assert "30.0 minutes" in report
        assert "5" in report  # cycles
        assert "100" in report  # markets scanned
        assert "3" in report  # binary opps
        assert "5.0c" in report  # avg spread (5+7+3)/3 = 5.0
        assert "7c" in report  # max spread
        assert "20" in report  # events scanned
        assert "10.0c" in report  # multi avg spread

    def test_report_zero_cycles(self):
        binary = {"markets_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}
        multi = {"events_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}
        report = _build_validation_report(0, 0.0, binary, multi)
        assert "VALIDATION REPORT" in report
        assert "0c" in report  # avg/max spread

    def test_report_no_spreads(self):
        binary = {"markets_scanned": 50, "opps_found": 0, "spreads": [], "errors": 0}
        multi = {"events_scanned": 10, "opps_found": 0, "spreads": [], "errors": 0}
        report = _build_validation_report(3, 900.0, binary, multi)
        assert "0.0c" in report


class TestValidateMode:
    """Test run_validate runs scans and produces a report file."""

    def test_validate_forces_dry_run(self, env, tmp_path):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        scanner = MarketScanner(api)
        engine = ArbEngine()
        multi_scanner = MultiArbScanner(api)
        executor = TradeExecutor(api=api, dry_run=False)
        risk = RiskManager()
        alerts = MagicMock(spec=AlertManager)

        with patch.object(scanner, "fetch_all_open_markets", return_value=[]), \
             patch.object(multi_scanner, "fetch_multi_outcome_events", return_value=[]), \
             patch("main.VALIDATE_DURATION", 0.1), \
             patch("main.LOGS_DIR", str(tmp_path)), \
             patch.object(config, "SCAN_INTERVAL", 120):
            run_validate(scanner, engine, multi_scanner, executor, risk, alerts, api)

        # Dry run was forced
        assert config.DRY_RUN is True
        assert executor.dry_run is True

        # Report file created
        report_path = tmp_path / "validation_report.txt"
        assert report_path.exists()
        content = report_path.read_text()
        assert "VALIDATION REPORT" in content
        assert "BINARY ARBITRAGE" in content
        assert "MULTI-OUTCOME ARBITRAGE" in content


class TestDemoFlag:
    """Test --demo flag overrides the base URL."""

    def test_demo_url_constant(self):
        assert "demo" in DEMO_BASE_URL.lower()

    def test_demo_sets_config(self, env):
        original_url = config.KALSHI_BASE_URL
        original_demo = config.IS_DEMO
        try:
            config.KALSHI_BASE_URL = DEMO_BASE_URL
            config.IS_DEMO = True
            assert config.IS_DEMO is True
            assert "demo" in config.KALSHI_BASE_URL.lower()
        finally:
            config.KALSHI_BASE_URL = original_url
            config.IS_DEMO = original_demo

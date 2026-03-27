import json
import os
from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import MagicMock, patch

from pnl_tracker import PnLTracker


@pytest.fixture
def tracker(tmp_path):
    path = str(tmp_path / "pnl_log.json")
    return PnLTracker(path=path)


# ── 1. Recording trades ──

class TestRecordTrades:
    def test_record_arb_complete(self, tracker):
        tracker.record_arb_complete("MKT-1", yes_price=40, no_price=55, count=2)
        assert len(tracker._trades) == 1
        t = tracker._trades[0]
        assert t["type"] == "arb_complete"
        assert t["ticker"] == "MKT-1"
        assert t["entry_price_cents"] == 95  # 40 + 55
        assert t["exit_price_cents"] == 100
        assert t["count"] == 2
        # gross = (100-95)*2 = 10, fees = 2*2*2 = 8, pnl = 2
        assert t["pnl_cents"] == 2
        assert t["fees_cents"] == 8

    def test_record_position_close_sl(self, tracker):
        tracker.record_position_close("MKT-1", "stop_loss", "yes",
                                       entry_price=40, exit_price=36, count=3)
        t = tracker._trades[0]
        assert t["type"] == "stop_loss"
        # gross = (36-40)*3 = -12, fees = 2*3 = 6, pnl = -18
        assert t["pnl_cents"] == -18
        assert t["fees_cents"] == 6

    def test_record_position_close_tp(self, tracker):
        tracker.record_position_close("MKT-1", "take_profit", "yes",
                                       entry_price=40, exit_price=46, count=2)
        t = tracker._trades[0]
        assert t["type"] == "take_profit"
        # gross = (46-40)*2 = 12, fees = 2*2 = 4, pnl = 8
        assert t["pnl_cents"] == 8

    def test_record_resolution(self, tracker):
        tracker.record_resolution("MKT-1", "yes", entry_price=40,
                                   resolved_price=100, count=2)
        t = tracker._trades[0]
        assert t["type"] == "resolution"
        # gross = (100-40)*2 = 120, fees = 2*2 = 4, pnl = 116
        assert t["pnl_cents"] == 116

    def test_persists_to_disk(self, tracker, tmp_path):
        tracker.record_arb_complete("MKT-1", 40, 55, 1)
        path = str(tmp_path / "pnl_log.json")
        assert os.path.exists(path)
        with open(path) as f:
            data = json.load(f)
        assert len(data["closed_trades"]) == 1

    def test_loads_from_disk(self, tmp_path):
        path = str(tmp_path / "pnl_log.json")
        t1 = PnLTracker(path=path)
        t1.record_arb_complete("MKT-1", 40, 55, 1)
        t2 = PnLTracker(path=path)
        assert len(t2._trades) == 1
        assert t2._trades[0]["ticker"] == "MKT-1"


# ── 2. Total P&L ──

class TestTotalPnL:
    def test_empty_tracker(self, tracker):
        result = tracker.get_total_pnl()
        assert result["total_pnl_cents"] == 0
        assert result["trade_count"] == 0
        assert result["win_rate"] == 0.0

    def test_aggregates_multiple_trades(self, tracker):
        tracker.record_arb_complete("MKT-1", 40, 55, 2)  # pnl = 2
        tracker.record_position_close("MKT-2", "stop_loss", "yes", 40, 36, 3)  # pnl = -18
        tracker.record_position_close("MKT-3", "take_profit", "no", 55, 63, 2)  # gross=16, fees=4, pnl=12
        result = tracker.get_total_pnl()
        # 2 + (-18) + 12 = -4
        assert result["total_pnl_cents"] == -4

    def test_win_loss_counts(self, tracker):
        tracker.record_arb_complete("MKT-1", 40, 55, 2)  # win (+2)
        tracker.record_position_close("MKT-2", "stop_loss", "yes", 40, 36, 1)  # loss (-10)
        tracker.record_position_close("MKT-3", "take_profit", "yes", 40, 46, 1)  # win (+4)
        result = tracker.get_total_pnl()
        assert result["wins"] == 2
        assert result["losses"] == 1
        assert result["trade_count"] == 3

    def test_win_rate_calculation(self, tracker):
        tracker.record_arb_complete("MKT-1", 40, 55, 1)  # win
        tracker.record_arb_complete("MKT-2", 40, 55, 1)  # win
        tracker.record_position_close("MKT-3", "stop_loss", "yes", 40, 36, 1)  # loss
        result = tracker.get_total_pnl()
        assert result["win_rate"] == round(2 / 3 * 100, 1)


# ── 3. Weekly P&L ──

class TestWeeklyPnL:
    def test_empty_tracker(self, tracker):
        assert tracker.get_weekly_pnl() == []

    def test_groups_by_iso_week(self, tracker):
        # Record trades with specific timestamps in different weeks
        tracker._trades = [
            {
                "timestamp": "2026-03-02T12:00:00+00:00",  # Mon, W10
                "ticker": "MKT-1", "type": "arb_complete", "side": "both",
                "entry_price_cents": 95, "exit_price_cents": 100,
                "count": 1, "pnl_cents": 10, "fees_cents": 4,
            },
            {
                "timestamp": "2026-03-04T12:00:00+00:00",  # Wed, W10
                "ticker": "MKT-2", "type": "stop_loss", "side": "yes",
                "entry_price_cents": 40, "exit_price_cents": 36,
                "count": 1, "pnl_cents": -6, "fees_cents": 2,
            },
            {
                "timestamp": "2026-03-09T12:00:00+00:00",  # Mon, W11
                "ticker": "MKT-3", "type": "take_profit", "side": "no",
                "entry_price_cents": 55, "exit_price_cents": 63,
                "count": 1, "pnl_cents": 6, "fees_cents": 2,
            },
        ]
        weeks = tracker.get_weekly_pnl()
        assert len(weeks) == 2
        assert weeks[0]["week"] == "2026-W10"
        assert weeks[0]["pnl_cents"] == 4  # 10 + (-6)
        assert weeks[0]["trade_count"] == 2
        assert weeks[0]["wins"] == 1
        assert weeks[0]["losses"] == 1
        assert weeks[1]["week"] == "2026-W11"
        assert weeks[1]["pnl_cents"] == 6
        assert weeks[1]["trade_count"] == 1

    def test_sorted_by_week(self, tracker):
        tracker._trades = [
            {
                "timestamp": "2026-03-16T12:00:00+00:00",  # W12
                "ticker": "A", "type": "arb_complete", "side": "both",
                "entry_price_cents": 95, "exit_price_cents": 100,
                "count": 1, "pnl_cents": 5, "fees_cents": 4,
            },
            {
                "timestamp": "2026-03-02T12:00:00+00:00",  # W10
                "ticker": "B", "type": "arb_complete", "side": "both",
                "entry_price_cents": 95, "exit_price_cents": 100,
                "count": 1, "pnl_cents": 3, "fees_cents": 4,
            },
        ]
        weeks = tracker.get_weekly_pnl()
        assert weeks[0]["week"] < weeks[1]["week"]


# ── 4. Open Positions ──

class TestOpenPositions:
    def test_fetches_from_api(self, tracker):
        api = MagicMock()
        api.get_positions.return_value = [
            {"ticker": "MKT-1", "side": "yes", "position": 3,
             "market_exposure": 120, "total_traded": 3, "resting_orders_count": 0},
            {"ticker": "MKT-2", "side": "no", "position": 2,
             "market_exposure": 110, "total_traded": 2, "resting_orders_count": 0},
        ]
        result = tracker.get_open_positions(api)
        assert result["count"] == 2
        assert result["total_value_cents"] == 230
        assert result["total_value_usd"] == 2.30

    def test_filters_zero_positions(self, tracker):
        api = MagicMock()
        api.get_positions.return_value = [
            {"ticker": "MKT-1", "side": "yes", "position": 0,
             "market_exposure": 0, "total_traded": 0, "resting_orders_count": 0},
        ]
        result = tracker.get_open_positions(api)
        assert result["count"] == 0

    def test_api_error_returns_empty(self, tracker):
        api = MagicMock()
        api.get_positions.side_effect = Exception("timeout")
        result = tracker.get_open_positions(api)
        assert result["count"] == 0
        assert result["total_value_usd"] == 0.0

    def test_empty_portfolio(self, tracker):
        api = MagicMock()
        api.get_positions.return_value = []
        result = tracker.get_open_positions(api)
        assert result["count"] == 0


# ── 5. Formatting ──

class TestFormatting:
    def test_format_total_pnl(self, tracker):
        tracker.record_arb_complete("MKT-1", 40, 55, 2)
        msg = tracker.format_total_pnl()
        assert "P&L SUMMARY" in msg
        assert "Total P&L:" in msg
        assert "Win rate:" in msg

    def test_format_weekly_pnl_empty(self, tracker):
        msg = tracker.format_weekly_pnl()
        assert "No trades recorded" in msg

    def test_format_weekly_pnl_with_data(self, tracker):
        tracker.record_arb_complete("MKT-1", 40, 55, 1)
        msg = tracker.format_weekly_pnl()
        assert "WEEKLY P&L" in msg
        assert "TOTAL:" in msg

    def test_format_open_positions_empty(self, tracker):
        api = MagicMock()
        api.get_positions.return_value = []
        msg = tracker.format_open_positions(api)
        assert "No open positions" in msg

    def test_format_open_positions_with_data(self, tracker):
        api = MagicMock()
        api.get_positions.return_value = [
            {"ticker": "MKT-1", "side": "yes", "position": 2,
             "market_exposure": 80, "total_traded": 2, "resting_orders_count": 0},
        ]
        msg = tracker.format_open_positions(api)
        assert "OPEN POSITIONS" in msg
        assert "MKT-1" in msg
        assert "Count: 1" in msg


# ── 6. Telegram command integration ──

class TestTelegramCommands:
    def test_profitloss_command(self, tracker):
        from main import handle_telegram_commands
        from executor import TradeExecutor
        from risk_manager import RiskManager

        poller = MagicMock()
        poller.poll_commands.return_value = ["/profitloss"]
        api = MagicMock()
        alerts = MagicMock()

        with patch("risk_manager.STATE_PATH", "/dev/null"), \
             patch("risk_manager.config") as rcfg:
            rcfg.MAX_DAILY_TRADES = 10
            rcfg.MAX_DAILY_LOSS = 10.0
            rcfg.MAX_POSITION_SIZE = 2.0
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            handle_telegram_commands(poller, risk, api, executor, alerts, tracker)

        alerts.send_alert.assert_called_once()
        msg = alerts.send_alert.call_args[0][0]
        assert "P&L SUMMARY" in msg

    def test_weekprofitloss_command(self, tracker):
        from main import handle_telegram_commands
        from executor import TradeExecutor
        from risk_manager import RiskManager

        poller = MagicMock()
        poller.poll_commands.return_value = ["/weekprofitloss"]
        api = MagicMock()
        alerts = MagicMock()

        with patch("risk_manager.STATE_PATH", "/dev/null"), \
             patch("risk_manager.config") as rcfg:
            rcfg.MAX_DAILY_TRADES = 10
            rcfg.MAX_DAILY_LOSS = 10.0
            rcfg.MAX_POSITION_SIZE = 2.0
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            handle_telegram_commands(poller, risk, api, executor, alerts, tracker)

        alerts.send_alert.assert_called_once()
        msg = alerts.send_alert.call_args[0][0]
        assert "WEEKLY P&L" in msg

    def test_openpositions_command(self, tracker):
        from main import handle_telegram_commands
        from executor import TradeExecutor
        from risk_manager import RiskManager

        poller = MagicMock()
        poller.poll_commands.return_value = ["/openpositions"]
        api = MagicMock()
        api.get_positions.return_value = []
        alerts = MagicMock()

        with patch("risk_manager.STATE_PATH", "/dev/null"), \
             patch("risk_manager.config") as rcfg:
            rcfg.MAX_DAILY_TRADES = 10
            rcfg.MAX_DAILY_LOSS = 10.0
            rcfg.MAX_POSITION_SIZE = 2.0
            risk = RiskManager()
            executor = TradeExecutor(api=api, dry_run=True)
            handle_telegram_commands(poller, risk, api, executor, alerts, tracker)

        alerts.send_alert.assert_called_once()
        msg = alerts.send_alert.call_args[0][0]
        assert "OPEN POSITIONS" in msg

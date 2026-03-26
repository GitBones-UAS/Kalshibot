import json
import os
import pytest
from unittest.mock import patch, MagicMock
from risk_manager import RiskManager, MIN_BALANCE_USD, MAX_CONSECUTIVE_FAILURES


@pytest.fixture
def state_file(tmp_path):
    path = str(tmp_path / "risk_state.json")
    with patch("risk_manager.STATE_PATH", path):
        yield path


@pytest.fixture
def mock_config():
    with patch("risk_manager.config") as cfg:
        cfg.MAX_DAILY_TRADES = 5
        cfg.MAX_DAILY_LOSS = 10.0
        cfg.MAX_POSITION_SIZE = 2.0
        yield cfg


@pytest.fixture
def risk(state_file, mock_config):
    with patch("risk_manager.log_error"):
        yield RiskManager()


# ── 1. Trade limits ──

class TestCanTrade:
    def test_allows_trade_within_limits(self, risk):
        ok, reason = risk.can_trade(1.0)
        assert ok is True
        assert reason == "ok"

    def test_blocks_when_daily_limit_reached(self, risk, mock_config):
        risk.daily_trade_count = mock_config.MAX_DAILY_TRADES
        ok, reason = risk.can_trade(1.0)
        assert ok is False
        assert "daily trade limit" in reason

    def test_blocks_when_size_exceeds_max(self, risk):
        ok, reason = risk.can_trade(5.0)  # > MAX_POSITION_SIZE=2.0
        assert ok is False
        assert "exceeds max position" in reason

    def test_blocks_when_daily_loss_exceeded(self, risk):
        risk.daily_pnl = -10.0
        ok, reason = risk.can_trade(1.0)
        assert ok is False
        assert "daily loss" in reason
        assert risk.kill_switch is True

    def test_blocks_when_exposure_limit_exceeded(self, risk):
        # MAX_POSITION_SIZE * MAX_DAILY_TRADES = 2.0 * 5 = 10.0
        risk.total_exposure = 9.5
        ok, reason = risk.can_trade(1.0)  # 9.5 + 1.0 > 10.0
        assert ok is False
        assert "exposure" in reason


# ── 2. Kill switch ──

class TestKillSwitch:
    def test_activate_blocks_trading(self, risk):
        risk.activate_kill_switch("test reason")
        assert risk.kill_switch is True
        assert risk.kill_reason == "test reason"
        ok, reason = risk.can_trade(1.0)
        assert ok is False
        assert "kill switch" in reason

    def test_deactivate_resumes_trading(self, risk):
        risk.activate_kill_switch("test")
        risk.deactivate_kill_switch()
        assert risk.kill_switch is False
        assert risk.kill_reason == ""
        ok, _ = risk.can_trade(1.0)
        assert ok is True

    def test_deactivate_resets_consecutive_failures(self, risk):
        risk.consecutive_failures = 5
        risk.deactivate_kill_switch()
        assert risk.consecutive_failures == 0


# ── 3. Daily reset ──

class TestDailyReset:
    def test_resets_on_new_day(self, risk):
        risk.daily_trade_count = 5
        risk.daily_pnl = -3.0
        risk.consecutive_failures = 2
        risk.last_reset_date = "2020-01-01"
        risk.check_daily_reset()
        assert risk.daily_trade_count == 0
        assert risk.daily_pnl == 0.0
        assert risk.consecutive_failures == 0

    def test_no_reset_same_day(self, risk):
        risk.daily_trade_count = 3
        risk.check_daily_reset()
        assert risk.daily_trade_count == 3


# ── 4. Record trade ──

class TestRecordTrade:
    def test_increments_trade_count(self, risk):
        risk.record_trade("MKT-A", 1.5)
        assert risk.daily_trade_count == 1

    def test_accumulates_exposure(self, risk):
        risk.record_trade("MKT-A", 1.5)
        risk.record_trade("MKT-B", 0.5)
        assert risk.total_exposure == 2.0

    def test_tracks_pnl(self, risk):
        risk.record_trade("MKT-A", 1.5, pnl=0.50)
        assert risk.daily_pnl == 0.50

    def test_appends_position(self, risk):
        risk.record_trade("MKT-A", 1.5)
        assert len(risk.open_positions) == 1
        assert risk.open_positions[0]["ticker"] == "MKT-A"
        assert risk.open_positions[0]["size_usd"] == 1.5

    def test_resets_consecutive_failures(self, risk):
        risk.consecutive_failures = 2
        risk.record_trade("MKT-A", 1.5)
        assert risk.consecutive_failures == 0


# ── 5. Consecutive failures ──

class TestConsecutiveFailures:
    def test_increments_on_failure(self, risk):
        risk.record_failure()
        assert risk.consecutive_failures == 1

    def test_kill_switch_on_max_failures(self, risk):
        for _ in range(MAX_CONSECUTIVE_FAILURES):
            risk.record_failure()
        assert risk.kill_switch is True
        assert str(MAX_CONSECUTIVE_FAILURES) in risk.kill_reason

    def test_no_kill_switch_below_max(self, risk):
        for _ in range(MAX_CONSECUTIVE_FAILURES - 1):
            risk.record_failure()
        assert risk.kill_switch is False


# ── 6. State persistence ──

class TestStatePersistence:
    def test_save_and_load_roundtrip(self, state_file, mock_config):
        with patch("risk_manager.log_error"):
            rm1 = RiskManager()
            rm1.daily_trade_count = 3
            rm1.daily_pnl = -2.5
            rm1.total_exposure = 4.0
            rm1.kill_switch = True
            rm1.kill_reason = "test"
            rm1.save_state()

            rm2 = RiskManager()
            assert rm2.daily_trade_count == 3
            assert rm2.daily_pnl == -2.5
            assert rm2.total_exposure == 4.0
            assert rm2.kill_switch is True
            assert rm2.kill_reason == "test"

    def test_load_missing_file_uses_defaults(self, tmp_path, mock_config):
        missing = str(tmp_path / "nonexistent.json")
        with patch("risk_manager.STATE_PATH", missing), \
             patch("risk_manager.log_error"):
            rm = RiskManager()
            assert rm.daily_trade_count == 0
            assert rm.kill_switch is False

    def test_state_file_created_on_save(self, state_file, mock_config):
        with patch("risk_manager.log_error"):
            rm = RiskManager()
            rm.save_state()
            assert os.path.exists(state_file)
            with open(state_file) as f:
                data = json.load(f)
            assert "daily_trade_count" in data


# ── 7. Balance check ──

class TestBalanceCheck:
    def test_low_balance_triggers_kill_switch(self, risk):
        api = MagicMock()
        api.get_balance.return_value = 10.0  # < MIN_BALANCE_USD (50)
        result = risk.check_balance(api)
        assert result is False
        assert risk.kill_switch is True
        assert "balance" in risk.kill_reason

    def test_sufficient_balance_passes(self, risk):
        api = MagicMock()
        api.get_balance.return_value = 100.0
        result = risk.check_balance(api)
        assert result is True
        assert risk.kill_switch is False

    def test_api_error_returns_true(self, risk):
        api = MagicMock()
        api.get_balance.side_effect = Exception("API down")
        result = risk.check_balance(api)
        assert result is True  # fails open

import json
import os
from datetime import datetime, timezone
from config import config
from logger import log_error

STATE_PATH = os.path.join(os.path.dirname(__file__), "logs", "risk_state.json")

MIN_BALANCE_USD = 50.0
MAX_CONSECUTIVE_FAILURES = 3


class RiskManager:
    def __init__(self):
        self.daily_trade_count = 0
        self.daily_pnl = 0.0
        self.open_positions = []
        self.total_exposure = 0.0
        self.kill_switch = False
        self.kill_reason = ""
        self.consecutive_failures = 0
        self.last_reset_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.load_state()

    def check_daily_reset(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.last_reset_date:
            self.daily_trade_count = 0
            self.daily_pnl = 0.0
            self.consecutive_failures = 0
            self.last_reset_date = today
            self.save_state()

    def can_trade(self, size_usd: float) -> tuple[bool, str]:
        self.check_daily_reset()

        if self.kill_switch:
            return False, f"kill switch is active: {self.kill_reason}"

        if self.daily_trade_count >= config.MAX_DAILY_TRADES:
            return False, f"daily trade limit reached ({config.MAX_DAILY_TRADES})"

        if self.daily_pnl <= -config.MAX_DAILY_LOSS:
            self.activate_kill_switch("daily loss limit exceeded")
            return False, f"daily loss limit reached (${config.MAX_DAILY_LOSS})"

        if size_usd > config.MAX_POSITION_SIZE:
            return False, f"size ${size_usd} exceeds max position ${config.MAX_POSITION_SIZE}"

        if self.total_exposure + size_usd > config.MAX_POSITION_SIZE * config.MAX_DAILY_TRADES:
            return False, "total exposure limit exceeded"

        return True, "ok"

    def check_balance(self, api) -> bool:
        try:
            balance = api.get_balance()
            if balance < MIN_BALANCE_USD:
                self.activate_kill_switch(f"balance too low: ${balance:.2f}")
                return False
            return True
        except Exception as e:
            log_error(f"RiskManager.check_balance: {e}")
            return True

    def record_trade(self, ticker: str, size_usd: float, pnl: float = 0.0):
        self.daily_trade_count += 1
        self.daily_pnl += pnl
        self.total_exposure += size_usd
        self.consecutive_failures = 0
        self.open_positions.append({
            "ticker": ticker,
            "size_usd": size_usd,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.save_state()

    def record_failure(self):
        self.consecutive_failures += 1
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.activate_kill_switch(f"{self.consecutive_failures} consecutive failures")
        self.save_state()

    def close_position(self, ticker: str, pnl: float = 0.0):
        self.open_positions = [p for p in self.open_positions if p["ticker"] != ticker]
        self.daily_pnl += pnl
        self.total_exposure = sum(p["size_usd"] for p in self.open_positions)
        self.save_state()

    def activate_kill_switch(self, reason: str = "manual"):
        self.kill_switch = True
        self.kill_reason = reason
        log_error(f"KILL SWITCH ACTIVATED: {reason}")
        self.save_state()

    def deactivate_kill_switch(self):
        self.kill_switch = False
        self.kill_reason = ""
        self.consecutive_failures = 0
        self.save_state()

    def get_status(self) -> dict:
        self.check_daily_reset()
        return {
            "daily_trade_count": self.daily_trade_count,
            "max_daily_trades": config.MAX_DAILY_TRADES,
            "daily_pnl": self.daily_pnl,
            "max_daily_loss": config.MAX_DAILY_LOSS,
            "open_positions": len(self.open_positions),
            "total_exposure": self.total_exposure,
            "kill_switch": self.kill_switch,
            "kill_reason": self.kill_reason,
            "consecutive_failures": self.consecutive_failures,
            "last_reset_date": self.last_reset_date,
        }

    def save_state(self):
        state = {
            "daily_trade_count": self.daily_trade_count,
            "daily_pnl": self.daily_pnl,
            "open_positions": self.open_positions,
            "total_exposure": self.total_exposure,
            "kill_switch": self.kill_switch,
            "kill_reason": self.kill_reason,
            "consecutive_failures": self.consecutive_failures,
            "last_reset_date": self.last_reset_date,
        }
        try:
            with open(STATE_PATH, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log_error(f"RiskManager.save_state: {e}")

    def load_state(self):
        if not os.path.exists(STATE_PATH):
            return
        try:
            with open(STATE_PATH, "r") as f:
                state = json.load(f)
            self.daily_trade_count = state.get("daily_trade_count", 0)
            self.daily_pnl = state.get("daily_pnl", 0.0)
            self.open_positions = state.get("open_positions", [])
            self.total_exposure = state.get("total_exposure", 0.0)
            self.kill_switch = state.get("kill_switch", False)
            self.kill_reason = state.get("kill_reason", "")
            self.consecutive_failures = state.get("consecutive_failures", 0)
            self.last_reset_date = state.get("last_reset_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
            self.check_daily_reset()
        except Exception as e:
            log_error(f"RiskManager.load_state: {e}")

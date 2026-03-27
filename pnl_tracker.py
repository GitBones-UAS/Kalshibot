import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from logger import log_error

PNL_LOG_PATH = os.path.join(os.path.dirname(__file__), "logs", "pnl_log.json")

FEE_PER_CONTRACT_CENTS = 2


class PnLTracker:
    def __init__(self, path: str = None):
        self._path = path or PNL_LOG_PATH
        self._trades = []
        self._load()

    def _load(self):
        if not os.path.exists(self._path):
            self._trades = []
            return
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            self._trades = data.get("closed_trades", [])
        except Exception as e:
            log_error(f"PnLTracker._load: {e}")
            self._trades = []

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            with open(self._path, "w") as f:
                json.dump({"closed_trades": self._trades}, f, indent=2)
        except Exception as e:
            log_error(f"PnLTracker._save: {e}")

    def record_arb_complete(self, ticker: str, yes_price: int, no_price: int, count: int):
        """Both legs filled — guaranteed profit of (100 - total_cost) per contract."""
        total_cost = yes_price + no_price
        gross_profit = (100 - total_cost) * count
        fees = FEE_PER_CONTRACT_CENTS * 2 * count  # fee on each leg
        pnl = gross_profit - fees
        self._trades.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "type": "arb_complete",
            "side": "both",
            "entry_price_cents": total_cost,
            "exit_price_cents": 100,
            "count": count,
            "pnl_cents": pnl,
            "fees_cents": fees,
        })
        self._save()

    def record_position_close(self, ticker: str, trade_type: str, side: str,
                               entry_price: int, exit_price: int, count: int):
        """Single-leg exit: SL, TP, or expiry sell."""
        gross_pnl = (exit_price - entry_price) * count
        fees = FEE_PER_CONTRACT_CENTS * count  # fee on exit order
        pnl = gross_pnl - fees
        self._trades.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "type": trade_type,
            "side": side,
            "entry_price_cents": entry_price,
            "exit_price_cents": exit_price,
            "count": count,
            "pnl_cents": pnl,
            "fees_cents": fees,
        })
        self._save()

    def record_resolution(self, ticker: str, side: str, entry_price: int,
                           resolved_price: int, count: int):
        """Position held to resolution (expiry_hold)."""
        gross_pnl = (resolved_price - entry_price) * count
        fees = FEE_PER_CONTRACT_CENTS * count
        pnl = gross_pnl - fees
        self._trades.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "type": "resolution",
            "side": side,
            "entry_price_cents": entry_price,
            "exit_price_cents": resolved_price,
            "count": count,
            "pnl_cents": pnl,
            "fees_cents": fees,
        })
        self._save()

    def get_total_pnl(self) -> dict:
        total_pnl = sum(t["pnl_cents"] for t in self._trades)
        total_fees = sum(t["fees_cents"] for t in self._trades)
        trade_count = len(self._trades)
        wins = sum(1 for t in self._trades if t["pnl_cents"] > 0)
        losses = sum(1 for t in self._trades if t["pnl_cents"] < 0)
        return {
            "total_pnl_cents": total_pnl,
            "total_pnl_usd": total_pnl / 100.0,
            "total_fees_cents": total_fees,
            "trade_count": trade_count,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / trade_count * 100, 1) if trade_count else 0.0,
        }

    def get_weekly_pnl(self) -> list[dict]:
        weeks = defaultdict(lambda: {"pnl_cents": 0, "fees_cents": 0, "trade_count": 0,
                                      "wins": 0, "losses": 0})
        for t in self._trades:
            try:
                ts = datetime.fromisoformat(t["timestamp"])
            except (ValueError, TypeError):
                continue
            iso = ts.isocalendar()
            week_key = f"{iso[0]}-W{iso[1]:02d}"
            w = weeks[week_key]
            w["pnl_cents"] += t["pnl_cents"]
            w["fees_cents"] += t["fees_cents"]
            w["trade_count"] += 1
            if t["pnl_cents"] > 0:
                w["wins"] += 1
            elif t["pnl_cents"] < 0:
                w["losses"] += 1

        result = []
        for week_key in sorted(weeks.keys()):
            w = weeks[week_key]
            result.append({
                "week": week_key,
                "pnl_cents": w["pnl_cents"],
                "pnl_usd": w["pnl_cents"] / 100.0,
                "fees_cents": w["fees_cents"],
                "trade_count": w["trade_count"],
                "wins": w["wins"],
                "losses": w["losses"],
            })
        return result

    def get_open_positions(self, api) -> dict:
        """Fetch current open positions from Kalshi API."""
        try:
            positions = api.get_positions()
            if not isinstance(positions, list):
                return {"count": 0, "total_value_cents": 0, "total_value_usd": 0.0, "positions": []}

            active = []
            total_value = 0
            for p in positions:
                qty = int(p.get("total_traded", 0)) - int(p.get("resting_orders_count", 0))
                if qty <= 0:
                    qty = int(p.get("position", 0))
                if qty <= 0:
                    continue
                market_value = int(p.get("market_exposure", 0))
                if market_value == 0:
                    # Estimate from position * last price if exposure not available
                    market_value = qty * int(p.get("average_price", 0))
                total_value += abs(market_value)
                active.append({
                    "ticker": p.get("ticker", ""),
                    "side": p.get("side", ""),
                    "quantity": qty,
                    "value_cents": abs(market_value),
                })

            return {
                "count": len(active),
                "total_value_cents": total_value,
                "total_value_usd": total_value / 100.0,
                "positions": active,
            }
        except Exception as e:
            log_error(f"PnLTracker.get_open_positions: {e}")
            return {"count": 0, "total_value_cents": 0, "total_value_usd": 0.0, "positions": []}

    def format_total_pnl(self) -> str:
        s = self.get_total_pnl()
        sign = "+" if s["total_pnl_usd"] >= 0 else ""
        return (
            f"P&L SUMMARY\n"
            f"Total P&L: {sign}${s['total_pnl_usd']:.2f} ({s['total_pnl_cents']}c)\n"
            f"Total fees: ${s['total_fees_cents'] / 100:.2f}\n"
            f"Trades: {s['trade_count']} ({s['wins']}W / {s['losses']}L)\n"
            f"Win rate: {s['win_rate']}%"
        )

    def format_weekly_pnl(self) -> str:
        weeks = self.get_weekly_pnl()
        if not weeks:
            return "WEEKLY P&L\nNo trades recorded yet."
        lines = ["WEEKLY P&L"]
        for w in weeks:
            sign = "+" if w["pnl_usd"] >= 0 else ""
            lines.append(
                f"  {w['week']}: {sign}${w['pnl_usd']:.2f} "
                f"({w['trade_count']} trades, {w['wins']}W/{w['losses']}L)"
            )
        total = self.get_total_pnl()
        sign = "+" if total["total_pnl_usd"] >= 0 else ""
        lines.append(f"  TOTAL: {sign}${total['total_pnl_usd']:.2f}")
        return "\n".join(lines)

    def format_open_positions(self, api) -> str:
        p = self.get_open_positions(api)
        if p["count"] == 0:
            return "OPEN POSITIONS\nNo open positions."
        lines = [
            f"OPEN POSITIONS",
            f"Count: {p['count']}",
            f"Total value: ${p['total_value_usd']:.2f}",
        ]
        for pos in p["positions"]:
            lines.append(
                f"  {pos['ticker']} {pos['side'].upper()} "
                f"x{pos['quantity']} (${pos['value_cents'] / 100:.2f})"
            )
        return "\n".join(lines)

import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from kalshi_client import KalshiAPI
from arb_engine import ArbitrageOpportunity
from logger import trade_logger, log_error

STOP_LOSS_PCT = 0.10   # -10%
TAKE_PROFIT_PCT = 0.15  # +15%
EXPIRY_WINDOW_HOURS = 12
HOLD_THRESHOLD_PCT = 60  # hold if >=60% chance of resolving in our favor


class TradeExecutor:
    def __init__(self, api: KalshiAPI = None, dry_run: bool = True, fill_timeout: float = 5.0,
                 pnl_tracker=None):
        self.api = api
        self.dry_run = dry_run
        self.fill_timeout = fill_timeout
        self.pnl_tracker = pnl_tracker
        self._oco_pairs = []  # [{ticker, title, side, sl_order_id, tp_order_id, entry_price, count}]

    def execute_arb_trade(self, opportunity: ArbitrageOpportunity, size_usd: float) -> dict:
        ticker = opportunity.market.ticker
        title = opportunity.market.title
        yes_price = opportunity.yes_price_cents
        no_price = opportunity.no_price_cents
        total_cost_dollars = opportunity.total_cost_cents / 100.0

        if total_cost_dollars <= 0:
            log_error(f"TradeExecutor: invalid total_cost_cents={opportunity.total_cost_cents} for {ticker}")
            return {"status": "error", "reason": "invalid total cost"}

        count = int(size_usd / total_cost_dollars)
        if count <= 0:
            log_error(f"TradeExecutor: size_usd={size_usd} too small for {ticker} (cost={total_cost_dollars})")
            return {"status": "error", "reason": "size too small for one contract"}

        if self.dry_run:
            trade_logger.log(ticker, title, "yes", yes_price, count, "DRY_RUN", "dry_run", 0, "dry run")
            trade_logger.log(ticker, title, "no", no_price, count, "DRY_RUN", "dry_run", 0, "dry run")
            return {
                "status": "dry_run",
                "ticker": ticker,
                "count": count,
                "yes_price_cents": yes_price,
                "no_price_cents": no_price,
            }

        # Live orders — batch both legs in a single API call
        yes_order_id = str(uuid4())
        no_order_id = str(uuid4())

        batch_result = self.api.post("/portfolio/orders/batched", data={
            "orders": [
                {
                    "ticker": ticker,
                    "action": "buy",
                    "side": "yes",
                    "count": count,
                    "type": "limit",
                    "yes_price": yes_price,
                    "client_order_id": yes_order_id,
                },
                {
                    "ticker": ticker,
                    "action": "buy",
                    "side": "no",
                    "count": count,
                    "type": "limit",
                    "no_price": no_price,
                    "client_order_id": no_order_id,
                },
            ]
        })

        if not batch_result:
            trade_logger.log(ticker, title, "yes", yes_price, count,
                             yes_order_id, "error", 0, "batch request failed")
            trade_logger.log(ticker, title, "no", no_price, count,
                             no_order_id, "error", 0, "batch request failed")
            return {"status": "error", "reason": "batch order request failed"}

        order_results = batch_result.get("orders", [])
        yes_result = order_results[0] if len(order_results) > 0 else {}
        no_result = order_results[1] if len(order_results) > 1 else {}

        yes_status = "error" if not yes_result else yes_result.get("order", {}).get("status", "submitted")
        no_status = "error" if not no_result else no_result.get("order", {}).get("status", "submitted")

        trade_logger.log(ticker, title, "yes", yes_price, count,
                         yes_result.get("order", {}).get("order_id", yes_order_id),
                         yes_status, 0, "")
        trade_logger.log(ticker, title, "no", no_price, count,
                         no_result.get("order", {}).get("order_id", no_order_id),
                         no_status, 0, "")

        # Monitor for partial fills and cancel unfilled legs
        yes_oid = yes_result.get("order", {}).get("order_id", "")
        no_oid = no_result.get("order", {}).get("order_id", "")
        if yes_oid and no_oid:
            self._monitor_and_cancel_unfilled(ticker, title, yes_oid, no_oid,
                                               yes_price, no_price, count)

        return {
            "status": "submitted",
            "ticker": ticker,
            "count": count,
            "yes_order": yes_result,
            "no_order": no_result,
        }

    def _monitor_and_cancel_unfilled(self, ticker: str, title: str,
                                      yes_order_id: str, no_order_id: str,
                                      yes_price: int = 0, no_price: int = 0,
                                      count: int = 0):
        deadline = time.monotonic() + self.fill_timeout
        poll_interval = 0.5

        while time.monotonic() < deadline:
            try:
                yes_order = self.api.get_order(yes_order_id)
                no_order = self.api.get_order(no_order_id)
            except Exception as e:
                log_error(f"TradeExecutor: fill monitor error for {ticker}: {e}")
                return

            yes_filled = yes_order.get("status") == "filled"
            no_filled = no_order.get("status") == "filled"

            if yes_filled and no_filled:
                if self.pnl_tracker:
                    self.pnl_tracker.record_arb_complete(ticker, yes_price, no_price, count)
                return  # both filled, arb complete

            yes_done = yes_order.get("status") in ("filled", "canceled", "cancelled")
            no_done = no_order.get("status") in ("filled", "canceled", "cancelled")
            if yes_done and no_done:
                return  # both terminal

            time.sleep(poll_interval)

        # Timeout — determine fill state and act
        try:
            yes_order = self.api.get_order(yes_order_id)
            no_order = self.api.get_order(no_order_id)
        except Exception as e:
            log_error(f"TradeExecutor: final status check failed for {ticker}: {e}")
            return

        yes_filled = yes_order.get("status") == "filled"
        no_filled = no_order.get("status") == "filled"

        if yes_filled and no_filled:
            if self.pnl_tracker:
                self.pnl_tracker.record_arb_complete(ticker, yes_price, no_price, count)
            return  # both filled during final check

        # Cancel any resting legs
        for oid, side in [(yes_order_id, "yes"), (no_order_id, "no")]:
            try:
                order = self.api.get_order(oid)
                if order.get("status") == "resting":
                    self.cancel_order(oid)
                    trade_logger.log(ticker, title, side, 0, 0, oid,
                                     "cancelled_unfilled", 0,
                                     f"partial fill timeout ({self.fill_timeout}s)")
            except Exception as e:
                log_error(f"TradeExecutor: cancel unfilled {side} {oid}: {e}")

        # Partial fill — one leg filled, place SL/TP resting orders for the uncovered position
        if yes_filled and not no_filled:
            self._place_sl_tp_orders(ticker, title, "yes", yes_price, count)
        elif no_filled and not yes_filled:
            self._place_sl_tp_orders(ticker, title, "no", no_price, count)

    def _place_sl_tp_orders(self, ticker: str, title: str, filled_side: str,
                             entry_price: int, count: int):
        stop_price = max(1, int(entry_price * (1 - STOP_LOSS_PCT)))
        take_price = min(99, int(entry_price * (1 + TAKE_PROFIT_PCT)))

        if stop_price >= take_price:
            log_error(f"TradeExecutor: invalid SL/TP for {ticker}: SL={stop_price} >= TP={take_price}")
            return {"sl_order": {}, "tp_order": {}}

        # Place take profit — resting sell at target price above market
        tp_result = self.api.post("/portfolio/orders", data={
            "ticker": ticker,
            "action": "sell",
            "side": filled_side,
            "count": count,
            "type": "limit",
            f"{filled_side}_price": take_price,
            "client_order_id": str(uuid4()),
        })
        tp_order_id = tp_result.get("order", {}).get("order_id", "")
        tp_status = tp_result.get("order", {}).get("status", "unknown")
        trade_logger.log(ticker, title, filled_side, take_price, count,
                         tp_order_id, "take_profit_placed", 0,
                         f"entry={entry_price}c target={take_price}c")

        # Place stop loss — resting sell at floor price below market
        sl_result = self.api.post("/portfolio/orders", data={
            "ticker": ticker,
            "action": "sell",
            "side": filled_side,
            "count": count,
            "type": "limit",
            f"{filled_side}_price": stop_price,
            "client_order_id": str(uuid4()),
        })
        sl_order_id = sl_result.get("order", {}).get("order_id", "")
        sl_status = sl_result.get("order", {}).get("status", "unknown")
        trade_logger.log(ticker, title, filled_side, stop_price, count,
                         sl_order_id, "stop_loss_placed", 0,
                         f"entry={entry_price}c floor={stop_price}c")

        # If the SL filled immediately (marketable limit), cancel the resting TP
        if sl_status == "filled" and tp_order_id:
            try:
                self.cancel_order(tp_order_id)
                trade_logger.log(ticker, title, filled_side, take_price, count,
                                 tp_order_id, "tp_cancelled_sl_filled", 0,
                                 f"SL filled immediately, TP no longer needed")
            except Exception as e:
                log_error(f"TradeExecutor: failed to cancel TP after SL fill for {ticker}: {e}")
        elif sl_order_id and tp_order_id:
            # Both resting — track as OCO pair for reconciliation
            self._oco_pairs.append({
                "ticker": ticker,
                "title": title,
                "side": filled_side,
                "sl_order_id": sl_order_id,
                "tp_order_id": tp_order_id,
                "entry_price": entry_price,
                "count": count,
            })

        return {"sl_order": sl_result, "tp_order": tp_result}

    def reconcile_sl_tp_orders(self):
        remaining = []
        for pair in self._oco_pairs:
            try:
                sl_order = self.api.get_order(pair["sl_order_id"])
                tp_order = self.api.get_order(pair["tp_order_id"])
                sl_status = sl_order.get("status", "")
                tp_status = tp_order.get("status", "")

                if sl_status == "filled":
                    # SL filled — cancel TP
                    if tp_status not in ("filled", "canceled", "cancelled"):
                        self.cancel_order(pair["tp_order_id"])
                    sl_fill_price = int(sl_order.get("yes_price", 0) or sl_order.get("no_price", 0))
                    trade_logger.log(pair["ticker"], pair["title"], pair["side"],
                                     0, pair["count"], pair["sl_order_id"],
                                     "stop_loss_filled", 0,
                                     f"entry={pair['entry_price']}c, TP cancelled")
                    if self.pnl_tracker:
                        stop_price = max(1, int(pair["entry_price"] * (1 - STOP_LOSS_PCT)))
                        exit_px = sl_fill_price if sl_fill_price > 0 else stop_price
                        self.pnl_tracker.record_position_close(
                            pair["ticker"], "stop_loss", pair["side"],
                            pair["entry_price"], exit_px, pair["count"])
                    continue  # resolved, don't keep

                if tp_status == "filled":
                    # TP filled — cancel SL
                    if sl_status not in ("filled", "canceled", "cancelled"):
                        self.cancel_order(pair["sl_order_id"])
                    tp_fill_price = int(tp_order.get("yes_price", 0) or tp_order.get("no_price", 0))
                    trade_logger.log(pair["ticker"], pair["title"], pair["side"],
                                     0, pair["count"], pair["tp_order_id"],
                                     "take_profit_filled", 0,
                                     f"entry={pair['entry_price']}c, SL cancelled")
                    if self.pnl_tracker:
                        take_price = min(99, int(pair["entry_price"] * (1 + TAKE_PROFIT_PCT)))
                        exit_px = tp_fill_price if tp_fill_price > 0 else take_price
                        self.pnl_tracker.record_position_close(
                            pair["ticker"], "take_profit", pair["side"],
                            pair["entry_price"], exit_px, pair["count"])
                    continue  # resolved, don't keep

                # Both cancelled externally (e.g. kill switch)
                if sl_status in ("canceled", "cancelled") and tp_status in ("canceled", "cancelled"):
                    continue  # resolved, don't keep

                # Still pending — keep tracking
                remaining.append(pair)

            except Exception as e:
                log_error(f"TradeExecutor: reconcile error for {pair['ticker']}: {e}")
                remaining.append(pair)  # keep tracking on error

        self._oco_pairs = remaining

    def check_expiring_positions(self):
        remaining = []
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=EXPIRY_WINDOW_HOURS)

        for pair in self._oco_pairs:
            try:
                market_data = self.api.get_public(f"/markets/{pair['ticker']}")
                market = market_data.get("market", market_data)
                close_time_str = market.get("close_time", "")

                if not close_time_str:
                    remaining.append(pair)
                    continue

                try:
                    close_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    remaining.append(pair)
                    continue

                if close_dt > cutoff:
                    remaining.append(pair)
                    continue

                # Within 12 hours of resolution — cancel both SL and TP orders
                for oid, label in [(pair["sl_order_id"], "SL"), (pair["tp_order_id"], "TP")]:
                    try:
                        order = self.api.get_order(oid)
                        if order.get("status") not in ("filled", "canceled", "cancelled"):
                            self.cancel_order(oid)
                    except Exception as e:
                        log_error(f"TradeExecutor: expiry cancel {label} {oid}: {e}")

                # Check current price to determine hold vs sell
                yes_price = market.get("yes_bid") or market.get("last_price") or 50
                yes_price = int(yes_price)
                side = pair["side"]
                our_price = yes_price if side == "yes" else (100 - yes_price)

                if our_price >= HOLD_THRESHOLD_PCT:
                    # >60% chance — hold to resolution
                    trade_logger.log(pair["ticker"], pair["title"], side,
                                     our_price, pair["count"], "",
                                     "expiry_hold", 0,
                                     f"price={our_price}c >= {HOLD_THRESHOLD_PCT}%, "
                                     f"holding to resolution at {close_time_str}")
                else:
                    # <60% chance — sell at market
                    result = self.api.post("/portfolio/orders", data={
                        "ticker": pair["ticker"],
                        "action": "sell",
                        "side": side,
                        "count": pair["count"],
                        "type": "limit",
                        f"{side}_price": max(1, our_price),
                        "client_order_id": str(uuid4()),
                    })
                    exit_oid = result.get("order", {}).get("order_id", "")
                    trade_logger.log(pair["ticker"], pair["title"], side,
                                     our_price, pair["count"], exit_oid,
                                     "expiry_sell", 0,
                                     f"price={our_price}c < {HOLD_THRESHOLD_PCT}%, "
                                     f"sold before resolution at {close_time_str}")
                    if self.pnl_tracker:
                        self.pnl_tracker.record_position_close(
                            pair["ticker"], "expiry_sell", side,
                            pair["entry_price"], our_price, pair["count"])

                # Pair resolved either way — don't keep tracking
                continue

            except Exception as e:
                log_error(f"TradeExecutor: expiry check error for {pair['ticker']}: {e}")
                remaining.append(pair)

        self._oco_pairs = remaining

    def cancel_order(self, order_id: str) -> dict:
        result = self.api.delete(f"/portfolio/orders/{order_id}")
        if result:
            trade_logger.log("", "", "", 0, 0, order_id, "cancelled", 0, "user cancelled")
        else:
            log_error(f"TradeExecutor: failed to cancel order {order_id}")
        return result

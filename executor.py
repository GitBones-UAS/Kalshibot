import time
from uuid import uuid4
from kalshi_client import KalshiAPI
from arb_engine import ArbitrageOpportunity
from logger import trade_logger, log_error


class TradeExecutor:
    def __init__(self, api: KalshiAPI = None, dry_run: bool = True, fill_timeout: float = 5.0):
        self.api = api
        self.dry_run = dry_run
        self.fill_timeout = fill_timeout

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
            self._monitor_and_cancel_unfilled(ticker, title, yes_oid, no_oid)

        return {
            "status": "submitted",
            "ticker": ticker,
            "count": count,
            "yes_order": yes_result,
            "no_order": no_result,
        }

    def _monitor_and_cancel_unfilled(self, ticker: str, title: str,
                                      yes_order_id: str, no_order_id: str):
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
                return  # both filled, arb complete

            yes_done = yes_order.get("status") in ("filled", "canceled", "cancelled")
            no_done = no_order.get("status") in ("filled", "canceled", "cancelled")
            if yes_done and no_done:
                return  # both terminal

            time.sleep(poll_interval)

        # Timeout — cancel any resting (unfilled) legs
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

    def cancel_order(self, order_id: str) -> dict:
        result = self.api.delete(f"/portfolio/orders/{order_id}")
        if result:
            trade_logger.log("", "", "", 0, 0, order_id, "cancelled", 0, "user cancelled")
        else:
            log_error(f"TradeExecutor: failed to cancel order {order_id}")
        return result

from uuid import uuid4
from kalshi_client import KalshiAPI
from arb_engine import ArbitrageOpportunity
from logger import trade_logger, log_error


class TradeExecutor:
    def __init__(self, api: KalshiAPI = None, dry_run: bool = True):
        self.api = api
        self.dry_run = dry_run

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

        # Live orders
        yes_order_id = str(uuid4())
        no_order_id = str(uuid4())

        yes_result = self.api.post("/portfolio/orders", data={
            "ticker": ticker,
            "action": "buy",
            "side": "yes",
            "count": count,
            "type": "limit",
            "yes_price": yes_price,
            "client_order_id": yes_order_id,
        })

        no_result = self.api.post("/portfolio/orders", data={
            "ticker": ticker,
            "action": "buy",
            "side": "no",
            "count": count,
            "type": "limit",
            "no_price": no_price,
            "client_order_id": no_order_id,
        })

        yes_status = "error" if not yes_result else yes_result.get("order", {}).get("status", "submitted")
        no_status = "error" if not no_result else no_result.get("order", {}).get("status", "submitted")

        trade_logger.log(ticker, title, "yes", yes_price, count,
                         yes_result.get("order", {}).get("order_id", yes_order_id),
                         yes_status, 0, "")
        trade_logger.log(ticker, title, "no", no_price, count,
                         no_result.get("order", {}).get("order_id", no_order_id),
                         no_status, 0, "")

        return {
            "status": "submitted",
            "ticker": ticker,
            "count": count,
            "yes_order": yes_result,
            "no_order": no_result,
        }

    def cancel_order(self, order_id: str) -> dict:
        result = self.api.delete(f"/portfolio/orders/{order_id}")
        if result:
            trade_logger.log("", "", "", 0, 0, order_id, "cancelled", 0, "user cancelled")
        else:
            log_error(f"TradeExecutor: failed to cancel order {order_id}")
        return result

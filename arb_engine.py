from dataclasses import dataclass
from datetime import datetime, timezone
from scanner import KalshiMarket
from logger import signal_logger, log_error

FEE_PER_CONTRACT_CENTS = 2


@dataclass
class ArbitrageOpportunity:
    market: KalshiMarket
    yes_price_cents: int
    no_price_cents: int
    total_cost_cents: int
    gross_spread_cents: int
    fee_cents: int
    net_profit_cents: int
    roi_percent: float
    timestamp: datetime


class ArbEngine:
    def scan_for_arbitrage(self, markets: list[KalshiMarket],
                           min_spread_cents: int = 3) -> list[ArbitrageOpportunity]:
        opportunities = []
        now = datetime.now(timezone.utc)

        for market in markets:
            try:
                yes = market.yes_price_cents
                no = market.no_price_cents
                total = yes + no

                if total >= (100 - min_spread_cents):
                    continue

                gross_spread = 100 - total
                fee = FEE_PER_CONTRACT_CENTS
                net_profit = gross_spread - fee

                if net_profit <= 0:
                    continue

                roi = (net_profit / total) * 100 if total > 0 else 0.0

                opportunities.append(ArbitrageOpportunity(
                    market=market,
                    yes_price_cents=yes,
                    no_price_cents=no,
                    total_cost_cents=total,
                    gross_spread_cents=gross_spread,
                    fee_cents=fee,
                    net_profit_cents=net_profit,
                    roi_percent=round(roi, 2),
                    timestamp=now,
                ))
            except Exception as e:
                log_error(f"ArbEngine.scan_for_arbitrage({market.ticker}): {e}")

        opportunities.sort(key=lambda o: o.roi_percent, reverse=True)
        return opportunities

    def is_profitable(self, yes_cents: int, no_cents: int) -> bool:
        total = yes_cents + no_cents
        gross_spread = 100 - total
        net_profit = gross_spread - FEE_PER_CONTRACT_CENTS
        return net_profit > 0

    def format_opportunity(self, opp: ArbitrageOpportunity) -> str:
        return (
            f"[{opp.market.ticker}] {opp.market.title}\n"
            f"  YES {opp.yes_price_cents}¢ + NO {opp.no_price_cents}¢ = {opp.total_cost_cents}¢\n"
            f"  Gross spread: {opp.gross_spread_cents}¢ | Fee: {opp.fee_cents}¢ | "
            f"Net profit: {opp.net_profit_cents}¢\n"
            f"  ROI: {opp.roi_percent}%"
        )

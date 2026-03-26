from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from kalshi_client import KalshiAPI
from logger import log_error

FEE_PER_CONTRACT_CENTS = 2


@dataclass
class MultiOutcomeOpportunity:
    event_ticker: str
    event_title: str
    num_markets: int
    markets: list[dict]
    total_yes_cost_cents: int
    gross_spread_cents: int
    fee_cents: int
    net_profit_cents: int
    roi_percent: float
    min_volume: int
    timestamp: datetime


class MultiArbScanner:
    def __init__(self, api: KalshiAPI):
        self.api = api

    def _fetch_event_detail(self, event_ticker: str) -> dict:
        try:
            return self.api.get_public(f"/events/{event_ticker}")
        except Exception as e:
            log_error(f"MultiArbScanner.fetch_event({event_ticker}): {e}")
            return {}

    def fetch_multi_outcome_events(self, limit=200, max_events=20, max_workers=5) -> list[dict]:
        events = []
        cursor = None

        while True:
            params = {"status": "open", "limit": limit}
            if cursor:
                params["cursor"] = cursor

            try:
                data = self.api.get_public("/events", params=params)
            except Exception as e:
                log_error(f"MultiArbScanner.fetch_multi_outcome_events: {e}")
                break

            batch = data.get("events", [])
            if not batch:
                break

            event_tickers = [
                (e.get("event_ticker", ""), e.get("title", ""))
                for e in batch if e.get("event_ticker")
            ]

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(self._fetch_event_detail, ticker): (ticker, title)
                    for ticker, title in event_tickers
                }
                for future in as_completed(futures):
                    ticker, title = futures[future]
                    detail = future.result()
                    event_markets = detail.get("markets", [])
                    open_markets = [m for m in event_markets if m.get("status") == "open"]

                    if len(open_markets) >= 3:
                        events.append({
                            "event_ticker": ticker,
                            "title": title,
                            "markets": open_markets,
                        })

            if max_events and len(events) >= max_events:
                break

            cursor = data.get("cursor")
            if not cursor:
                break

        return events

    def scan_for_multi_arb(self, events: list[dict],
                           min_spread_cents: int = 3) -> list[MultiOutcomeOpportunity]:
        opportunities = []
        now = datetime.now(timezone.utc)

        for event in events:
            try:
                market_dicts = []
                volumes = []

                for m in event["markets"]:
                    yes_price = m.get("yes_bid") or m.get("last_price") or 50
                    volume = int(m.get("volume", 0))
                    market_dicts.append({
                        "ticker": m.get("ticker", ""),
                        "title": m.get("title", ""),
                        "yes_price_cents": int(yes_price),
                    })
                    volumes.append(volume)

                if any(v == 0 for v in volumes):
                    continue

                total_yes = sum(md["yes_price_cents"] for md in market_dicts)
                num_markets = len(market_dicts)

                if total_yes >= (100 - min_spread_cents):
                    continue

                gross_spread = 100 - total_yes
                fee = FEE_PER_CONTRACT_CENTS * num_markets
                net_profit = gross_spread - fee

                if net_profit <= 0:
                    continue

                roi = (net_profit / total_yes) * 100 if total_yes > 0 else 0.0

                opportunities.append(MultiOutcomeOpportunity(
                    event_ticker=event["event_ticker"],
                    event_title=event.get("title", ""),
                    num_markets=num_markets,
                    markets=market_dicts,
                    total_yes_cost_cents=total_yes,
                    gross_spread_cents=gross_spread,
                    fee_cents=fee,
                    net_profit_cents=net_profit,
                    roi_percent=round(roi, 2),
                    min_volume=min(volumes),
                    timestamp=now,
                ))
            except Exception as e:
                log_error(f"MultiArbScanner.scan_for_multi_arb({event.get('event_ticker', '?')}): {e}")

        opportunities.sort(key=lambda o: o.roi_percent, reverse=True)
        return opportunities

    def format_opportunity(self, opp: MultiOutcomeOpportunity) -> str:
        lines = [
            f"[MULTI-ARB] {opp.event_ticker} — {opp.event_title}",
            f"  {opp.num_markets} outcomes, total YES cost: {opp.total_yes_cost_cents}¢",
        ]
        for m in opp.markets:
            lines.append(f"    {m['ticker']}: YES {m['yes_price_cents']}¢ — {m['title']}")
        lines.append(
            f"  Gross spread: {opp.gross_spread_cents}¢ | "
            f"Fee: {opp.fee_cents}¢ ({opp.num_markets}x{FEE_PER_CONTRACT_CENTS}¢) | "
            f"Net profit: {opp.net_profit_cents}¢"
        )
        lines.append(f"  ROI: {opp.roi_percent}% | Min volume: {opp.min_volume}")
        return "\n".join(lines)

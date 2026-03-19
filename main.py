import argparse
import asyncio
import os
from datetime import datetime, timezone

from config import config
from kalshi_client import KalshiAPI
from scanner import MarketScanner
from arb_engine import ArbEngine
from multi_arb import MultiArbScanner
from logger import signal_logger, multi_arb_logger, log_error

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")


def print_banner():
    mode = "DEMO" if config.IS_DEMO else "PRODUCTION"
    dry = "ON" if config.DRY_RUN else "OFF"
    min_spread_cents = int(config.MIN_SPREAD * 100)
    print("=" * 45)
    print("  kalshi_bot")
    print("=" * 45)
    print(f"  Mode:          {mode}")
    print(f"  Dry run:       {dry}")
    print(f"  Scan interval: {config.SCAN_INTERVAL}s")
    print(f"  Min spread:    {min_spread_cents}¢")
    print(f"  Max position:  ${config.MAX_POSITION_SIZE}")
    print(f"  Max daily loss:${config.MAX_DAILY_LOSS}")
    print("=" * 45)


def run_scan_cycle(scanner: MarketScanner, engine: ArbEngine):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{now}] Scanning markets...")

    markets = scanner.fetch_all_open_markets()
    print(f"  Fetched {len(markets)} open markets")

    if not markets:
        print("  No markets found, skipping.")
        return

    min_spread_cents = int(config.MIN_SPREAD * 100)
    opportunities = engine.scan_for_arbitrage(markets, min_spread_cents=min_spread_cents)

    for opp in opportunities:
        m = opp.market
        signal_logger.log(
            event_ticker=m.event_ticker,
            market_ticker=m.ticker,
            title=m.title,
            yes_price_cents=opp.yes_price_cents,
            no_price_cents=opp.no_price_cents,
            total_cost_cents=opp.total_cost_cents,
            spread_cents=opp.gross_spread_cents,
            estimated_profit_cents=opp.net_profit_cents,
            action="ARB_DETECTED",
        )

    if opportunities:
        print(f"  Found {len(opportunities)} binary arb opportunities:")
        for opp in opportunities:
            print(f"    {engine.format_opportunity(opp)}")
    else:
        print("  No binary arb opportunities found.")


def run_multi_arb_cycle(multi_scanner: MultiArbScanner):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{now}] Scanning multi-outcome events...")

    min_spread_cents = int(config.MIN_SPREAD * 100)
    events = multi_scanner.fetch_multi_outcome_events()
    print(f"  Found {len(events)} events with 3+ outcomes")

    if not events:
        print("  No multi-outcome events found, skipping.")
        return

    opportunities = multi_scanner.scan_for_multi_arb(events, min_spread_cents=min_spread_cents)

    for opp in opportunities:
        markets_str = "; ".join(
            f"{m['ticker']}={m['yes_price_cents']}c" for m in opp.markets
        )
        multi_arb_logger.log(
            event_ticker=opp.event_ticker,
            event_title=opp.event_title,
            num_markets=opp.num_markets,
            total_yes_cost_cents=opp.total_yes_cost_cents,
            gross_spread_cents=opp.gross_spread_cents,
            fee_cents=opp.fee_cents,
            net_profit_cents=opp.net_profit_cents,
            roi_percent=opp.roi_percent,
            min_volume=opp.min_volume,
            markets_summary=markets_str,
        )

    if opportunities:
        print(f"  Found {len(opportunities)} multi-arb opportunities:")
        for opp in opportunities:
            print(f"    {multi_scanner.format_opportunity(opp)}")
    else:
        print("  No multi-arb opportunities found.")


async def main():
    parser = argparse.ArgumentParser(description="kalshi_bot - Kalshi arbitrage scanner")
    parser.add_argument("--scan-once", action="store_true",
                        help="Run a single scan and exit")
    args = parser.parse_args()

    os.makedirs(LOGS_DIR, exist_ok=True)

    try:
        config.validate()
    except (ValueError, FileNotFoundError) as e:
        print(f"Config error: {e}")
        return

    api = KalshiAPI(
        api_key_id=config.KALSHI_API_KEY_ID,
        private_key_path=config.KALSHI_PRIVATE_KEY_PATH,
        base_url=config.KALSHI_BASE_URL,
    )
    scanner = MarketScanner(api)
    engine = ArbEngine()
    multi_scanner = MultiArbScanner(api)

    print_banner()

    if args.scan_once:
        run_scan_cycle(scanner, engine)
        run_multi_arb_cycle(multi_scanner)
        print("\nSingle scan complete.")
        return

    print(f"Starting continuous scan (every {config.SCAN_INTERVAL}s)...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            try:
                run_scan_cycle(scanner, engine)
                run_multi_arb_cycle(multi_scanner)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                log_error(f"main loop: {e}")
                print(f"  Error during scan: {e}")
            await asyncio.sleep(config.SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")


if __name__ == "__main__":
    asyncio.run(main())

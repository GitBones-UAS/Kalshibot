import argparse
import asyncio
import os
import time
from datetime import datetime, timezone

import requests
from config import config
from kalshi_client import KalshiAPI
from scanner import MarketScanner
from arb_engine import ArbEngine
from multi_arb import MultiArbScanner
from executor import TradeExecutor
from risk_manager import RiskManager
from alerts import AlertManager
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


def cancel_all_orders(api: KalshiAPI, executor: TradeExecutor, alerts: AlertManager):
    orders = api.get_orders(status="resting")
    if not orders:
        return
    cancelled = 0
    for order in orders:
        order_id = order.get("order_id", "")
        if order_id:
            executor.cancel_order(order_id)
            cancelled += 1
    if cancelled:
        alerts.send_alert(f"Kill switch: cancelled {cancelled} open orders")
        print(f"  Kill switch: cancelled {cancelled} open orders")


class TelegramPoller:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self.last_update_id = 0

    def poll_commands(self) -> list[str]:
        if not self.enabled:
            return []
        try:
            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            resp = requests.get(url, params={
                "offset": self.last_update_id + 1,
                "timeout": 0,
            }, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            commands = []
            for update in data.get("result", []):
                self.last_update_id = update["update_id"]
                msg = update.get("message", {})
                if str(msg.get("chat", {}).get("id", "")) != self.chat_id:
                    continue
                text = msg.get("text", "").strip().lower()
                if text.startswith("/"):
                    commands.append(text)
            return commands
        except Exception as e:
            log_error(f"TelegramPoller.poll_commands: {e}")
            return []


def handle_telegram_commands(poller: TelegramPoller, risk: RiskManager,
                             api: KalshiAPI, executor: TradeExecutor,
                             alerts: AlertManager):
    commands = poller.poll_commands()
    for cmd in commands:
        if cmd == "/kill":
            risk.activate_kill_switch("manual via Telegram")
            cancel_all_orders(api, executor, alerts)
            alerts.send_alert("KILL SWITCH ACTIVATED (manual)")
            print("  Telegram command: /kill")
        elif cmd == "/status":
            status = risk.get_status()
            msg = (
                f"STATUS\n"
                f"Trades: {status['daily_trade_count']}/{status['max_daily_trades']}\n"
                f"PnL: ${status['daily_pnl']:.2f}\n"
                f"Open positions: {status['open_positions']}\n"
                f"Exposure: ${status['total_exposure']:.2f}\n"
                f"Kill switch: {'ON - ' + status['kill_reason'] if status['kill_switch'] else 'OFF'}\n"
                f"Failures: {status['consecutive_failures']}"
            )
            alerts.send_alert(msg)
            print("  Telegram command: /status")
        elif cmd == "/resume":
            risk.deactivate_kill_switch()
            alerts.send_alert("Kill switch deactivated. Bot resumed.")
            print("  Telegram command: /resume")


def run_scan_cycle(scanner: MarketScanner, engine: ArbEngine, executor: TradeExecutor,
                   risk: RiskManager, alerts: AlertManager, api: KalshiAPI):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{now}] Scanning markets...")

    risk.check_balance(api)
    if risk.kill_switch:
        print(f"  Kill switch active: {risk.kill_reason}")
        return

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

        try:
            alerts.send_opportunity_alert(opp)
        except Exception as e:
            log_error(f"Alert failed for {m.ticker}: {e}")

        size_usd = config.MAX_POSITION_SIZE
        can, reason = risk.can_trade(size_usd)

        if can:
            count = int(size_usd / (opp.total_cost_cents / 100.0)) if opp.total_cost_cents > 0 else 0
            yes_deep = scanner.validate_orderbook_depth(m.ticker, "yes", opp.yes_price_cents, count)
            no_deep = scanner.validate_orderbook_depth(m.ticker, "no", opp.no_price_cents, count)
            if not yes_deep or not no_deep:
                print(f"    Skipping {m.ticker}: insufficient orderbook depth")
                continue

            try:
                result = executor.execute_arb_trade(opp, size_usd)
                status = result.get("status", "unknown")
                alerts.send_trade_result(m.ticker, status, f"count={result.get('count', 0)}")
                if status in ("submitted", "dry_run"):
                    risk.record_trade(m.ticker, size_usd)
                elif status == "error":
                    risk.record_failure()
                print(f"    Trade {status}: {m.ticker} x{result.get('count', 0)}")
            except Exception as e:
                risk.record_failure()
                log_error(f"Trade execution failed for {m.ticker}: {e}")
                alerts.send_error_alert(f"Trade execution failed: {m.ticker} - {e}")
        else:
            print(f"    Risk blocked {m.ticker}: {reason}")

    if risk.kill_switch:
        cancel_all_orders(api, executor, alerts)
        alerts.send_error_alert(f"Auto kill switch: {risk.kill_reason}")

    if opportunities:
        print(f"  Found {len(opportunities)} binary arb opportunities")
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
    executor = TradeExecutor(api=api, dry_run=config.DRY_RUN)
    risk = RiskManager()
    alerts = AlertManager()
    poller = TelegramPoller(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)

    print_banner()

    mode = "DRY_RUN" if config.DRY_RUN else "LIVE"
    env = "DEMO" if config.IS_DEMO else "PRODUCTION"
    alerts.send_alert(f"Bot started. Mode: {mode}. Environment: {env}")

    if args.scan_once:
        run_scan_cycle(scanner, engine, executor, risk, alerts, api)
        run_multi_arb_cycle(multi_scanner)
        print("\nSingle scan complete.")
        return

    print(f"Starting continuous scan (every {config.SCAN_INTERVAL}s)...")
    print("Press Ctrl+C to stop.\n")

    last_summary = time.monotonic()

    try:
        while True:
            try:
                handle_telegram_commands(poller, risk, api, executor, alerts)
                run_scan_cycle(scanner, engine, executor, risk, alerts, api)
                run_multi_arb_cycle(multi_scanner)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                log_error(f"main loop: {e}")
                print(f"  Error during scan: {e}")
                alerts.send_error_alert(f"Scan error: {e}")

            if time.monotonic() - last_summary >= 86400:
                alerts.send_daily_summary(risk.get_status())
                last_summary = time.monotonic()

            await asyncio.sleep(config.SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        alerts.send_alert("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())

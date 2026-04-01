import argparse
import asyncio
import csv
import os
import time
from datetime import datetime, timezone

import requests
from aiohttp import web
from config import config
from kalshi_client import KalshiAPI
from scanner import MarketScanner
from arb_engine import ArbEngine
from multi_arb import MultiArbScanner
from executor import TradeExecutor
from risk_manager import RiskManager
from alerts import AlertManager
from pnl_tracker import PnLTracker
from logger import signal_logger, multi_arb_logger, log_error, TRADE_CSV, SIGNAL_CSV

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")
DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.kalshi.co/trade-api/v2"
VALIDATE_DURATION = 30 * 60  # 30 minutes


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
                             alerts: AlertManager, pnl: PnLTracker = None):
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
        elif cmd == "/profitloss" and pnl:
            alerts.send_alert(pnl.format_total_pnl())
            print("  Telegram command: /profitloss")
        elif cmd == "/weekprofitloss" and pnl:
            alerts.send_alert(pnl.format_weekly_pnl())
            print("  Telegram command: /weekprofitloss")
        elif cmd == "/openpositions" and pnl:
            alerts.send_alert(pnl.format_open_positions(api))
            print("  Telegram command: /openpositions")


def run_scan_cycle(scanner: MarketScanner, engine: ArbEngine, executor: TradeExecutor,
                   risk: RiskManager, alerts: AlertManager, api: KalshiAPI) -> dict:
    """Run a binary arb scan cycle. Returns stats dict."""
    stats = {"markets_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{now}] Scanning markets...")

    # Reconcile any pending SL/TP OCO pairs from previous cycles
    executor.reconcile_sl_tp_orders()

    risk.check_balance(api)
    if risk.kill_switch:
        print(f"  Kill switch active: {risk.kill_reason}")
        return stats

    markets = scanner.fetch_all_open_markets()
    stats["markets_scanned"] = len(markets)
    print(f"  Fetched {len(markets)} open markets")

    if not markets:
        print("  No markets found, skipping.")
        return stats

    min_spread_cents = int(config.MIN_SPREAD * 100)
    opportunities = engine.scan_for_arbitrage(markets, min_spread_cents=min_spread_cents)
    stats["opps_found"] = len(opportunities)
    stats["spreads"] = [opp.gross_spread_cents for opp in opportunities]

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
            stats["errors"] += 1

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
                    stats["errors"] += 1
                print(f"    Trade {status}: {m.ticker} x{result.get('count', 0)}")
            except Exception as e:
                risk.record_failure()
                stats["errors"] += 1
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

    return stats


def run_multi_arb_cycle(multi_scanner: MultiArbScanner) -> dict:
    """Run a multi-outcome arb scan cycle. Returns stats dict."""
    stats = {"events_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[{now}] Scanning multi-outcome events...")

    min_spread_cents = int(config.MIN_SPREAD * 100)
    try:
        events = multi_scanner.fetch_multi_outcome_events()
    except Exception as e:
        log_error(f"multi_arb fetch error: {e}")
        stats["errors"] += 1
        print(f"  Error fetching events: {e}")
        return stats

    stats["events_scanned"] = len(events)
    print(f"  Found {len(events)} events with 3+ outcomes")

    if not events:
        print("  No multi-outcome events found, skipping.")
        return stats

    try:
        opportunities = multi_scanner.scan_for_multi_arb(events, min_spread_cents=min_spread_cents)
    except Exception as e:
        log_error(f"multi_arb scan error: {e}")
        stats["errors"] += 1
        print(f"  Error scanning events: {e}")
        return stats

    stats["opps_found"] = len(opportunities)
    stats["spreads"] = [opp.gross_spread_cents for opp in opportunities]

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

    return stats


def run_validate(scanner, engine, multi_scanner, executor, risk, alerts, api):
    """Run validation mode: 30 min dry run with stats collection."""
    config.DRY_RUN = True
    executor.dry_run = True

    print(f"\n{'=' * 45}")
    print("  VALIDATION MODE")
    print(f"  Duration: {VALIDATE_DURATION // 60} minutes")
    print(f"  DRY_RUN forced ON")
    print(f"{'=' * 45}\n")

    cycles = 0
    total_binary_stats = {"markets_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}
    total_multi_stats = {"events_scanned": 0, "opps_found": 0, "spreads": [], "errors": 0}
    start = time.monotonic()

    try:
        while time.monotonic() - start < VALIDATE_DURATION:
            cycles += 1
            print(f"\n--- Validation cycle {cycles} ---")

            try:
                binary_stats = run_scan_cycle(scanner, engine, executor, risk, alerts, api)
                total_binary_stats["markets_scanned"] += binary_stats["markets_scanned"]
                total_binary_stats["opps_found"] += binary_stats["opps_found"]
                total_binary_stats["spreads"].extend(binary_stats["spreads"])
                total_binary_stats["errors"] += binary_stats["errors"]
            except Exception as e:
                total_binary_stats["errors"] += 1
                log_error(f"validate binary scan: {e}")
                print(f"  Binary scan error: {e}")

            try:
                multi_stats = run_multi_arb_cycle(multi_scanner)
                total_multi_stats["events_scanned"] += multi_stats["events_scanned"]
                total_multi_stats["opps_found"] += multi_stats["opps_found"]
                total_multi_stats["spreads"].extend(multi_stats["spreads"])
                total_multi_stats["errors"] += multi_stats["errors"]
            except Exception as e:
                total_multi_stats["errors"] += 1
                log_error(f"validate multi scan: {e}")
                print(f"  Multi scan error: {e}")

            remaining = VALIDATE_DURATION - (time.monotonic() - start)
            if remaining > 0:
                wait = min(config.SCAN_INTERVAL, remaining)
                time.sleep(wait)

    except KeyboardInterrupt:
        print("\nValidation interrupted early.")

    elapsed = time.monotonic() - start
    report = _build_validation_report(cycles, elapsed, total_binary_stats, total_multi_stats)

    print(f"\n{report}")

    report_path = os.path.join(LOGS_DIR, "validation_report.txt")
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(report_path, "w") as f:
        f.write(report)
    print(f"\nReport saved to {report_path}")


def _build_validation_report(cycles, elapsed, binary, multi) -> str:
    def avg_spread(spreads):
        return round(sum(spreads) / len(spreads), 1) if spreads else 0.0

    def max_spread(spreads):
        return max(spreads) if spreads else 0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_errors = binary["errors"] + multi["errors"]

    lines = [
        "=" * 50,
        "  VALIDATION REPORT",
        f"  Generated: {ts}",
        "=" * 50,
        "",
        "OVERVIEW",
        f"  Duration:           {elapsed / 60:.1f} minutes",
        f"  Cycles completed:   {cycles}",
        f"  Total errors:       {total_errors}",
        "",
        "BINARY ARBITRAGE",
        f"  Markets scanned:    {binary['markets_scanned']}",
        f"  Avg markets/cycle:  {binary['markets_scanned'] / cycles:.0f}" if cycles else "  Avg markets/cycle:  0",
        f"  Opps found:         {binary['opps_found']}",
        f"  Avg spread:         {avg_spread(binary['spreads'])}c",
        f"  Max spread:         {max_spread(binary['spreads'])}c",
        f"  Errors:             {binary['errors']}",
        "",
        "MULTI-OUTCOME ARBITRAGE",
        f"  Events scanned:     {multi['events_scanned']}",
        f"  Avg events/cycle:   {multi['events_scanned'] / cycles:.0f}" if cycles else "  Avg events/cycle:   0",
        f"  Opps found:         {multi['opps_found']}",
        f"  Avg spread:         {avg_spread(multi['spreads'])}c",
        f"  Max spread:         {max_spread(multi['spreads'])}c",
        f"  Errors:             {multi['errors']}",
        "",
        "=" * 50,
    ]
    return "\n".join(lines)


# ── Dashboard server ──────────────────────────────────────────────────


class DashboardState:
    def __init__(self):
        self.start_time = time.monotonic()
        self.last_scan_time = None
        self.scan_count = 0
        self.opps_found_today = 0
        self._last_reset_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def record_scan(self, opps_found):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._last_reset_date:
            self.opps_found_today = 0
            self._last_reset_date = today
        self.last_scan_time = datetime.now(timezone.utc).isoformat()
        self.scan_count += 1
        self.opps_found_today += opps_found


class _Ctx:
    """Shared refs for dashboard route handlers."""
    state = None
    risk = None
    api = None
    pnl = None
    executor = None
    alerts = None


_ctx = _Ctx()


def _format_uptime(seconds):
    s = int(seconds)
    days, s = divmod(s, 86400)
    hours, s = divmod(s, 3600)
    minutes, s = divmod(s, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def _read_csv_tail(path, n=50):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        return list(reversed(rows[-n:]))
    except Exception:
        return []


async def _handle_dashboard(request):
    fpath = os.path.join(os.path.dirname(__file__), "dashboard.html")
    try:
        with open(fpath, "r") as f:
            html = f.read()
        return web.Response(text=html, content_type="text/html")
    except FileNotFoundError:
        return web.Response(text="dashboard.html not found", status=404)


async def _handle_status(request):
    uptime = time.monotonic() - _ctx.state.start_time if _ctx.state else 0
    return web.json_response({
        "status": "running",
        "uptime_seconds": round(uptime),
        "uptime_human": _format_uptime(uptime),
        "last_scan_time": _ctx.state.last_scan_time if _ctx.state else None,
        "scan_count": _ctx.state.scan_count if _ctx.state else 0,
        "opps_found_today": _ctx.state.opps_found_today if _ctx.state else 0,
        "dry_run": config.DRY_RUN,
        "kill_switch_active": _ctx.risk.kill_switch if _ctx.risk else False,
        "environment": "demo" if config.IS_DEMO else "production",
        "scan_interval": config.SCAN_INTERVAL,
    })


async def _handle_balance(request):
    if config.DRY_RUN or not _ctx.api:
        exposure = _ctx.risk.total_exposure if _ctx.risk else 0.0
        return web.json_response({
            "balance_usd": 0.0,
            "total_exposure_usd": round(exposure, 2),
            "available_usd": 0.0,
        })
    try:
        balance = _ctx.api.get_balance()
        exposure = _ctx.risk.total_exposure if _ctx.risk else 0.0
        return web.json_response({
            "balance_usd": round(balance, 2),
            "total_exposure_usd": round(exposure, 2),
            "available_usd": round(balance - exposure, 2),
        })
    except Exception:
        return web.json_response({
            "balance_usd": 0.0,
            "total_exposure_usd": 0.0,
            "available_usd": 0.0,
        })


async def _handle_positions(request):
    if not _ctx.api:
        return web.json_response({"positions": []})
    try:
        positions = _ctx.api.get_positions()
        result = []
        for p in positions:
            qty = int(p.get("position", 0))
            if qty == 0:
                continue
            result.append({
                "ticker": p.get("ticker", ""),
                "title": p.get("market_title", p.get("title", "")),
                "side": "yes" if qty > 0 else "no",
                "count": abs(qty),
                "avg_price_cents": int(p.get("average_price", 0)),
                "current_price_cents": int(p.get("last_price", 0)),
                "pnl_cents": int(p.get("realized_pnl", 0)),
            })
        return web.json_response({"positions": result})
    except Exception:
        return web.json_response({"positions": []})


async def _handle_trades(request):
    return web.json_response({"trades": _read_csv_tail(TRADE_CSV, 50)})


async def _handle_signals(request):
    return web.json_response({"signals": _read_csv_tail(SIGNAL_CSV, 50)})


async def _handle_daily_stats(request):
    status = _ctx.risk.get_status() if _ctx.risk else {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_trades = []
    if _ctx.pnl:
        for t in _ctx.pnl._trades:
            if t.get("timestamp", "").startswith(today):
                today_trades.append(t)

    wins = sum(1 for t in today_trades if t.get("pnl_cents", 0) > 0)
    losses = sum(1 for t in today_trades if t.get("pnl_cents", 0) < 0)
    pnls = [t.get("pnl_cents", 0) for t in today_trades]
    total = wins + losses

    return web.json_response({
        "daily_trades": status.get("daily_trade_count", 0),
        "daily_pnl_cents": int(status.get("daily_pnl", 0) * 100),
        "daily_opps_found": _ctx.state.opps_found_today if _ctx.state else 0,
        "win_count": wins,
        "loss_count": losses,
        "win_rate_percent": round(wins / total * 100, 1) if total > 0 else 0.0,
        "best_trade_cents": max(pnls) if pnls else 0,
        "worst_trade_cents": min(pnls) if pnls else 0,
    })


async def _handle_kill(request):
    if _ctx.risk:
        _ctx.risk.activate_kill_switch("dashboard")
        if _ctx.api and _ctx.executor and _ctx.alerts:
            cancel_all_orders(_ctx.api, _ctx.executor, _ctx.alerts)
    return web.json_response({"success": True, "message": "Kill switch activated via dashboard"})


async def _handle_resume(request):
    if _ctx.risk:
        _ctx.risk.deactivate_kill_switch()
    return web.json_response({"success": True, "message": "Kill switch deactivated"})


async def _handle_switch_env(request):
    if not _ctx.risk or not _ctx.risk.kill_switch:
        return web.json_response(
            {"success": False, "message": "Kill switch must be active to switch environments"},
            status=400,
        )
    if config.IS_DEMO:
        config.KALSHI_BASE_URL = PROD_BASE_URL
        config.IS_DEMO = False
        if _ctx.api:
            _ctx.api.base_url = PROD_BASE_URL
        env = "production"
    else:
        config.KALSHI_BASE_URL = DEMO_BASE_URL
        config.IS_DEMO = True
        if _ctx.api:
            _ctx.api.base_url = DEMO_BASE_URL
        env = "demo"
    return web.json_response({"success": True, "environment": env})


async def _start_dashboard_server():
    app = web.Application()
    app.router.add_get("/dashboard", _handle_dashboard)
    app.router.add_get("/api/status", _handle_status)
    app.router.add_get("/api/balance", _handle_balance)
    app.router.add_get("/api/positions", _handle_positions)
    app.router.add_get("/api/trades", _handle_trades)
    app.router.add_get("/api/signals", _handle_signals)
    app.router.add_get("/api/daily-stats", _handle_daily_stats)
    app.router.add_post("/api/kill", _handle_kill)
    app.router.add_post("/api/resume", _handle_resume)
    app.router.add_post("/api/switch-env", _handle_switch_env)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()
    print("  Dashboard:     http://127.0.0.1:8080/dashboard")
    return runner


async def main():
    parser = argparse.ArgumentParser(description="kalshi_bot - Kalshi arbitrage scanner")
    parser.add_argument("--scan-once", action="store_true",
                        help="Run a single scan cycle and exit")
    parser.add_argument("--validate", action="store_true",
                        help="Run 30-min dry run, print report, and exit")
    parser.add_argument("--demo", action="store_true",
                        help="Force Kalshi demo API regardless of .env")
    args = parser.parse_args()

    os.makedirs(LOGS_DIR, exist_ok=True)

    # --demo: override base URL before anything uses it
    if args.demo:
        config.KALSHI_BASE_URL = DEMO_BASE_URL
        config.IS_DEMO = True
        print(f"[--demo] Forcing demo API: {DEMO_BASE_URL}")

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
    pnl = PnLTracker()
    executor = TradeExecutor(api=api, dry_run=config.DRY_RUN, pnl_tracker=pnl)
    risk = RiskManager()
    alerts = AlertManager()
    poller = TelegramPoller(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)

    print_banner()

    # --validate: 30-min dry run with report
    if args.validate:
        run_validate(scanner, engine, multi_scanner, executor, risk, alerts, api)
        return

    mode = "DRY_RUN" if config.DRY_RUN else "LIVE"
    env = "DEMO" if config.IS_DEMO else "PRODUCTION"
    alerts.send_alert(f"Bot started. Mode: {mode}. Environment: {env}")

    # --scan-once: single cycle and exit
    if args.scan_once:
        run_scan_cycle(scanner, engine, executor, risk, alerts, api)
        run_multi_arb_cycle(multi_scanner)
        print("\nSingle scan complete.")
        return

    # Start dashboard server
    _ctx.state = DashboardState()
    _ctx.risk = risk
    _ctx.api = api
    _ctx.pnl = pnl
    _ctx.executor = executor
    _ctx.alerts = alerts
    dash_runner = await _start_dashboard_server()

    print(f"Starting continuous scan (every {config.SCAN_INTERVAL}s)...")
    print("Press Ctrl+C to stop.\n")

    last_summary = time.monotonic()
    last_expiry_check = time.monotonic()
    EXPIRY_CHECK_INTERVAL = 8 * 3600  # 8 hours

    try:
        while True:
            try:
                handle_telegram_commands(poller, risk, api, executor, alerts, pnl)
                b_stats = run_scan_cycle(scanner, engine, executor, risk, alerts, api)
                m_stats = run_multi_arb_cycle(multi_scanner)

                if _ctx.state:
                    _ctx.state.record_scan(
                        b_stats.get("opps_found", 0) + m_stats.get("opps_found", 0))

                # Check for positions nearing market resolution (every 8 hours)
                if time.monotonic() - last_expiry_check >= EXPIRY_CHECK_INTERVAL:
                    executor.check_expiring_positions()
                    last_expiry_check = time.monotonic()

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
        await dash_runner.cleanup()
        alerts.send_alert("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())

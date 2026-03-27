from datetime import datetime, timezone, timedelta
import pytest
from scanner import KalshiMarket
from arb_engine import ArbEngine, FEE_PER_CONTRACT_CENTS


def _future_close_time(days=30):
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_market(ticker="MKT-A", yes=45, no=45, **kwargs):
    defaults = dict(
        event_ticker="EVT-A", title="Test market",
        volume=100, status="open", close_time=_future_close_time(),
    )
    defaults.update(kwargs)
    return KalshiMarket(
        ticker=ticker, yes_price_cents=yes, no_price_cents=no, **defaults,
    )


@pytest.fixture
def engine():
    return ArbEngine()


# ── 1. Profitable: YES=45, NO=52 -> total=97, gross=3, fee=2, net=1 ──

class TestProfitable:
    def test_is_profitable(self, engine):
        assert engine.is_profitable(45, 52) is True

    def test_scan_finds_opportunity(self, engine):
        markets = [_make_market(yes=45, no=52)]
        opps = engine.scan_for_arbitrage(markets, min_spread_cents=0)
        assert len(opps) == 1
        opp = opps[0]
        assert opp.total_cost_cents == 97
        assert opp.gross_spread_cents == 3
        assert opp.net_profit_cents == 1


# ── 2. Breakeven: YES=49, NO=49 -> total=98, gross=2, fee=2, net=0 ──

class TestBreakeven:
    def test_not_profitable(self, engine):
        assert engine.is_profitable(49, 49) is False

    def test_scan_excludes_breakeven(self, engine):
        markets = [_make_market(yes=49, no=49)]
        opps = engine.scan_for_arbitrage(markets, min_spread_cents=0)
        assert len(opps) == 0


# ── 3. Unprofitable: YES=50, NO=50 -> total=100, gross=0, fee=2, net=-2 ──

class TestUnprofitable:
    def test_not_profitable(self, engine):
        assert engine.is_profitable(50, 50) is False

    def test_scan_excludes_unprofitable(self, engine):
        markets = [_make_market(yes=50, no=50)]
        opps = engine.scan_for_arbitrage(markets, min_spread_cents=0)
        assert len(opps) == 0


# ── 4. Wide spread: YES=30, NO=30 -> total=60, gross=40, fee=2, net=38 ──

class TestWideSpread:
    def test_is_profitable(self, engine):
        assert engine.is_profitable(30, 30) is True

    def test_scan_calculates_correct_profit(self, engine):
        markets = [_make_market(yes=30, no=30)]
        opps = engine.scan_for_arbitrage(markets)
        assert len(opps) == 1
        opp = opps[0]
        assert opp.total_cost_cents == 60
        assert opp.gross_spread_cents == 40
        assert opp.net_profit_cents == 38
        assert opp.roi_percent == round((38 / 60) * 100, 2)


# ── 5. Sorting: 3 markets sorted by ROI descending ──

class TestSorting:
    def test_sorted_by_roi_descending(self, engine):
        markets = [
            _make_market(ticker="LOW", yes=45, no=45),    # total=90, net=8,  roi=8.89%
            _make_market(ticker="HIGH", yes=20, no=20),   # total=40, net=58, roi=145%
            _make_market(ticker="MID", yes=35, no=35),    # total=70, net=28, roi=40%
        ]
        opps = engine.scan_for_arbitrage(markets)
        assert len(opps) == 3
        assert opps[0].market.ticker == "HIGH"
        assert opps[1].market.ticker == "MID"
        assert opps[2].market.ticker == "LOW"
        rois = [o.roi_percent for o in opps]
        assert rois == sorted(rois, reverse=True)


# ── 6. Fee calculation: always 2 cents ──

class TestFeeCalculation:
    def test_fee_constant_is_2(self):
        assert FEE_PER_CONTRACT_CENTS == 2

    def test_fee_applied_in_opportunity(self, engine):
        markets = [_make_market(yes=40, no=40)]
        opp = engine.scan_for_arbitrage(markets)[0]
        assert opp.fee_cents == 2
        assert opp.net_profit_cents == opp.gross_spread_cents - 2


# ── 7. Realistic market data ──

class TestRealMarketData:
    def test_realistic_market_scan(self, engine):
        """Scan across markets resembling real Kalshi data."""
        markets = [
            _make_market(ticker="PRES-24-DEM-NY", event_ticker="PRES-24",
                         title="Democratic candidate wins New York?",
                         yes=42, no=53, volume=1250),
            _make_market(ticker="FED-RATE-25MAR", event_ticker="FED-RATE",
                         title="Fed raises rates in March?",
                         yes=67, no=33, volume=8400),
            _make_market(ticker="WEATHER-NYC-80F", event_ticker="WEATHER-NYC",
                         title="NYC reaches 80F on March 25?",
                         yes=12, no=85, volume=320),
            _make_market(ticker="BTC-100K", event_ticker="BTC-PRICE",
                         title="Bitcoin exceeds 100K on March 25?",
                         yes=55, no=45, volume=15600),
            _make_market(ticker="NBA-LAL-WIN", event_ticker="NBA-LAL",
                         title="Lakers win tonight?",
                         yes=35, no=58, volume=2100),
        ]
        opps = engine.scan_for_arbitrage(markets, min_spread_cents=0)

        # 3 arb markets: NBA (93), PRES (95), WEATHER (97)
        # FED (100) and BTC (100) have no spread
        assert len(opps) == 3

        tickers = [o.market.ticker for o in opps]
        assert tickers[0] == "NBA-LAL-WIN"       # ROI: 5/93 = 5.38%
        assert tickers[1] == "PRES-24-DEM-NY"    # ROI: 3/95 = 3.16%
        assert tickers[2] == "WEATHER-NYC-80F"    # ROI: 1/97 = 1.03%

        best = opps[0]
        assert best.total_cost_cents == 93
        assert best.gross_spread_cents == 7
        assert best.net_profit_cents == 5
        assert best.roi_percent == round((5 / 93) * 100, 2)

    def test_min_spread_filters_narrow_arbs(self, engine):
        """With min_spread=3, total must be < 97 to pass."""
        markets = [
            _make_market(ticker="NARROW", yes=45, no=52),  # total=97, filtered
            _make_market(ticker="WIDE", yes=44, no=51),    # total=95, passes
        ]
        opps = engine.scan_for_arbitrage(markets, min_spread_cents=3)
        assert len(opps) == 1
        assert opps[0].market.ticker == "WIDE"

import pytest
from unittest.mock import MagicMock
from multi_arb import MultiArbScanner, MultiOutcomeOpportunity, FEE_PER_CONTRACT_CENTS


def _make_event(event_ticker="EVT-1", title="Who wins?", markets=None):
    if markets is None:
        markets = [
            {"ticker": "M-A", "title": "A wins", "yes_bid": 20, "volume": 50, "status": "open"},
            {"ticker": "M-B", "title": "B wins", "yes_bid": 20, "volume": 50, "status": "open"},
            {"ticker": "M-C", "title": "C wins", "yes_bid": 20, "volume": 50, "status": "open"},
        ]
    return {"event_ticker": event_ticker, "title": title, "markets": markets}


@pytest.fixture
def mock_api():
    return MagicMock()


@pytest.fixture
def scanner(mock_api):
    return MultiArbScanner(mock_api)


# ── 1. Profitable event: 3 markets at 20¢ each = 60¢ total ──

class TestProfitable:
    def test_detects_multi_arb(self, scanner):
        events = [_make_event()]  # 20+20+20=60, gross=40, fee=6, net=34
        opps = scanner.scan_for_multi_arb(events)
        assert len(opps) == 1
        opp = opps[0]
        assert opp.total_yes_cost_cents == 60
        assert opp.gross_spread_cents == 40
        assert opp.fee_cents == 6  # 3 markets * 2¢
        assert opp.net_profit_cents == 34
        assert opp.num_markets == 3

    def test_roi_calculation(self, scanner):
        events = [_make_event()]
        opp = scanner.scan_for_multi_arb(events)[0]
        expected_roi = round((34 / 60) * 100, 2)
        assert opp.roi_percent == expected_roi

    def test_markets_list_populated(self, scanner):
        events = [_make_event()]
        opp = scanner.scan_for_multi_arb(events)[0]
        assert len(opp.markets) == 3
        assert opp.markets[0]["ticker"] == "M-A"
        assert opp.markets[0]["yes_price_cents"] == 20


# ── 2. Unprofitable event: 3 markets at 34¢ each = 102¢ ──

class TestUnprofitable:
    def test_no_arb_when_total_exceeds_100(self, scanner):
        markets = [
            {"ticker": "M-A", "title": "A", "yes_bid": 34, "volume": 50, "status": "open"},
            {"ticker": "M-B", "title": "B", "yes_bid": 34, "volume": 50, "status": "open"},
            {"ticker": "M-C", "title": "C", "yes_bid": 34, "volume": 50, "status": "open"},
        ]
        events = [_make_event(markets=markets)]  # 102¢ total
        opps = scanner.scan_for_multi_arb(events)
        assert len(opps) == 0

    def test_no_arb_at_exact_100(self, scanner):
        markets = [
            {"ticker": "M-A", "title": "A", "yes_bid": 33, "volume": 50, "status": "open"},
            {"ticker": "M-B", "title": "B", "yes_bid": 33, "volume": 50, "status": "open"},
            {"ticker": "M-C", "title": "C", "yes_bid": 34, "volume": 50, "status": "open"},
        ]
        events = [_make_event(markets=markets)]  # 100¢ total
        opps = scanner.scan_for_multi_arb(events)
        assert len(opps) == 0


# ── 3. Breakeven: fees eat the spread ──

class TestBreakeven:
    def test_fee_eats_spread_4_markets(self, scanner):
        # 4 markets at 24¢ = 96¢, gross=4, fee=4*2=8, net=-4 -> excluded
        markets = [
            {"ticker": f"M-{i}", "title": f"O{i}", "yes_bid": 24, "volume": 10, "status": "open"}
            for i in range(4)
        ]
        events = [_make_event(markets=markets)]
        opps = scanner.scan_for_multi_arb(events, min_spread_cents=0)
        assert len(opps) == 0

    def test_fee_eats_spread_3_markets(self, scanner):
        # 3 markets at 32¢ = 96¢, gross=4, fee=3*2=6, net=-2 -> excluded
        markets = [
            {"ticker": f"M-{i}", "title": f"O{i}", "yes_bid": 32, "volume": 10, "status": "open"}
            for i in range(3)
        ]
        events = [_make_event(markets=markets)]
        opps = scanner.scan_for_multi_arb(events, min_spread_cents=0)
        assert len(opps) == 0


# ── 4. Sorting by ROI descending ──

class TestSorting:
    def test_sorted_by_roi(self, scanner):
        evt_low = _make_event(event_ticker="LOW", markets=[
            {"ticker": "L-A", "title": "A", "yes_bid": 30, "volume": 10, "status": "open"},
            {"ticker": "L-B", "title": "B", "yes_bid": 30, "volume": 10, "status": "open"},
            {"ticker": "L-C", "title": "C", "yes_bid": 30, "volume": 10, "status": "open"},
        ])  # total=90, gross=10, fee=6, net=4, roi=4.44%

        evt_high = _make_event(event_ticker="HIGH", markets=[
            {"ticker": "H-A", "title": "A", "yes_bid": 10, "volume": 10, "status": "open"},
            {"ticker": "H-B", "title": "B", "yes_bid": 10, "volume": 10, "status": "open"},
            {"ticker": "H-C", "title": "C", "yes_bid": 10, "volume": 10, "status": "open"},
        ])  # total=30, gross=70, fee=6, net=64, roi=213.33%

        opps = scanner.scan_for_multi_arb([evt_low, evt_high])
        assert len(opps) == 2
        assert opps[0].event_ticker == "HIGH"
        assert opps[1].event_ticker == "LOW"
        assert opps[0].roi_percent > opps[1].roi_percent


# ── 5. Volume filter: skip markets with 0 volume ──

class TestVolumeFilter:
    def test_skips_zero_volume(self, scanner):
        markets = [
            {"ticker": "M-A", "title": "A", "yes_bid": 20, "volume": 50, "status": "open"},
            {"ticker": "M-B", "title": "B", "yes_bid": 20, "volume": 0, "status": "open"},
            {"ticker": "M-C", "title": "C", "yes_bid": 20, "volume": 50, "status": "open"},
        ]
        events = [_make_event(markets=markets)]
        opps = scanner.scan_for_multi_arb(events)
        assert len(opps) == 0

    def test_min_volume_tracked(self, scanner):
        markets = [
            {"ticker": "M-A", "title": "A", "yes_bid": 20, "volume": 100, "status": "open"},
            {"ticker": "M-B", "title": "B", "yes_bid": 20, "volume": 5, "status": "open"},
            {"ticker": "M-C", "title": "C", "yes_bid": 20, "volume": 50, "status": "open"},
        ]
        events = [_make_event(markets=markets)]
        opp = scanner.scan_for_multi_arb(events)[0]
        assert opp.min_volume == 5


# ── 6. Fee calculation: 2¢ per contract ──

class TestFeeCalculation:
    def test_fee_scales_with_num_markets(self, scanner):
        for n in [3, 4, 5]:
            markets = [
                {"ticker": f"M-{i}", "title": f"O{i}", "yes_bid": 10, "volume": 10, "status": "open"}
                for i in range(n)
            ]
            events = [_make_event(markets=markets)]
            opp = scanner.scan_for_multi_arb(events)[0]
            assert opp.fee_cents == n * FEE_PER_CONTRACT_CENTS


# ── 7. format_opportunity ──

class TestFormat:
    def test_contains_key_info(self, scanner):
        events = [_make_event()]
        opp = scanner.scan_for_multi_arb(events)[0]
        text = scanner.format_opportunity(opp)
        assert "MULTI-ARB" in text
        assert "EVT-1" in text
        assert "Who wins?" in text
        assert "M-A" in text
        assert "34" in text  # net profit
        assert "40" in text  # gross spread


# ── 8. Live API fetch (mocked) ──

class TestFetchMultiOutcomeEvents:
    def test_fetches_and_filters(self, scanner, mock_api):
        responses = {
            "/events": {
                "events": [
                    {"event_ticker": "EVT-3WAY", "title": "3-way race"},
                    {"event_ticker": "EVT-BINARY", "title": "Yes/No"},
                ],
                "cursor": None,
            },
            "/events/EVT-3WAY": {
                "markets": [
                    {"ticker": "A", "title": "A", "yes_bid": 30, "volume": 10, "status": "open"},
                    {"ticker": "B", "title": "B", "yes_bid": 30, "volume": 10, "status": "open"},
                    {"ticker": "C", "title": "C", "yes_bid": 30, "volume": 10, "status": "open"},
                ],
            },
            "/events/EVT-BINARY": {
                "markets": [
                    {"ticker": "Y", "title": "Yes", "yes_bid": 60, "volume": 10, "status": "open"},
                    {"ticker": "N", "title": "No", "yes_bid": 40, "volume": 10, "status": "open"},
                ],
            },
        }
        mock_api.get_public.side_effect = lambda path, **kwargs: responses.get(path, {})
        events = scanner.fetch_multi_outcome_events()
        assert len(events) == 1  # only 3+ markets
        assert events[0]["event_ticker"] == "EVT-3WAY"
        assert len(events[0]["markets"]) == 3

    def test_api_error_returns_empty(self, scanner, mock_api):
        mock_api.get_public.side_effect = Exception("timeout")
        events = scanner.fetch_multi_outcome_events()
        assert events == []

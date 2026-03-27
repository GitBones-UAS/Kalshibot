import time
from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import MagicMock, patch
from scanner import MarketScanner, KalshiMarket


def _default_close_time():
    """30 days from now — always within the 200-day test cutoff."""
    return (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_raw_market(ticker="MKT-A", event_ticker="EVT-A", title="Will it rain?",
                     yes_bid=65, volume=100, status="open", close_time=None):
    if close_time is None:
        close_time = _default_close_time()
    return {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "title": title,
        "yes_bid": yes_bid,
        "volume": volume,
        "status": status,
        "close_time": close_time,
    }


@pytest.fixture
def mock_api():
    return MagicMock()


@pytest.fixture(autouse=True)
def _no_expiry_filter():
    """Disable expiry filtering by default so existing tests aren't affected."""
    with patch("scanner.config") as cfg, \
         patch("scanner.signal_logger"):
        cfg.MAX_DAYS_TO_EXPIRY = 200
        yield cfg


@pytest.fixture
def scanner(mock_api):
    return MarketScanner(mock_api)


# ── 1. fetch_open_markets returns non-empty list of KalshiMarket objects ──

class TestFetchOpenMarketsBasic:
    def test_returns_list_of_kalshi_market(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(), _make_raw_market(ticker="MKT-B")],
            "cursor": None,
        }
        markets, _ = scanner.fetch_open_markets()
        assert len(markets) > 0
        assert all(isinstance(m, KalshiMarket) for m in markets)

    def test_returns_correct_count(self, scanner, mock_api):
        raw = [_make_raw_market(ticker=f"MKT-{i}") for i in range(5)]
        mock_api.get_public.return_value = {"markets": raw, "cursor": None}
        markets, _ = scanner.fetch_open_markets()
        assert len(markets) == 5


# ── 2. Each market has ticker, title, yes_price_cents between 1-99 ──

class TestMarketFields:
    @pytest.mark.parametrize("yes_bid", [1, 25, 50, 75, 99])
    def test_yes_price_in_range(self, scanner, mock_api, yes_bid):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(yes_bid=yes_bid)],
            "cursor": None,
        }
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert 1 <= m.yes_price_cents <= 99

    def test_ticker_and_title_populated(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="ABC-123", title="Some title")],
            "cursor": None,
        }
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert m.ticker == "ABC-123"
        assert m.title == "Some title"

    def test_all_fields_present(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market()],
            "cursor": None,
        }
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert m.ticker != ""
        assert m.event_ticker != ""
        assert m.title != ""
        assert isinstance(m.yes_price_cents, int)
        assert isinstance(m.no_price_cents, int)
        assert isinstance(m.volume, int)
        assert m.status == "open"
        assert m.close_time != ""


# ── 3. no_price_cents = 100 - yes_price_cents ──

class TestNoPriceComplement:
    @pytest.mark.parametrize("yes_bid", [1, 10, 33, 50, 67, 90, 99])
    def test_no_price_is_complement(self, scanner, mock_api, yes_bid):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(yes_bid=yes_bid)],
            "cursor": None,
        }
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert m.no_price_cents == 100 - m.yes_price_cents
        assert m.yes_price_cents + m.no_price_cents == 100

    def test_complement_with_last_price_fallback(self, scanner, mock_api):
        """When yes_bid is missing, _parse_market falls back to last_price."""
        raw = {
            "ticker": "FB-1", "event_ticker": "EVT-1", "title": "Fallback test",
            "last_price": 40, "volume": 10, "status": "open", "close_time": _default_close_time(),
        }
        mock_api.get_public.return_value = {"markets": [raw], "cursor": None}
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert m.yes_price_cents == 40
        assert m.no_price_cents == 60

    def test_complement_default_when_no_price(self, scanner, mock_api):
        """When both yes_bid and last_price are missing, defaults to 50."""
        raw = {
            "ticker": "DEF-1", "event_ticker": "EVT-1", "title": "Default test",
            "volume": 0, "status": "open", "close_time": _default_close_time(),
        }
        mock_api.get_public.return_value = {"markets": [raw], "cursor": None}
        markets, _ = scanner.fetch_open_markets()
        m = markets[0]
        assert m.yes_price_cents == 50
        assert m.no_price_cents == 50


# ── 4. Error handling with mocked failed response ──

class TestErrorHandling:
    def test_exception_returns_empty_list_and_none_cursor(self, scanner, mock_api):
        mock_api.get_public.side_effect = Exception("Connection timeout")
        markets, cursor = scanner.fetch_open_markets()
        assert markets == []
        assert cursor is None

    def test_http_error_returns_empty(self, scanner, mock_api):
        mock_api.get_public.side_effect = ConnectionError("Server unreachable")
        markets, cursor = scanner.fetch_open_markets()
        assert markets == []
        assert cursor is None

    def test_empty_response_returns_empty_list(self, scanner, mock_api):
        mock_api.get_public.return_value = {}
        markets, cursor = scanner.fetch_open_markets()
        assert markets == []
        assert cursor is None

    @patch("scanner.log_error")
    def test_error_is_logged(self, mock_log_error, scanner, mock_api):
        mock_api.get_public.side_effect = Exception("API failure")
        scanner.fetch_open_markets()
        mock_log_error.assert_called_once()
        assert "API failure" in mock_log_error.call_args[0][0]


# ── 5. Pagination: fetch with small limit, verify cursor returned ──

class TestPagination:
    def test_cursor_returned_when_present(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market()],
            "cursor": "abc123next",
        }
        markets, cursor = scanner.fetch_open_markets(limit=1)
        assert cursor == "abc123next"
        assert len(markets) == 1

    def test_no_cursor_on_last_page(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market()],
            "cursor": None,
        }
        _, cursor = scanner.fetch_open_markets()
        assert cursor is None

    def test_cursor_passed_to_api(self, scanner, mock_api):
        mock_api.get_public.return_value = {"markets": [], "cursor": None}
        scanner.fetch_open_markets(limit=5, cursor="page2cursor")
        call_args = mock_api.get_public.call_args
        params = call_args[1].get("params") or call_args[0][1]
        assert params["cursor"] == "page2cursor"
        assert params["limit"] == 5

    def test_limit_parameter_forwarded(self, scanner, mock_api):
        mock_api.get_public.return_value = {"markets": [], "cursor": None}
        scanner.fetch_open_markets(limit=10)
        call_args = mock_api.get_public.call_args
        params = call_args[1].get("params") or call_args[0][1]
        assert params["limit"] == 10

    def test_paginated_fetch_multiple_pages(self, scanner, mock_api):
        """Simulate two pages via fetch_all_open_markets."""
        page1 = {"markets": [_make_raw_market(ticker="P1")], "cursor": "next"}
        page2 = {"markets": [_make_raw_market(ticker="P2")], "cursor": None}
        mock_api.get_public.side_effect = [page1, page2]

        all_markets = scanner.fetch_all_open_markets()
        assert len(all_markets) == 2
        assert all_markets[0].ticker == "P1"
        assert all_markets[1].ticker == "P2"
        assert mock_api.get_public.call_count == 2


# ── 6. Multi-page fetch with cursor chaining ──

class TestMultiPageFetch:
    def test_three_page_pagination(self, scanner, mock_api):
        """Verify fetch_all_open_markets chains cursors across 3 pages."""
        page1 = {"markets": [_make_raw_market(ticker="P1-A"),
                              _make_raw_market(ticker="P1-B")], "cursor": "cur1"}
        page2 = {"markets": [_make_raw_market(ticker="P2-A")], "cursor": "cur2"}
        page3 = {"markets": [_make_raw_market(ticker="P3-A"),
                              _make_raw_market(ticker="P3-B")], "cursor": None}
        mock_api.get_public.side_effect = [page1, page2, page3]

        all_markets = scanner.fetch_all_open_markets()

        assert len(all_markets) == 5
        assert [m.ticker for m in all_markets] == ["P1-A", "P1-B", "P2-A", "P3-A", "P3-B"]
        assert mock_api.get_public.call_count == 3

    def test_stops_on_empty_page(self, scanner, mock_api):
        """Pagination stops when a page returns no markets."""
        page1 = {"markets": [_make_raw_market(ticker="M1")], "cursor": "cur1"}
        page2 = {"markets": [], "cursor": "cur2"}
        mock_api.get_public.side_effect = [page1, page2]

        all_markets = scanner.fetch_all_open_markets()
        assert len(all_markets) == 1
        assert mock_api.get_public.call_count == 2

    def test_single_page_no_cursor(self, scanner, mock_api):
        """Single page with no cursor returns all markets."""
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker=f"M{i}") for i in range(3)],
            "cursor": None,
        }
        all_markets = scanner.fetch_all_open_markets()
        assert len(all_markets) == 3
        assert mock_api.get_public.call_count == 1


# ── 7. Market data caching ──

class TestMarketCache:
    def test_cache_returns_same_data_without_api_call(self, mock_api):
        scanner = MarketScanner(mock_api, cache_ttl=30.0)
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="CACHED")],
            "cursor": None,
        }
        first = scanner.fetch_all_open_markets()
        second = scanner.fetch_all_open_markets()
        assert first == second
        assert mock_api.get_public.call_count == 1

    def test_cache_expires_after_ttl(self, mock_api):
        scanner = MarketScanner(mock_api, cache_ttl=0.0)
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="M1")],
            "cursor": None,
        }
        scanner.fetch_all_open_markets()
        scanner.fetch_all_open_markets()
        assert mock_api.get_public.call_count == 2

    def test_invalidate_cache_forces_refetch(self, mock_api):
        scanner = MarketScanner(mock_api, cache_ttl=60.0)
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="OLD")],
            "cursor": None,
        }
        scanner.fetch_all_open_markets()
        scanner.invalidate_cache()

        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="NEW")],
            "cursor": None,
        }
        result = scanner.fetch_all_open_markets()
        assert result[0].ticker == "NEW"
        assert mock_api.get_public.call_count == 2

    def test_cache_default_ttl(self, mock_api):
        scanner = MarketScanner(mock_api)
        assert scanner.cache_ttl == 30.0
        assert scanner._market_cache == []
        assert scanner._cache_time == 0.0


# ── 8. Orderbook depth validation ──

class TestOrderbookValidation:
    def test_sufficient_depth_returns_true(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "orderbook": {"yes": [[40, 5], [45, 3]], "no": []}
        }
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 45, min_contracts=5) is True

    def test_insufficient_depth_returns_false(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "orderbook": {"yes": [[40, 1]], "no": []}
        }
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 45, min_contracts=5) is False

    def test_empty_orderbook_fails_open(self, scanner, mock_api):
        mock_api.get_public.return_value = {}
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 50, min_contracts=1) is True

    def test_api_error_fails_open(self, scanner, mock_api):
        mock_api.get_public.side_effect = Exception("timeout")
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 50, min_contracts=1) is True

    def test_no_side_validation(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "orderbook": {"yes": [], "no": [[55, 10], [60, 5]]}
        }
        assert scanner.validate_orderbook_depth("MKT-A", "no", 60, min_contracts=10) is True

    def test_only_counts_levels_at_or_below_price(self, scanner, mock_api):
        mock_api.get_public.return_value = {
            "orderbook": {"yes": [[30, 2], [40, 3], [50, 10]], "no": []}
        }
        # Only levels <= 40: [30,2] + [40,3] = 5
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 40, min_contracts=5) is True
        assert scanner.validate_orderbook_depth("MKT-A", "yes", 40, min_contracts=6) is False

    def test_empty_ticker_fails_open(self, scanner, mock_api):
        assert scanner.validate_orderbook_depth("", "yes", 50, min_contracts=1) is True


# ── 9. Expiry filter ──

def _close_time_in(days):
    """Generate a close_time N days from now."""
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestExpiryFilter:
    def test_filters_out_far_future_markets(self, mock_api, _no_expiry_filter):
        _no_expiry_filter.MAX_DAYS_TO_EXPIRY = 10
        scanner = MarketScanner(mock_api)
        mock_api.get_public.return_value = {
            "markets": [
                _make_raw_market(ticker="SOON", close_time=_close_time_in(5)),
                _make_raw_market(ticker="FAR", close_time=_close_time_in(365)),
            ],
            "cursor": None,
        }
        markets = scanner.fetch_all_open_markets()
        tickers = [m.ticker for m in markets]
        assert "SOON" in tickers
        assert "FAR" not in tickers

    def test_logs_too_far_out_to_signal_csv(self, mock_api, _no_expiry_filter):
        _no_expiry_filter.MAX_DAYS_TO_EXPIRY = 10
        scanner = MarketScanner(mock_api)
        mock_api.get_public.return_value = {
            "markets": [
                _make_raw_market(ticker="FAR", close_time=_close_time_in(365)),
            ],
            "cursor": None,
        }
        with patch("scanner.signal_logger") as mock_logger:
            scanner.fetch_all_open_markets()
            mock_logger.log.assert_called_once()
            call_kwargs = mock_logger.log.call_args
            assert call_kwargs[1]["action"] == "TOO_FAR_OUT"
            assert call_kwargs[1]["market_ticker"] == "FAR"

    def test_empty_close_time_passes_filter(self, mock_api, _no_expiry_filter):
        _no_expiry_filter.MAX_DAYS_TO_EXPIRY = 10
        scanner = MarketScanner(mock_api)
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="NO-TIME", close_time="")],
            "cursor": None,
        }
        markets = scanner.fetch_all_open_markets()
        assert len(markets) == 1
        assert markets[0].ticker == "NO-TIME"

    def test_invalid_close_time_passes_filter(self, mock_api, _no_expiry_filter):
        _no_expiry_filter.MAX_DAYS_TO_EXPIRY = 10
        scanner = MarketScanner(mock_api)
        mock_api.get_public.return_value = {
            "markets": [_make_raw_market(ticker="BAD-TIME", close_time="not-a-date")],
            "cursor": None,
        }
        markets = scanner.fetch_all_open_markets()
        assert len(markets) == 1

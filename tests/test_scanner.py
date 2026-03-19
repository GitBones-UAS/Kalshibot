import pytest
from unittest.mock import MagicMock, patch
from scanner import MarketScanner, KalshiMarket


def _make_raw_market(ticker="MKT-A", event_ticker="EVT-A", title="Will it rain?",
                     yes_bid=65, volume=100, status="open", close_time="2026-12-31T00:00:00Z"):
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
            "last_price": 40, "volume": 10, "status": "open", "close_time": "2026-12-31T00:00:00Z",
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
            "volume": 0, "status": "open", "close_time": "2026-12-31T00:00:00Z",
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

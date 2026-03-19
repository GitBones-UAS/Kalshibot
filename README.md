# kalshi_bot

Automated arbitrage detection and execution bot for the Kalshi prediction market.

## Features

- **Scanner** - Continuously scans Kalshi markets for arbitrage opportunities
- **Arb Engine** - Detects mispriced Yes/No contract pairs
- **Multi-Arb** - Finds multi-contract arbitrage across related markets
- **Executor** - Places trades via the Kalshi API
- **Risk Manager** - Enforces position limits, daily loss limits, and trade caps
- **Alerts** - Sends trade notifications via Telegram

## Setup

1. Clone the repo
2. Create a virtual environment: `python -m venv venv`
3. Install dependencies: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and fill in your credentials
5. Run: `python main.py`

## Testing

```
pytest tests/
```

## Configuration

See `.env.example` for all available settings. The bot runs in `DRY_RUN=true` mode by default (no real trades).

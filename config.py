import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        self.KALSHI_API_KEY_ID: str = os.getenv("KALSHI_API_KEY_ID", "")
        self.KALSHI_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
        self.KALSHI_BASE_URL: str = os.getenv("KALSHI_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")
        self.TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
        self.DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() == "true"
        self.SCAN_INTERVAL: int = int(os.getenv("SCAN_INTERVAL_SECONDS", "120"))
        self.MIN_SPREAD: float = float(os.getenv("MIN_SPREAD_THRESHOLD", "0.03"))
        self.MAX_POSITION_SIZE: float = float(os.getenv("MAX_POSITION_SIZE_USD", "2.0"))
        self.MAX_DAILY_TRADES: int = int(os.getenv("MAX_DAILY_TRADES", "10"))
        self.MAX_DAILY_LOSS: float = float(os.getenv("MAX_DAILY_LOSS_USD", "10.0"))
        self.MAX_DAYS_TO_EXPIRY: int = int(os.getenv("MAX_DAYS_TO_EXPIRY", "45"))
        self.IS_DEMO: bool = "demo" in self.KALSHI_BASE_URL.lower()

    def validate(self):
        if not self.KALSHI_API_KEY_ID:
            raise ValueError("KALSHI_API_KEY_ID is required")
        if not self.KALSHI_PRIVATE_KEY_PATH:
            raise ValueError("KALSHI_PRIVATE_KEY_PATH is required")
        pem_path = Path(self.KALSHI_PRIVATE_KEY_PATH)
        if not pem_path.exists():
            raise FileNotFoundError(f"Private key not found: {pem_path}")

    def print_summary(self):
        mode = "DEMO" if self.IS_DEMO else "PRODUCTION"
        dry = "ON" if self.DRY_RUN else "OFF"
        telegram = "configured" if self.TELEGRAM_TOKEN else "not configured"
        print(f"=== kalshi_bot config ===")
        print(f"  Mode:             {mode}")
        print(f"  Dry run:          {dry}")
        print(f"  Base URL:         {self.KALSHI_BASE_URL}")
        print(f"  Scan interval:    {self.SCAN_INTERVAL}s")
        print(f"  Min spread:       {self.MIN_SPREAD}")
        print(f"  Max position:     ${self.MAX_POSITION_SIZE}")
        print(f"  Max daily trades: {self.MAX_DAILY_TRADES}")
        print(f"  Max daily loss:   ${self.MAX_DAILY_LOSS}")
        print(f"  Telegram:         {telegram}")
        print(f"=========================")


config = Config()

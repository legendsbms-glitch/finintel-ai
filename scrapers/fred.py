"""
FRED API Scraper
==============
Fetches macro data: WEI, Fed balance sheet, M2, interest rates
API: https://fred.stlouisfed.org/docs/api/fred/
"""
import os
import json
import logging
from typing import List, Dict, Any
import requests
from .base import BaseScraper

logger = logging.getLogger(__name__)


class FredScraper(BaseScraper):
    """Scrapes Federal Reserve Economic Data (FRED)."""

    # Core macro indicators to track
    INDICATORS = {
        # Weekly Economic Index
        "WEI": "williot",
        
        # Fed Balance Sheet
        "FED_BALANCE_TOTAL": "BALANCE",
        "CASH_ASSETS": "WALDB",
        "SECURITIES": "WFSLBO",
        
        # Money Supply
        "M2": "M2SL",
        "M2V": "M2V",
        
        # Interest Rates
        "FED_FUNDS_RATE": "DFEDTARU",
        "10Y_YIELD": "DGS10",
        
        # Inflation
        "CPI": "CPILFESL",
        "PCE": "PCEPI",
        
        # Employment
        "UNRATE": "UNRATE",
        "PAYEMS": "PAYEMS",
        
        # GDP
        "GDPC1": "GDPC1",
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = os.environ.get("FRED_API_KEY", "")

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch latest values for configured indicators."""
        if not self.api_key:
            logger.warning("FRED_API_KEY not set, skipping")
            return []

        items = []
        base_url = "https://api.stlouisfed.org/fred/series/observations"

        for name, series_id in self.INDICATORS.items():
            url = f"{base_url}/{series_id}"
            params = {
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": "2024-01-01",  # Last year of data
                "limit": 1  # Just latest value
            }

            response = self._fetch(url, params)
            if not response:
                continue

            try:
                data = response.json()
                if "observations" in data and data["observations"]:
                    obs = data["observations"][-1]
                    items.append({
                        "url": url,
                        "raw_content": json.dumps(obs),
                        "ticker": name,
                        "data_type": "macro",
                        "value": obs.get("value"),
                        "date": obs.get("date"),
                        "series_id": series_id
                    })
                    logger.info(f"FRED {name}: {obs.get('value')} ({obs.get('date')})")

            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing FRED {name}: {e}")

        return items

    def compute_changes(self, items: List[Dict[str, Any]]) -> Dict[str, float]:
        """Compute period-over-period changes for key indicators."""
        changes = {}

        for item in items:
            if item.get("data_type") != "macro":
                continue

            try:
                value = float(item.get("value", 0))
                name = item.get("ticker", "")

                # Store for later comparison (would need DB for actual delta)
                changes[name] = value

            except (ValueError, TypeError):
                continue

        return changes


# Allow running directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = FredScraper({"scraping": {}})
    scraper.run()
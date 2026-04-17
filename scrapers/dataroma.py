"""
Dataroma Scraper
==============
Scrapes superinvestor holdings from Dataroma
Website: https://dataroma.com
"""
import logging
import re
from typing import List, Dict, Any
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class DataromaScraper(BaseScraper):
    """Scrapes superinvestor holdings and changes."""

    HOLDINGS_URL = "https://dataroma.com/grades/grades.php"
    TRACKING_URL = "https://dataroma.com/grades/track.php"

    # Top superinvestors to track
    SUPERINVESTORS = [
        "Warren Buffett",      # Berkshire Hathaway
        "Ray Dalio",         # Bridgewater
        "Bill Ackman",       # Pershing Square
        "Carl Icahn",       # Icahn Capital
        "David Tepper",      # Appaloosa
        "George Soros",      # Soros Fund
        "Stanley Druckenmiller",
        "John Paulson",
        "Leon Cooperman",
        "Jim Simons",        # Renaissance
    ]

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_holdings(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Fetch latest superinvestor holdings."""
        items = []

        response = self._fetch(self.HOLDINGS_URL)
        if not response:
            return items

        soup = self._parse_html(response)

        # Parse holdings table
        for row in soup.select("table.grades_table tr")[1:limit+1]:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            try:
                ticker = cols[0].get_text(strip=True)
                if not ticker or len(ticker) > 6 or not ticker.replace("$", "").isalnum():
                    continue

                company = cols[1].get_text(strip=True)
                sector = cols[2].get_text(strip=True)
                owner = cols[3].get_text(strip=True)
                action = cols[4].get_text(strip=True)
                shares = cols[5].get_text(strip=True) if len(cols) > 5 else ""
                value = cols[6].get_text(strip=True) if len(cols) > 6 else ""

                # Only track known superinvestors
                if any(si.lower() in owner.lower() for si in self.SUPERINVESTORS):
                    item = {
                        "url": self.HOLDINGS_URL,
                        "raw_content": f"{ticker}: {owner} {action} {shares} ({sector})",
                        "ticker": ticker,
                        "data_type": "superinvestor",
                        "company": company,
                        "sector": sector,
                        "owner": owner,
                        "action": action,
                        "shares": self._parse_number(shares),
                        "value": self._parse_number(value)
                    }
                    items.append(item)

            except (IndexError, ValueError):
                continue

        logger.info(f"Fetched {len(items)} superinvestor holdings")
        return items

    def fetch_tracking(self, ticker: str) -> Dict[str, Any]:
        """Track a specific ticker across all superinvestors."""
        params = {"symbol": ticker}
        response = self._fetch(self.TRACKING_URL, params)
        if not response:
            return {}

        soup = self._parse_html(response)

        # Parse tracking table
        owners = []
        for row in soup.select("table.tracking_table tr")[1:21]:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            try:
                owner = cols[0].get_text(strip=True)
                action = cols[2].get_text(strip=True)
                shares = cols[3].get_text(strip=True)

                if owner and action:
                    owners.append({
                        "owner": owner,
                        "action": action,
                        "shares": shares
                    })
            except:
                continue

        return {
            "ticker": ticker,
            "tracked_by": owners,
            "owner_count": len(owners)
        }

    def _parse_number(self, text: str) -> float:
        if not text:
            return 0.0
        text = text.replace("$", "").replace(",", "").replace(" ", "")
        multiplier = 1
        if text.endswith("M"):
            multiplier = 1000000
            text = text[:-1]
        elif text.endswith("K"):
            multiplier = 1000
            text = text[:-1]
        try:
            return float(text) * multiplier
        except ValueError:
            return 0.0

    def fetch(self) -> List[Dict[str, Any]]:
        return self.fetch_holdings()


# Run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = DataromaScraper({"scraping": {}})
    items = scraper.run()
    for item in items[:10]:
        print(f"  {item['ticker']}: {item['owner']}")
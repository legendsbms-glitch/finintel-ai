"""
OpenInsider Scraper
=================
Scrapes Form 4 insider filings and sentiment scores
Website: https://openinsider.com
"""
import re
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseScraper

logger = logging.getLogger(__name__)


class OpenInsiderScraper(BaseScraper):
    """Scrapes insider trading data from OpenInsider."""

    # Latest filings URL
    BASE_URL = "https://openinsider.com/insider/sentiment"

    # Form 4 ticker search
    TICKER_URL = "https://openinsider.com/cgi/stock/race8.pl"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_latest(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch latest insider filings."""
        items = []

        # Try main sentiment page first
        response = self._fetch(self.BASE_URL)
        if response:
            soup = self._parse_html(response)

            # Parse table rows (insider filings table)
            for row in soup.select("table.table-wsad tr")[1:limit+1]:  # Skip header
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue

                try:
                    # Extract data from columns
                    ticker = cols[1].get_text(strip=True)
                    if not ticker or len(ticker) > 6:
                        continue

                    company = cols[2].get_text(strip=True)
                    insider_name = cols[3].get_text(strip=True)
                    title = cols[4].get_text(strip=True)
                    action = cols[5].get_text(strip=True)
                    shares = cols[6].get_text(strip=True)
                    cost = cols[7].get_text(strip=True)
                    value = cols[8].get_text(strip=True)
                    filing_date = cols[10].get_text(strip=True)

                    # Parse numeric values
                    shares_num = self._parse_number(shares)
                    value_num = self._parse_number(value)

                    # Determine sentiment
                    sentiment = action.upper()  # BUY, SELL, etc.
                    is_large = value_num and value_num > 100000
                    is_c_suite = any(x in title.upper() for x in ["CEO", "CFO", "COO", "CTO", "CFO"])

                    item = {
                        "url": f"https://openinsider.com{cols[1].find('a')['href']}" if cols[1].find('a') else self.BASE_URL,
                        "raw_content": f"{ticker}: {insider_name} {action} {shares} shares ${value}",
                        "ticker": ticker,
                        "data_type": "form_4",
                        "company": company,
                        "insider_name": insider_name,
                        "insider_title": title,
                        "action": action,
                        "shares": shares_num,
                        "cost": cost,
                        "value": value_num,
                        "filing_date": filing_date,
                        "is_large_trade": is_large,
                        "is_c_suite": is_c_suite,
                        "trade_type": "buy" if "BUY" in action.upper() else "sell"
                    }
                    items.append(item)

                except (IndexError, ValueError) as e:
                    logger.debug(f"Skipping row: {e}")
                    continue

        logger.info(f"Fetched {len(items)} insider filings from OpenInsider")
        return items

    def fetch_by_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch filings for a specific ticker."""
        items = []

        params = {"symbol": ticker}
        response = self._fetch(self.TICKER_URL, params)

        if response:
            soup = self._parse_html(response)

            for row in soup.select("table.table-wsad tr")[1:51]:
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue

                try:
                    ticker = cols[1].get_text(strip=True)
                    insider_name = cols[3].get_text(strip=True)
                    action = cols[5].get_text(strip=True)
                    shares = cols[6].get_text(strip=True)
                    value = cols[8].get_text(strip=True)
                    filing_date = cols[10].get_text(strip=True)

                    value_num = self._parse_number(value)

                    item = {
                        "url": self.TICKER_URL,
                        "raw_content": f"{ticker}: {insider_name} {action} {shares} ${value}",
                        "ticker": ticker,
                        "data_type": "form_4",
                        "action": action,
                        "value": value_num,
                        "filing_date": filing_date,
                        "trade_type": "buy" if "BUY" in action.upper() else "sell"
                    }
                    items.append(item)

                except (IndexError, ValueError):
                    continue

        return items

    def _parse_number(self, text: str) -> float:
        """Parse formatted number strings."""
        if not text:
            return 0.0

        # Remove $, commas, K, M suffixes
        text = text.replace("$", "").replace(",", "").replace(" ", "")

        multiplier = 1
        if text.endswith("K"):
            multiplier = 1000
            text = text[:-1]
        elif text.endswith("M"):
            multiplier = 1000000
            text = text[:-1]
        elif text.endswith("B"):
            multiplier = 1000000000
            text = text[:-1]

        try:
            return float(text) * multiplier
        except ValueError:
            return 0.0

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch latest insider filings."""
        return self.fetch_latest(limit=50)


# Allow running directly
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    scraper = OpenInsiderScraper({"scraping": {}})
    items = scraper.run()
    for item in items[:5]:
        print(f"  {item.get('ticker')}: {item.get('value')}")
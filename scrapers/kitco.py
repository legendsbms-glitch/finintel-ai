"""
Kitco Scraper
============
Scrapes gold, silver spot prices and lease rates
Website: https://www.kitco.com
"""
import logging
import re
from typing import List, Dict, Any
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class KitcoScraper(BaseScraper):
    """Scrapes precious metals data from Kitco."""

    GOLD_URL = "https://www.kitco.com/price/"
    SILVER_URL = "https://www.kitco.com/price/"
    MARKET_URL = "https://www.kitco.com/charts/"
    INDIAN_URL = "https://www.kitco.com/price/"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_prices(self) -> List[Dict[str, Any]]:
        """Fetch current spot prices for gold and silver."""
        items = []

        # Gold spot
        response = self._fetch(self.GOLD_URL)
        if response:
            soup = self._parse_html(response)
            
            # Try to find price in page
            price_elem = soup.select(".price")[0] if soup.select(".price") else None
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                items.append({
                    "url": self.GOLD_URL,
                    "raw_content": f"Gold spot: {price_text}",
                    "ticker": "XAUUSD",
                    "data_type": "spot_price",
                    "price": price_text,
                    "metal": "gold"
                })

        # Silver spot
        response = self._fetch(self.SILVER_URL)
        if response:
            soup = self._parse_html(response)
            price_elem = soup.select(".price")[0] if soup.select(".price") else None
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                items.append({
                    "url": self.SILVER_URL,
                    "raw_content": f"Silver spot: {price_text}",
                    "ticker": "XAGUSD",
                    "data_type": "spot_price",
                    "price": price_text,
                    "metal": "silver"
                })

        # Try market page for both
        response = self._fetch(self.MARKET_URL)
        if response:
            soup = self._parse_html(response)

            for row in soup.select("table.markets-table tr")[1:10]:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                try:
                    metal = cols[0].get_text(strip=True)
                    bid = cols[1].get_text(strip=True)
                    ask = cols[2].get_text(strip=True)

                    if metal in ["Gold", "Silver", "Platinum", "Palladium"]:
                        ticker = {"Gold": "XAU", "Silver": "XAG", "Platinum": "XPT", "Palladium": "XPD"}.get(metal, "")
                        
                        item = {
                            "url": self.MARKET_URL,
                            "raw_content": f"{metal}: Bid {bid}, Ask {ask}",
                            "ticker": ticker + "USD",
                            "data_type": "spot_price",
                            "bid": bid,
                            "ask": ask,
                            "metal": metal.lower()
                        }
                        
                        # Avoid duplicates
                        if not any(i.get("ticker") == item["ticker"] for i in items):
                            items.append(item)

                except (IndexError, ValueError):
                    continue

        logger.info(f"Fetched {len(items)} metal prices")
        return items

    def fetch_indian_gold(self) -> List[Dict[str, Any]]:
        """Fetch Indian gold premiums (important for India sentiment)."""
        items = []

        response = self._fetch(self.INDIAN_URL)
        if response:
            soup = self._parse_html(response)

            # Look for Indian gold premium data
            for row in soup.select("table.gold-rates tr")[1:6]:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                try:
                    city = cols[0].get_text(strip=True)
                    premium = cols[1].get_text(strip=True)
                    change = cols[2].get_text(strip=True)

                    if city and premium:
                        items.append({
                            "url": self.INDIAN_URL,
                            "raw_content": f"India {city}: {premium} ({change})",
                            "ticker": "INR",
                            "data_type": "premium",
                            "city": city,
                            "premium": premium,
                            "change": change
                        })

                except:
                    continue

        return items

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch all metal data."""
        items = []
        items.extend(self.fetch_prices())
        items.extend(self.fetch_indian_gold())
        return items


# Gold/Silver ratio scraper
class GoldSilverRatioScraper(BaseScraper):
    """Tracks gold/silver ratio for signals."""

    RATIO_URL = "https://www.macrotrends.net/1148/gold-silver-ratio"

    def fetch_ratio(self) -> List[Dict[str, Any]]:
        items = []
        
        response = self._fetch(self.RATIO_URL)
        if response:
            soup = self._parse_html(response)
            
            # Find ratio in table
            ratio_elem = soup.select("table[data-table='gold-silver-ratio'] td")[0] if soup.select("table[data-table='gold-silver-ratio'] td") else None
            
            if ratio_elem:
                ratio = ratio_elem.get_text(strip=True)
                items.append({
                    "url": self.RATIO_URL,
                    "raw_content": f"Gold/Silver Ratio: {ratio}",
                    "ticker": "XAU/XAG",
                    "data_type": "ratio",
                    "ratio": ratio
                })

        return items

    def fetch(self) -> List[Dict[str, Any]]:
        return self.fetch_ratio()


# Run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = KitcoScraper({"scraping": {}})
    items = scraper.run()
    for item in items:
        print(f"  {item.get('ticker')}: {item.get('raw_content')}")
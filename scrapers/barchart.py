"""
Barchart Scraper
===============
Scrapes dark pool prints and unusual options activity
Website: https://barchart.com
"""
import logging
import re
from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BarchartScraper(BaseScraper):
    """Scrapes dark pool and options flow data from Barchart."""

    # Key pages
    DARK_POOL_URL = "https://www.barchart.com/market-data/unusual-activity"
    OPTIONS_URL = "https://www.barchart.com/stocks-most-accurate/includes-options-flow"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_dark_pool(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Fetch dark pool prints (unusual volume in dark pools)."""
        items = []

        response = self._fetch(self.DARK_POOL_URL)
        if not response:
            return items

        soup = self._parse_html(response)

        # Find the dark pool table
        for row in soup.select("table[data-table-name='unusual-options-activity'] tr")[1:limit+1]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue

            try:
                ticker = cols[0].get_text(strip=True)
                if not ticker or ticker == "Symbol":
                    continue

                # Parse columns
                name = cols[1].get_text(strip=True)
                price = cols[2].get_text(strip=True)
                change = cols[3].get_text(strip=True)
                vol = cols[4].get_text(strip=True)
                pct = cols[5].get_text(strip=True)
                dollar_vol = cols[6].get_text(strip=True)
                "vol" = cols[7].get_text(strip=True)  # Put/Call volume

                # Parse numeric values
                vol_num = self._parse_number(vol)
                pct_num = self._parse_number(pct.replace("%", ""))
                dollar_vol_num = self._parse_number(dollar_vol)

                # Flag as unusual if > 5x average
                is_unusual = pct_num and pct_num > 500  # 500% of typical

                item = {
                    "url": f"https://www.barchart.com/stocks/{ticker}/options",
                    "raw_content": f"{ticker}: {vol} dark pool vol ({pct})",
                    "ticker": ticker,
                    "data_type": "dark_pool",
                    "price": price,
                    "change": change,
                    "volume": vol_num,
                    "volume_pct": pct_num,
                    "dollar_volume": dollar_vol_num,
                    "put_call": vol,
                    "is_unusual": is_unusual
                }
                items.append(item)

            except (IndexError, ValueError) as e:
                logger.debug(f"Parse error: {e}")
                continue

        logger.info(f"Fetched {len(items)} dark pool prints")
        return items

    def fetch_options_flow(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Fetch unusual options activity."""
        items = []

        response = self._fetch(self.OPTIONS_URL)
        if not response:
            return items

        soup = self._parse_html(response)

        for row in soup.select("table[data-table-name='most-accurate'] tr")[1:limit+1]:
            cols = row.find_all("td")
            if len(cols) < 8:
                continue

            try:
                ticker = cols[0].get_text(strip=True)
                if not ticker or ticker == "Symbol":
                    continue

                # Parse columns
                name = cols[1].get_text(strip=True)
                price = cols[2].get_text(strip=True)
                call_vol = cols[3].get_text(strip=True)
                put_vol = cols[4].get_text(strip=True)
                total_vol = cols[5].get_text(strip=True)
                call_oi = cols[6].get_text(strip=True)
                put_oi = cols[7].get_text(strip=True)

                total_num = self._parse_number(total_vol)
                call_num = self._parse_number(call_vol)
                put_num = self._parse_number(put_vol)

                # Determine sentiment (call > put = bullish)
                sentiment = "neutral"
                if call_num > put_num * 1.5:
                    sentiment = "bullish"
                elif put_num > call_num * 1.5:
                    sentiment = "bearish"

                item = {
                    "url": f"https://www.barchart.com/stocks/{ticker}/options",
                    "raw_content": f"{ticker}: {call_vol} calls, {put_vol} puts",
                    "ticker": ticker,
                    "data_type": "options_flow",
                    "price": price,
                    "call_volume": call_num,
                    "put_volume": put_num,
                    "total_volume": total_num,
                    "sentiment": sentiment
                }
                items.append(item)

            except (IndexError, ValueError):
                continue

        logger.info(f"Fetched {len(items)} options flow records")
        return items

    def _parse_number(self, text: str) -> float:
        """Parse formatted numbers (M, K suffixes)."""
        if not text:
            return 0.0

        text = text.replace("$", "").replace(",", "").replace("%", "").replace(" ", "")

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
        """Fetch both dark pool and options flow."""
        items = []
        items.extend(self.fetch_dark_pool())
        items.extend(self.fetch_options_flow())
        return items


# Allow running directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = BarchartScraper({"scraping": {}})
    items = scraper.run()
    for item in items[:10]:
        print(f"  {item.get('ticker')}: {item.get('data_type')}")
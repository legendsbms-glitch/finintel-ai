"""
IPO/ICO Scraper
==============
Scrapes upcoming IPOs from Renaissance Capital and ICOs from CoinGecko
"""
import logging
import json
import requests
from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseScraper

logger = logging.getLogger(__name__)


class IPOscraper(BaseScraper):
    """Scrapes IPO calendar from Renaissance Capital."""

    # Renaissance Capital RSS
    IPO_RSS = "https://www.renaissancecapital.com/Research/IPO-Calendar/rss.aspx"
    
    # IPOMonitor (alternative)
    IPOMONITOR_URL = "https://www.iposcreener.com"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_ipo_rss(self, limit: int = 15) -> List[Dict[str, Any]]:
        """Fetch IPOs via Renaissance Capital RSS."""
        items = []

        response = self._fetch(self.IPO_RSS)
        if not response:
            return items

        try:
            import feedparser
            feed = feedparser.parse(response.content)

            for entry in feed.entries[:limit]:
                title = entry.get("title", "")
                
                # Parse IPO info from title
                # Format: "TICKER: Company Name (Exchange) - $XX.XM"
                parts = title.split(":")
                ticker = parts[0].strip() if parts else ""
                
                # Extract price info
                price = "$"
                price_idx = title.find("($")
                if price_idx > 0:
                    end = title.find(")", price_idx)
                    if end > price_idx:
                        price = title[price_idx:end+1]

                # Extract exchange
                exchange = "NASDAQ"
                exchange_idx = title.find("(")
                if exchange_idx > 0:
                    end = title.find(")", exchange_idx)
                    if end > exchange_idx:
                        exchange = title[exchange_idx+1:end]

                item = {
                    "url": entry.get("link", ""),
                    "raw_content": title,
                    "ticker": ticker,
                    "data_type": "ipo",
                    "company": title.split(") ")[-1] if ") " in title else title,
                    "exchange": exchange,
                    "price": price,
                    "status": "upcoming"
                }
                items.append(item)

        except Exception as e:
            logger.error(f"RSS parse error: {e}")

        logger.info(f"Fetched {len(items)} IPOs")
        return items

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch upcoming IPOs."""
        return self.fetch_ipo_rss()


class ICOscraper(BaseScraper):
    """Scrapes ICO/token launch data from CoinGecko."""

    # CoinGecko API (free tier)
    COINGECKO_API = "https://api.coingecko.com/api/v3"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def fetch_upcoming(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch upcoming ICOs from CoinGecko."""
        items = []

        try:
            # Get trending coins (includes new listings)
            url = f"{self.COINGECKO_API}/search/trending"
            response = self._fetch(url)
            
            if not response:
                return items

            data = response.json()
            coins = data.get("coins", [])

            for coin in coins[:limit]:
                item_data = coin.get("item", {})
                
                # Only include recently added / upcoming
                name = item_data.get("name", "")
                symbol = item_data.get("symbol", "").upper()
                market_cap_rank = item_data.get("market_cap_rank", 0)

                # New coins (high rank = recent) or low cap = potential upcoming
                if market_cap_rank < 500 or market_cap_rank > 1000:
                    item = {
                        "url": f"https://www.coingecko.com/en/coins/{item_data.get('id')}",
                        "raw_content": f"{symbol}: {name} (Rank #{market_cap_rank})",
                        "ticker": symbol,
                        "data_type": "ico",
                        "name": name,
                        "market_cap_rank": market_cap_rank,
                        "score": 6 if market_cap_rank < 100 else 5
                    }
                    items.append(item)

        except Exception as e:
            logger.error(f"CoinGecko error: {e}")

        logger.info(f"Fetched {len(items)} ICOs")
        return items

    def fetch_trending(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch trending coins (includes new launches)."""
        items = []

        try:
            url = f"{self.COINGECKO_API}/search/trending"
            response = self._fetch(url)
            
            if not response:
                return items

            data = response.json()
            coins = data.get("coins", [])

            for coin in coins[:limit]:
                item_data = coin.get("item", {})
                
                item = {
                    "url": f"https://www.coingecko.com/en/coins/{item_data.get('id')}",
                    "raw_content": f"{item_data.get('symbol', '').upper()}: {item_data.get('name')}",
                    "ticker": item_data.get("symbol", "").upper(),
                    "data_type": "trending",
                    "name": item_data.get("name"),
                    "market_cap_rank": item_data.get("market_cap_rank"),
                    "thumb": item_data.get("thumb")
                }
                items.append(item)

        except Exception as e:
            logger.error(f"CoinGecko trending error: {e}")

        return items

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch ICO data."""
        items = []
        items.extend(self.fetch_upcoming())
        items.extend(self.fetch_trending())
        return items


# Run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test IPO
    ipo_scraper = IPOscraper({"scraping": {}})
    ipo_items = ipo_scraper.fetch()
    print(f"\n{len(ipo_items)} IPOs:")
    for item in ipo_items[:5]:
        print(f"  {item.get('ticker')}: {item.get('company')[:40]}")
    
    # Test ICO
    ico_scraper = ICOscraper({"scraping": {}})
    ico_items = ico_scraper.fetch()
    print(f"\n{len(ico_items)} ICOs:")
    for item in ico_items[:5]:
        print(f"  {item.get('ticker')}: {item.get('name', 'N/A')[:40]}")
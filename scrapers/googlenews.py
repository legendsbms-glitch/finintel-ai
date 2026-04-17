"""
Google News RSS Scraper
=====================
Fetches geopolitical and market news via Google News RSS feeds
"""
import logging
import feedparser
from typing import List, Dict, Any
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class GoogleNewsScraper(BaseScraper):
    """Fetches news via Google News RSS."""

    # Key news search queries (URL-encoded)
    NEWS_FEEDS = {
        "fed": "https://news.google.com/rss/search?q=Fed+OR+Federal+Reserve&hl=en-US&gl=US&ceid=US:en",
        "inflation": "https://news.google.com/rss/search?q=inflation+OR+CPI+OR+PCE&hl=en-US&gl=US&ceid=US:en",
        "tariff": "https://news.google.com/rss/search?q=tariff+OR+trade+war&hl=en-US&gl=US&ceid=US:en",
        "earnings": "https://news.google.com/rss/search?q=earnings+OR+quarterly&hl=en-US&gl=US&ceid=US:en",
        "ipo": "https://news.google.com/rss/search?q=IPO+OR+initial+public+offer&hl=en-US&gl=US&ceid=US:en",
        "crypto": "https://news.google.com/rss/search?q=bitcoin+OR+cryptocurrency&hl=en-US&gl=US&ceid=US:en",
        "oil": "https://news.google.com/rss/search?q=oil+OR+OPEC+OR+crude&hl=en-US&gl=US&ceid=US:en",
        "geopolitical": "https://news.google.com/rss/search?q=geopolitical+OR+war+OR+sanction&hl=en-US&gl=US&ceid=US:en",
    }

    # Keywords that trigger higher importance
    HIGH_IMPORTANCE_KEYWORDS = [
        "fed", "federal reserve", "rate hike", "rate cut", "tapering",
        "tariff", "trade war", "sanction", "crisis",
        "recession", "default", "bankruptcy",
        "major", "breaking", "urgent", "alert"
    ]

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Override timeout for RSS feeds
        self.request_timeout = 15

    def fetch_feed(self, feed_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch a specific news feed."""
        feed_url = self.NEWS_FEEDS.get(feed_name)
        if not feed_url:
            return []

        items = []
        
        response = self._fetch(feed_url)
        if not response:
            return items

        try:
            # Parse RSS XML
            feed = feedparser.parse(response.content)
            
            for entry in feed.entries[:limit]:
                try:
                    title = entry.get("title", "")[:200]
                    link = entry.get("link", "")
                    published = entry.get("published", "")
                    
                    # Check for high-importance keywords
                    importance = 5
                    title_lower = title.lower()
                    for keyword in self.HIGH_IMPORTANCE_KEYWORDS:
                        if keyword in title_lower:
                            importance = 8
                            break

                    item = {
                        "url": link,
                        "raw_content": title,
                        "ticker": None,
                        "data_type": f"news_{feed_name}",
                        "title": title,
                        "published": published,
                        "importance": importance,
                        "feed": feed_name
                    }
                    items.append(item)

                except Exception as e:
                    logger.debug(f"Parse error: {e}")
                    continue

        except Exception as e:
            logger.error(f"Feed error for {feed_name}: {e}")

        return items

    def fetch_all(self, limit_per_feed: int = 5) -> List[Dict[str, Any]]:
        """Fetch all configured news feeds."""
        all_items = []

        for feed_name in self.NEWS_FEEDS.keys():
            items = self.fetch_feed(feed_name, limit_per_feed)
            all_items.extend(items)
            logger.info(f"  {feed_name}: {len(items)} items")

        return all_items

    def search_keywords(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """Search for specific keywords in news."""
        items = []

        # Build search query
        query = "+OR+".join(keywords)
        search_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

        response = self._fetch(search_url)
        if not response:
            return items

        try:
            feed = feedparser.parse(response.content)
            
            for entry in feed.entries[:20]:
                title = entry.get("title", "")[:200]
                item = {
                    "url": entry.get("link", ""),
                    "raw_content": title,
                    "ticker": None,
                    "data_type": "news_search",
                    "title": title,
                    "keywords_found": [k for k in keywords if k.lower() in title.lower()],
                    "importance": 7  # Default search importance
                }
                items.append(item)

        except Exception as e:
            logger.error(f"Search error: {e}")

        return items

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch all feeds."""
        return self.fetch_all(limit_per_feed=5)


# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = GoogleNewsScraper({"scraping": {}})
    items = scraper.run()
    print(f"\nTotal: {len(items)} news items")
    for item in items[:10]:
        print(f"  [{item.get('importance')}] {item.get('title')[:80]}")
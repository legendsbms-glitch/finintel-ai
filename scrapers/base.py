"""
Base Scraper Class
================
All scrapers inherit from this.
"""
import os
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all data scrapers."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.request_timeout = config.get("scraping", {}).get("request_timeout", 30)
        self.rate_limit_delay = config.get("scraping", {}).get("rate_limit_delay", 2)
        self.retry_attempts = config.get("scraping", {}).get("retry_attempts", 3)
        self.user_agent = config.get("scraping", {}).get(
            "user_agent", 
            "FinIntelAI/1.0 (Personal Financial Research Tool)"
        )
        self._last_request_time = 0

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }

    def _rate_limit(self):
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _fetch(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """Fetch a URL with retry logic."""
        self._rate_limit()

        for attempt in range(self.retry_attempts):
            try:
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=self.request_timeout
                )
                response.raise_for_status()
                return response

            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"All attempts failed for {url}")
        return None

    def _parse_html(self, response: requests.Response) -> BeautifulSoup:
        """Parse HTML response."""
        return BeautifulSoup(response.content, "lxml")

    @abstractmethod
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the source.
        Returns list of raw data items.
        """
        pass

    def save_to_db(self, db, items: List[Dict[str, Any]]):
        """Save fetched items to database."""
        import sqlite3

        if isinstance(db, str):
            conn = sqlite3.connect(db)
        else:
            conn = db

        cursor = conn.cursor()

        for item in items:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO raw_data 
                    (source, url, raw_content, ticker, data_type)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    self.__class__.__name__.lower().replace("scraper", ""),
                    item.get("url"),
                    item.get("raw_content"),
                    item.get("ticker"),
                    item.get("data_type")
                ))
            except Exception as e:
                logger.error(f"Error saving item: {e}")

        conn.commit()
        if isinstance(db, str):
            conn.close()

    def run(self, db_path: str = "data/finintel.db"):
        """Fetch and save data."""
        logger.info(f"Running {self.__class__.__name__}...")
        items = self.fetch()
        if items:
            self.save_to_db(db_path, items)
            logger.info(f"Saved {len(items)} items")
        else:
            logger.info("No new items")
        return items
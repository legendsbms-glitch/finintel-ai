"""
FinIntel AI - Main Orchestrator
========================
Coordinates: scraping → AI processing → storage → dashboard
"""
import os
import sys
import logging
import sqlite3
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from scrapers.fred import FredScraper
from scrapers.openinsider import OpenInsiderScraper
from scrapers.barchart import BarchartScraper
from scrapers.dataroma import DataromaScraper
from scrapers.kitco import KitcoScraper, GoldSilverRatioScraper
from scrapers.googlenews import GoogleNewsScraper
from scrapers.ipoico import IPOscraper, ICOscraper
from ai_brain.client import GroqClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


class FinIntelOrchestrator:
    """Main orchestrator for the FinIntel system."""

    def __init__(self, config_path: str = "config/config.yaml"):
        # Load config
        self.config = self._load_config(config_path)

        # Paths
        self.db_path = self.config.get("database", {}).get("path", "data/finintel.db")
        self._ensure_data_dir()

        # Initialize database
        self._init_db()

        # Initialize clients
        api_key = os.environ.get("GROQ_API_KEY", "")
        model = self.config.get("ai_brain", {}).get("model", "llama-3-70b-8192")
        self.ai = GroqClient(api_key=api_key, model=model)

        # Initialize scrapers
        self.scrapers = {
            # Core (Phase 1)
            "fred": FredScraper(self.config),
            "openinsider": OpenInsiderScraper(self.config),
            "barchart": BarchartScraper(self.config),
            # Phase 2 additions
            "dataroma": DataromaScraper(self.config),
            "kitco": KitcoScraper(self.config),
            "gold_ratio": GoldSilverRatioScraper(self.config),
            "news": GoogleNewsScraper(self.config),
            "ipo": IPOscraper(self.config),
            "ico": ICOscraper(self.config),
        }

    def _load_config(self, path: str) -> dict:
        """Load YAML config."""
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config not found: {path}, using defaults")
            return {"scraping": {}, "ai_brain": {}}

    def _ensure_data_dir(self):
        """Create data directory if needed."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self):
        """Initialize SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Load and execute schema
        schema_path = Path(__file__).parent / "storage" / "schema.sql"
        if schema_path.exists():
            with open(schema_path) as f:
                cursor.executescript(f.read())

        # Create watchlist from config
        watchlist = self.config.get("watchlist", {})
        for ticker in watchlist.get("equities", []):
            cursor.execute(
                "INSERT OR IGNORE INTO watchlist (ticker, asset_class) VALUES (?, ?)",
                (ticker, "equity")
            )
        for ticker in watchlist.get("etfs", []):
            cursor.execute(
                "INSERT OR IGNORE INTO watchlist (ticker, asset_class) VALUES (?, ?)",
                (ticker, "etf")
            )
        for ticker in watchlist.get("crypto", []):
            cursor.execute(
                "INSERT OR IGNORE INTO watchlist (ticker, asset_class) VALUES (?, ?)",
                (ticker, "crypto")
            )

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def run_scrapers(self, sources: list = None) -> dict:
        """Run all configured scrapers."""
        if sources is None:
            sources = list(self.scrapers.keys())

        results = {}
        for source in sources:
            scraper = self.scrapers.get(source)
            if not scraper:
                logger.warning(f"Unknown scraper: {source}")
                continue

            try:
                logger.info(f"Running {source} scraper...")
                items = scraper.fetch()
                scraper.save_to_db(self.db_path, items)
                results[source] = len(items)
                logger.info(f"  → {len(items)} items cached")

            except Exception as e:
                logger.error(f"Error running {source}: {e}")
                results[source] = 0

        return results

    def process_unprocessed(self, use_ai: bool = True) -> int:
        """Process raw data through AI brain."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get unprocessed items
        cursor.execute("""
            SELECT id, source, raw_content, ticker, data_type
            FROM raw_data
            WHERE id NOT IN (
                SELECT raw_data_id 
                FROM scored_items 
                WHERE raw_data_id IS NOT NULL
            )
            ORDER BY fetched_at DESC
            LIMIT 100
        """)
        raw_items = cursor.fetchall()

        if not raw_items:
            logger.info("No new items to process")
            conn.close()
            return 0

        processed = 0
        for row in raw_items:
            item = {
                "id": row[0],
                "source": row[1],
                "raw_content": row[2],
                "ticker": row[3],
                "data_type": row[4]
            }

            # Score with AI or rules
            if use_ai and self.ai.api_key:
                try:
                    result = self.ai.analyze(item["raw_content"])
                    if result:
                        item.update(result)
                except Exception as e:
                    logger.debug(f"AI error for {item.get('ticker')}: {e}")
                    item.update(self.ai.score_with_rules(item))
            else:
                item.update(self.ai.score_with_rules(item))

            # Save to scored_items
            try:
                cursor.execute("""
                    INSERT INTO scored_items 
                    (source, raw_data_id, ticker, category, title, importance_score, importance_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get("source"),
                    item.get("id"),
                    item.get("ticker"),
                    item.get("category", "Equity"),
                    item.get("summary", "")[:200],
                    item.get("importance_score", 5),
                    item.get("reason", "")
                ))
                processed += 1

            except sqlite3.Error as e:
                logger.error(f"DB error: {e}")

        conn.commit()
        conn.close()

        logger.info(f"Processed {processed} items through AI brain")
        return processed

    def get_top_signals(self, limit: int = 20, 
                      min_score: int = 5,
                      category: str = None) -> list:
        """Get top signals from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
            SELECT ticker, title, category, importance_score, importance_reason, fetched_at
            FROM scored_items
            WHERE is_archived = 0
            AND importance_score >= ?
        """
        params = [min_score]

        if category:
            query += " AND category = ?"
            params.append(category)

        query += " ORDER BY importance_score DESC, fetched_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()

        signals = []
        for row in results:
            signals.append({
                "ticker": row[0],
                "title": row[1],
                "category": row[2],
                "score": row[3],
                "reason": row[4],
                "time": row[5]
            })

        return signals

    def get_watchlist_signals(self) -> list:
        """Get signals for items in watchlist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT w.ticker, w.asset_class, s.importance_score, s.title, s.category
            FROM watchlist w
            LEFT JOIN scored_items s ON w.ticker = s.ticker AND s.is_archived = 0
            WHERE s.importance_score IS NOT NULL
            ORDER BY s.importance_score DESC
        """)

        results = cursor.fetchall()
        conn.close()

        return [{
            "ticker": row[0],
            "asset_class": row[1],
            "score": row[2],
            "title": row[3],
            "category": row[4]
        } for row in results]

    def run_full_cycle(self) -> dict:
        """Run complete scraping + processing cycle."""
        logger.info("=" * 50)
        logger.info("Starting FinIntel AI cycle")
        logger.info("=" * 50)

        # Step 1: Scrape
        logger.info("Phase 1: Scraping...")
        scrape_results = self.run_scrapers()
        
        # Step 2: Process through AI
        logger.info("Phase 2: AI Processing...")
        processed = self.process_unprocessed()

        # Step 3: Summary
        top = self.get_top_signals(limit=10, min_score=7)
        logger.info(f"Phase 3: Top signals (score≥7):")
        for sig in top:
            logger.info(f"  [{sig['score']}] {sig['ticker'] or sig['category']}: {sig['title'][:60]}")

        return {
            "scraped": scrape_results,
            "processed": processed,
            "top_signals": top
        }


# CLI
def main():
    parser = argparse.ArgumentParser(description="FinIntel AI Orchestrator")
    parser.add_argument("--scrape", nargs="+", help="Specific scrapers to run")
    parser.add_argument("--process", action="store_true", help="Process through AI brain")
    parser.add_argument("--signals", action="store_true", help="Show top signals")
    parser.add_argument("--watchlist", action="store_true", help="Show watchlist signals")
    parser.add_argument("--full", action="store_true", help="Run full cycle")
    parser.add_argument("--config", default="config/config.yaml", help="Config path")

    args = parser.parse_args()

    orch = FinIntelOrchestrator(args.config)

    if args.full:
        orch.run_full_cycle()
    elif args.scrape:
        orch.run_scrapers(args.scrape)
    elif args.process:
        orch.process_unprocessed()
    elif args.signals:
        for sig in orch.get_top_signals():
            print(f"[{sig['score']}] {sig['ticker']}: {sig['title'][:80]}")
    elif args.watchlist:
        for sig in orch.get_watchlist_signals():
            print(f"[{sig['score']}] {sig['ticker']} ({sig['asset_class']}): {sig['title'][:60]}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
"""
FinIntel AI Scheduler
===================
Automated scheduling for scrapers and processing
"""
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import sys
import os
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import FinIntelOrchestrator
from ai_brain.cross_signal import CrossSignalEngine, WatchlistManager

logger = logging.getLogger(__name__)


class FinIntelScheduler:
    """ scheduler for FinIntel AI."""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.orch = FinIntelOrchestrator(config_path)
        self.scheduler = BlockingScheduler()
        self.db_path = self.orch.db_path

    def job_scrape_and_process(self):
        """Combined scrape + process job."""
        logger.info("=" * 40)
        logger.info("Scheduled job: Scrape + Process")
        logger.info("=" * 40)
        
        try:
            # Scrape all sources
            self.orch.run_scrapers()
            
            # Process through AI
            self.orch.process_unprocessed()
            
            # Cross-signal detection
            cross = CrossSignalEngine(self.db_path)
            cross.run_detection()
            
            logger.info("Job completed successfully")
            
        except Exception as e:
            logger.error(f"Job error: {e}")

    def job_daily_summary(self):
        """Generate daily summary."""
        logger.info("Generating daily summary...")
        from ai_brain.cross_signal import NotificationManager
        nm = NotificationManager(self.db_path)
        
        # Would send email in production
        summary = nm.generate_daily_summary()
        logger.info(f"Summary: {summary[:200]}...")

    def start(self, scrape_interval_hours: int = 4):
        """Start the scheduler."""
        
        # Main job: scrape + process every N hours
        self.scheduler.add_job(
            self.job_scrape_and_process,
            'interval',
            hours=scrape_interval_hours,
            id='scrape_process',
            replace_existing=True
        )
        
        # Daily summary at 8 AM UTC
        self.scheduler.add_job(
            self.job_daily_summary,
            CronTrigger(hour=8, minute=0),
            id='daily_summary',
            replace_existing=True
        )
        
        logger.info(f"Scheduler started. Main job every {scrape_interval_hours} hours.")
        logger.info("Press Ctrl+C to stop.")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")
            self.scheduler.shutdown()


# CLI
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="FinIntel AI Scheduler")
    parser.add_argument("--hours", type=int, default=4, help="Scrape interval in hours")
    parser.add_argument("--config", default="config/config.yaml", help="Config path")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    
    if args.once:
        scheduler = FinIntelScheduler(args.config)
        scheduler.job_scrape_and_process()
    else:
        scheduler = FinIntelScheduler(args.config)
        scheduler.start(args.hours)
"""
Cross-Signal Detection Engine
=========================
Detects connections between signals across different asset categories
"""
import sqlite3
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CrossSignalEngine:
    """AI-powered cross-asset signal detection."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def find_correlations(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Find correlated signals in the time window."""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Get recent signals
        cursor.execute("""
            SELECT id, ticker, category, title, importance_score, fetched_at
            FROM scored_items
            WHERE is_archived = 0
            AND fetched_at > datetime('now', '-' || ? || ' hours')
            ORDER BY fetched_at DESC
        """, (hours,))

        signals = cursor.fetchall()
        conn.close()

        if len(signals) < 2:
            return []

        correlations = []

        # Build signal index
        by_ticker = {}
        by_category = {}
        
        for sig in signals:
            sig_id, ticker, category, title, score, fetched = sig
            
            if ticker:
                if ticker not in by_ticker:
                    by_ticker[ticker] = []
                by_ticker[ticker].append({
                    "id": sig_id,
                    "title": title,
                    "score": score,
                    "category": category
                })
            
            if category not in by_category:
                by_category[category] = []
            by_category[category].append({
                "id": sig_id,
                "ticker": ticker,
                "title": title,
                "score": score
            })

        # Find correlations
        # 1. Same ticker across categories
        for ticker, sigs in by_ticker.items():
            if len(sigs) >= 2:
                categories = set(s["category"] for s in sigs)
                if len(categories) >= 2:
                    correlations.append({
                        "type": "multi_asset",
                        "ticker": ticker,
                        "categories": list(categories),
                        "signals": sigs,
                        "explanation": f"Signal for {ticker} across {len(categories)} categories"
                    })

        # 2. Sector correlations (simplified - would need sector mapping)
        # For now, flag high activity in multiple categories
        high_score_sigs = [s for s in signals if s[3] >= 7]
        if len(high_score_sigs) >= 3:
            categories = set(s[2] for s in high_score_sigs)
            if len(categories) >= 2:
                correlations.append({
                    "type": "sector_rotation",
                    "categories": list(categories),
                    "count": len(high_score_sigs),
                    "explanation": f"High activity across {len(categories)} categories"
                })

        return correlations

    def save_correlations(self, correlations: List[Dict[str, Any]]):
        """Save detected correlations to database."""
        conn = self.get_connection()
        cursor = conn.cursor()

        for corr in correlations:
            try:
                # Get signal IDs
                sigs = corr.get("signals", [])
                if len(sigs) >= 2:
                    cursor.execute("""
                        INSERT OR IGNORE INTO cross_signals
                        (signal_a_id, signal_b_id, connection_type, ai_explanation)
                        VALUES (?, ?, ?, ?)
                    """, (
                        sigs[0]["id"],
                        sigs[1]["id"],
                        corr.get("type", "correlated"),
                        corr.get("explanation", "")
                    ))
            except sqlite3.Error as e:
                logger.debug(f"Save error: {e}")

        conn.commit()
        conn.close()

    def run_detection(self) -> int:
        """Run full detection cycle."""
        logger.info("Running cross-signal detection...")
        
        correlations = self.find_correlations(hours=24)
        
        if correlations:
            self.save_correlations(correlations)
            logger.info(f"Found {len(correlations)} correlations")
        else:
            logger.info("No new correlations found")
        
        return len(correlations)


class WatchlistManager:
    """Manage personal watchlist with alerts."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def add(self, ticker: str, asset_class: str, notes: str = "") -> bool:
        """Add ticker to watchlist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO watchlist (ticker, asset_class, notes)
                VALUES (?, ?, ?)
            """, (ticker, asset_class, notes))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False

    def remove(self, ticker: str) -> bool:
        """Remove ticker from watchlist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("DELETE FROM watchlist WHERE ticker = ?", (ticker,))
            conn.commit()
            conn.close()
            return True
        except:
            conn.close()
            return False

    def get_with_alerts(self) -> List[Dict[str, Any]]:
        """Get watchlist with active signals."""
        conn = self.get_connection()
        
        df = pd.read_sql_query("""
            SELECT w.ticker, w.asset_class, w.notes,
                   s.importance_score, s.title, s.category
            FROM watchlist w
            LEFT JOIN scored_items s ON w.ticker = s.ticker 
                AND s.is_archived = 0 
                AND s.importance_score >= 6
            ORDER BY s.importance_score DESC NULLS LAST
        """, conn)
        
        conn.close()
        
        # Filter to only watchlist items with alerts
        alerts = []
        for _, row in df.iterrows():
            if row["importance_score"]:
                alerts.append({
                    "ticker": row["ticker"],
                    "asset_class": row["asset_class"],
                    "score": row["importance_score"],
                    "title": row["title"],
                    "category": row["category"]
                })
        
        return alerts


# Test
if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "data/finintel.db"
    
    engine = CrossSignalEngine(db)
    count = engine.run_detection()
    print(f"Found {count} correlations")


# Add email notifications (optional)
class NotificationManager:
    """Daily summary email notifications."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # Would use SMTP or SendGrid in production
        self.email_enabled = False

    def generate_daily_summary(self) -> str:
        """Generate daily summary text."""
        conn = sqlite3.connect(self.db_path)
        
        # Top signals
        top = pd.read_sql_query("""
            SELECT ticker, title, category, importance_score
            FROM scored_items
            WHERE is_archived = 0
            AND fetched_at > datetime('now', '-24 hours')
            AND importance_score >= 6
            ORDER BY importance_score DESC
            LIMIT 10
        """, conn)
        
        # Stats
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM scored_items
            WHERE is_archived = 0 AND fetched_at > datetime('now', '-24 hours')
        """)
        total_24h = cursor.fetchone()[0]
        
        conn.close()
        
        if top.empty:
            return "No high-priority signals in the last 24 hours."
        
        lines = ["📊 FinIntel Daily Summary", "=" * 30, ""]
        lines.append(f"Total signals (24h): {total_24h}")
        lines.append(f"High priority: {len(top)}")
        lines.append("")
        
        for _, row in top.iterrows():
            emoji = "🔴" if row["importance_score"] >= 8 else "🟠"
            lines.append(f"{emoji} [{row['importance_score']}] {row['ticker'] or row['category']}: {row['title'][:60]}")
        
        return "\n".join(lines)

    def send_if_configured(self, recipients: List[str]) -> bool:
        """Send daily summary if email configured."""
        if not self.email_enabled:
            return False
        
        summary = self.generate_daily_summary()
        
        # Would implement actual email sending here
        print(f"Would send to {recipients}: {summary[:200]}...")
        return True
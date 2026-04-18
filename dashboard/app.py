"""
FinIntel AI Dashboard
==================
Streamlit dashboard for viewing scored signals
"""
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# Page config
st.set_page_config(
    page_title="FinIntel AI",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling
st.markdown("""
<style>
    .main { background-color: #0e1117 }
    .stApp { background-color: #0e1117 }
    .score-high { color: #ff4b4b; font-weight: bold; }
    .score-medium { color: #ffa500; font-weight: bold; }
    .score-low { color: #90EE90; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Database path - use absolute path from project root
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
DB_PATH = os.environ.get("FININTEL_DB", str(PROJECT_ROOT / "data" / "finintel.db"))


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_signals(min_score: int = 1, category: str = None, limit: int = 50) -> pd.DataFrame:
    conn = get_connection()
    
    query = """
        SELECT 
            ticker,
            title,
            category,
            importance_score,
            importance_reason,
            fetched_at
        FROM scored_items
        WHERE is_archived = 0
        AND importance_score >= ?
    """
    params = [min_score]

    if category and category != "All":
        query += " AND category = ?"
        params.append(category)

    query += " ORDER BY importance_score DESC, fetched_at DESC LIMIT ?"
    params.append(limit)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_watchlist() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT w.ticker, w.asset_class, w.notes, w.added_at,
               s.importance_score, s.title, s.category, s.fetched_at
        FROM watchlist w
        LEFT JOIN (
            SELECT ticker, MAX(importance_score) as importance_score,
                   MAX(title) as title, MAX(category) as category, MAX(fetched_at) as fetched_at
            FROM scored_items
            WHERE is_archived = 0
            GROUP BY ticker
        ) s ON w.ticker = s.ticker
        ORDER BY s.importance_score DESC NULLS LAST
    """, conn)
    conn.close()
    return df


def get_macro_summary() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT ticker, title, category, importance_score, fetched_at
        FROM scored_items
        WHERE category = 'Macro'
        AND is_archived = 0
        ORDER BY fetched_at DESC
        LIMIT 20
    """, conn)
    conn.close()
    return df


def get_stats() -> Dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    
    # Total signals
    cursor.execute("SELECT COUNT(*) FROM scored_items WHERE is_archived = 0")
    total = cursor.fetchone()[0]
    
    # By category
    cursor.execute("""
        SELECT category, COUNT(*) as cnt
        FROM scored_items
        WHERE is_archived = 0
        GROUP BY category
    """)
    by_category = {row[0]: row[1] for row in cursor.fetchall()}
    
    # High importance
    cursor.execute("SELECT COUNT(*) FROM scored_items WHERE is_archived = 0 AND importance_score >= 8")
    high = cursor.fetchone()[0]
    
    # Recent activity
    cursor.execute("SELECT COUNT(*) FROM scored_items WHERE is_archived = 0 AND fetched_at > datetime('now', '-24 hours')")
    last_24h = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total": total,
        "by_category": by_category,
        "high_importance": high,
        "last_24h": last_24h
    }


def score_color(score: int) -> str:
    if score >= 8:
        return "🔴"
    elif score >= 6:
        return "🟠"
    else:
        return "🟢"


def main():
    st.title("📊 FinIntel AI")
    st.caption("Personal Financial Intelligence System")

    # Sidebar
    with st.sidebar:
        st.header("Controls")
        
        min_score = st.slider("Minimum Score", 1, 10, 5)
        category = st.selectbox("Category", 
            ["All", "Macro", "Equity", "Commodity", "Crypto", "Geopolitical", "IPO", "ICO"])
        
        st.divider()
        
        if st.button("🔄 Refresh Data"):
            st.rerun()
        
        st.divider()
        
        # Stats
        stats = get_stats()
        st.metric("Total Signals", stats["total"])
        st.metric("High Priority (≥8)", stats["high_importance"])
        st.metric("Last 24 Hours", stats["last_24h"])

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Signals", "👁️ Watchlist", "📉 Macro", "⚙️ Settings"])
    
    with tab1:
        st.subheader("🎯 Top Signals")
        
        df = get_signals(min_score=min_score, category=category)
        
        if df.empty:
            st.info("No signals yet. Run scrapers first: `python main.py --full`")
        else:
            for _, row in df.iterrows():
                score = row["importance_score"]
                emoji = score_color(score)
                
                with st.container():
                    col1, col2, col3 = st.columns([1, 4, 3])
                    with col1:
                        st.write(f"{emoji} **{score}**")
                    with col2:
                        ticker = row["ticker"] or row["category"]
                        st.write(f"**{ticker}**")
                    with col3:
                        st.caption(row["title"][:80])
                    st.divider()

    with tab2:
        st.subheader("👁️ Your Watchlist")
        
        watch = get_watchlist()
        
        if watch.empty:
            st.info("Watchlist empty. Add tickers in config.yaml")
        else:
            for _, row in watch.iterrows():
                score = row["importance_score"] or 0
                if score > 0:
                    emoji = score_color(score)
                    st.write(f"{emoji} **{row['ticker']}** ({row['asset_class']}) - {row['title'][:60] or 'No signal'}")
                else:
                    st.write(f"⚪ **{row['ticker']}** ({row['asset_class']}) - No signal")
                st.divider()

    with tab3:
        st.subheader("📉 Macro Overview")
        
        macro = get_macro_summary()
        
        if macro.empty:
            st.info("No macro data yet")
        else:
            st.dataframe(macro, use_container_width=True)

    with tab4:
        st.subheader("⚙️ Settings")
        
        st.write("**Scrapers Available:**")
        scrapers = [
            "fred - FRED API (Macro)",
            "openinsider - Insider Filings",
            "barchart - Dark Pool/Options",
            "dataroma - Superinvestors",
            "kitco - Gold/Silver",
            "gold_ratio - Gold/Silver Ratio",
            "news - Google News RSS",
            "ipo - IPO Calendar",
            "ico - ICO/Token Launches"
        ]
        for s in scrapers:
            st.write(f"- {s}")
        
        st.write("\n**To run:**")
        st.code("python main.py --scrape fred openinsider barchart\npython main.py --process\npython main.py --full")

    # Auto-refresh
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")


if __name__ == "__main__":
    main()
#!/bin/bash
# FinIntel AI Dashboard Launcher
# ============================

# Get directory of this script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Set database path
export FININTEL_DB="${DIR}/../data/finintel.db"

# Check if database exists
if [ ! -f "$FININTEL_DB" ]; then
    echo "Database not found. Run scrapers first:"
    echo "  python main.py --full"
    exit 1
fi

# Start Streamlit
echo "Starting FinIntel Dashboard..."
streamlit run dashboard/app.py --server.port 8501 --server.address localhost
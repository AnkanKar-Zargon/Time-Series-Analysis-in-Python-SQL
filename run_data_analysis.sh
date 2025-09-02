#!/bin/bash
set -euo pipefail

# Load environment variables from .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env file not found!"
    exit 1
fi

echo "🔗 Running full analysis on DB: $DATABASE as user: $USER"
mkdir -p results

# Map section names in SQL to CSV filenames
declare -A sections=(
    ["current_database"]="current_database.csv"
    ["overview"]="overview.csv"
    ["daily_summary"]="daily_summary.csv"
    ["daily_returns"]="daily_returns.csv"
    ["moving_averages"]="moving_averages.csv"
    ["volatility"]="volatility.csv"
    ["top_volume"]="top_volume.csv"
    ["gaps"]="gaps.csv"
)

# Loop over sections and run each query
for section in "${!sections[@]}"; do
    echo "📊 Running $section ..."
    
    # Extract query between markers -- Section: <section>
    query=$(awk "/-- Section: $section/{flag=1;next}/-- Section:/{flag=0}flag" data_analysis.sql)

    # Execute query and save to CSV (headers included)
    mysql -u"$USER" -p"$PASSWORD" "$DATABASE" --batch \
        -e "$query" > "results/${sections[$section]}"
done

echo "✅ All results saved in results/ folder"

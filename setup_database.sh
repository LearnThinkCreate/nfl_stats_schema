#!/bin/zsh

# NFL Stats Database Setup Script
# This script executes all SQL files in order to create the complete database schema
# Author: Warren Hyson

echo "🏈 NFL Stats Database Setup Script 🏈"
echo "------------------------------------"

# Database connection parameters
DB_USER="xxxxxxxx"
DB_PASSWORD="xxxxxxx"
DB_NAME="nfl_stats"  # Default database name - adjust if needed
DB_HOST="localhost"  # Default host - adjust if needed
DB_PORT="5432"       # Default port - adjust if needed

# List of SQL files in execution order
SQL_FILES=(
  "01_extensions.sql"
  "02_teams.sql"
  "03_players.sql"
  "04_games.sql"
  "05_pbp.sql"
  "06_playstats.sql"
  "07_snap_counts.sql"
  "08_game_stats_view.sql"
  "09_season_stats.sql"
  "10_career_stats.sql"
  "11_player_career_team.sql"
  "12_player_relevance.sql"
  "13_func_get_filtered_stats.sql"
)

# Create database if it doesn't exist
echo "Creating database if it doesn't exist..."
export PGPASSWORD="$DB_PASSWORD"
createdb -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" "$DB_NAME" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "Database already exists or could not be created. Continuing..."
fi

# Setup function for better error handling
execute_sql_file() {
  local file=$1
  echo "Executing $file..."
  
  psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -f "$file" 2>&1
  
  if [ $? -ne 0 ]; then
    echo "❌ Error executing $file"
    echo "You may need to fix errors and re-run this script."
    return 1
  else
    echo "✅ Successfully executed $file"
    return 0
  fi
}

# Get script directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || { echo "Cannot change to script directory"; exit 1; }

# Execute each SQL file in order
echo "Starting schema creation process..."

for file in "${SQL_FILES[@]}"; do
  if [ -f "$file" ]; then
    execute_sql_file "$file" || exit 1
  else
    echo "⚠️ Warning: File $file not found, skipping..."
  fi
done

echo "------------------------------------"
echo "🎉 Database schema setup complete!"
echo "To verify: psql -U $DB_USER -d $DB_NAME -c '\dt nfl_stats.*'"
echo "------------------------------------"

# Unset password env variable for security
unset PGPASSWORD

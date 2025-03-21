# NFL Statistics Database Schema

## Overview

This repository contains a PostgreSQL database schema designed for storing and analyzing NFL statistics. The system is optimized for a read-heavy analytics workload supporting an NFL Analytics web application. It enables efficient querying of player, team, and game statistics across multiple seasons with sophisticated filtering capabilities.

## Database Design Philosophy

The database follows several core design principles:

1. **Progressive Data Refinement**: Data flows from raw play-by-play data to increasingly aggregated views (play → game → season → career)
2. **Efficient Filtering**: Pre-computed flags in play-level tables support filtering by game situation (red zone, leading/trailing, etc.)
3. **Read-Heavy Optimization**: Structure prioritizes fast analytical queries with appropriate denormalization where beneficial
4. **Separation of Raw Data and Metrics**: Statistics views split into base aggregations and derived metrics for maintainability

## Schema Structure

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│   Teams  │     │  Players │     │   Games  │
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │
     └────────────────┴────────────────┼────────────┐
                                       │            │
                                       ▼            ▼
                                 ┌──────────┐  ┌──────────┐
                                 │   Plays  │  │Snap Counts│
                                 └────┬─────┘  └───────────┘
                                      │
                                      ▼
                                ┌──────────┐
                                │ PlayStats│
                                └────┬─────┘
                                     │
                                     ▼
                          ┌─────────────────────┐
                          │ Player Game Stats   │ (Materialized View)
                          └─────────┬───────────┘
                                    │
                   ┌────────────────┴────────────────┐
                   │                                 │
                   ▼                                 ▼
      ┌─────────────────────┐           ┌─────────────────────┐
      │Game Stats w/Metrics │           │    Season Stats     │ (View)
      │      (View)         │           └─────────┬───────────┘
      └─────────────────────┘                     │
                                    ┌─────────────┴─────────────┐
                                    │                           │
                                    ▼                           ▼
                        ┌─────────────────────┐      ┌─────────────────────┐
                        │Season Stats w/Metrics│      │    Career Stats     │ (View)
                        │      (View)          │      └─────────┬───────────┘
                        └─────────────────────┘                 │
                                                                ▼
                                                     ┌─────────────────────┐
                                                     │Career Stats w/Metrics│
                                                     │      (View)          │
                                                     └─────────────────────┘
```

### Core Tables

#### 1. Reference Tables

- **teams**: Team information including identifiers, names, divisions, conferences, and branding
- **players**: Player biographical information, positions, physical attributes, and team affiliations

#### 2. Game Information

- **games**: Game metadata including dates, teams, scores, weather, and stadium information

#### 3. Play-Level Data

- **plays**: Play-by-play data with pre-computed metrics and situation flags (partitioned by season)
- **playstats**: Player statistics at the play level with attribution, inherits situation flags (partitioned by season)
- **snap_counts**: Player participation data for each play

### Statistical Views and Materialized Views

The schema uses a progressive refinement approach to statistics:

1. **player_game_stats** (Materialized View): Aggregates play-level data to game-level statistics
   - Feeds into both player_game_stats_with_metrics and player_season_stats

2. **player_season_stats** (View): Aggregates game-level data from player_game_stats
   - Feeds into both player_season_stats_with_metrics and player_career_stats

3. **player_career_stats** (View): Aggregates season-level data from player_season_stats
   - Feeds into player_career_stats_with_metrics

Each level has a companion view that adds derived metrics without modifying the base data:
- **player_game_stats_with_metrics**: Adds per-game derived metrics
- **player_season_stats_with_metrics**: Adds per-season derived metrics
- **player_career_stats_with_metrics**: Adds career-level derived metrics

This separation allows for efficient calculation of the base aggregations while moving derived calculations to a separate step, optimizing read operations.

## Partitioning Strategy

Large fact tables are partitioned by season to improve query performance and maintenance:

```sql
CREATE TABLE plays (
    -- columns
    PRIMARY KEY (season, play_id, game_id)
) PARTITION BY RANGE (season);

-- Create partitions for each season
CREATE TABLE plays_2023 PARTITION OF plays
FOR VALUES FROM (2023) TO (2024);
```

Benefits of this approach include:
- Improved query performance for season-specific queries
- Easier maintenance (old seasons can be compressed or archived)
- More efficient index usage
- Parallel query opportunities

## Indexing Strategy

The indexing strategy is tailored to the common query patterns of an NFL analytics application:

### 1. Primary Identifiers
```sql
-- Primary team identifier
CREATE INDEX idx_teams_name_trgm ON teams USING GIN (team_name gin_trgm_ops);

-- Player name with trigram support for fuzzy search
CREATE INDEX idx_players_display_name_trgm ON players USING GIN (display_name gin_trgm_ops);
```

### 2. Common Filter Dimensions
```sql
-- Season and week filtering
CREATE INDEX idx_games_season_week ON games(season, week, game_type);

-- Team-based game filtering
CREATE INDEX idx_games_teams ON games(home_team, away_team);
```

### 3. UI-Specific Filtering Support
```sql
-- Support for common UI filters
CREATE INDEX idx_playstats_is_leading ON playstats (is_leading);
CREATE INDEX idx_playstats_is_red_zone ON playstats (is_red_zone);
CREATE INDEX idx_playstats_is_late_down ON playstats (is_late_down);
```

### 4. Player Performance Lookups
```sql
-- Indexes to support player-specific stat lookups
CREATE INDEX idx_plays_passer ON plays(passer_player_id) WHERE passer_player_id IS NOT NULL;
CREATE INDEX idx_plays_rusher ON plays(rusher_player_id) WHERE rusher_player_id IS NOT NULL;
CREATE INDEX idx_plays_receiver ON plays(receiver_player_id) WHERE receiver_player_id IS NOT NULL;
```

## Advanced Filtering Function

The schema provides a powerful dynamic SQL function (`get_filtered_season_stats`) that supports filtering by game situation flags (red zone, leading/trailing, etc.) that aren't available in the aggregated season views:

```sql
-- Example call to get QB red zone stats for 2023 regular season
SELECT * FROM get_filtered_season_stats(
    p_season => 2023,
    p_is_red_zone => 1,
    p_position => 'QB',
    p_season_type => 'REG',
    p_sort_column => 'completion_percentage',
    p_sort_direction => 'DESC',
    p_min_attempts => 20
);
```

This function:
1. Filters plays and playstats tables using situation flags (is_red_zone, is_leading, etc.)
2. Aggregates statistics only for plays matching these criteria
3. Allows for minimum thresholds (attempts, targets, etc.) for qualifying players
4. Returns player-season level statistics for filtered situations

In production applications, it's common to generate equivalent SQL on the server side (e.g., in JavaScript) rather than using the dynamic SQL function directly, for better performance and security.

That's what I'm doing for my application but I built this function first as a POC

## CTE Usage Example

Common Table Expressions (CTEs) are used extensively to modularize complex queries, particularly in statistical views:

```sql
CREATE MATERIALIZED VIEW player_game_stats AS
WITH playstats_agg AS (
    -- Aggregate playstats at player-game level
    SELECT
        ps.player_id,
        ps.game_id,
        -- Aggregations...
    FROM playstats ps
    JOIN players pl ON ps.player_id = pl.gsis_id
    GROUP BY ps.player_id, ps.game_id, ps.season, ps.week, ps.team
),
passing_stats AS (
    -- QB passing-specific metrics
    SELECT
        p.passer_player_id AS player_id,
        -- EPA calculations...
    FROM plays p
    WHERE p.play_type IN ('pass', 'qb_spike')
    GROUP BY p.passer_player_id, p.game_id
)
```

This approach allows:
1. Breaking complex calculations into manageable pieces
2. Better query optimizer understanding of data relationships
3. Improved readability and maintainability
4. Separation of concerns between raw aggregation and calculated metrics

## Setup and Maintenance

The database setup is automated through the `setup_database.sh` script, which:
1. Creates the database if it doesn't exist
2. Executes SQL files in the correct order to build the schema
3. Provides error handling and reporting

Ongoing maintenance is supported through functions like:
- `refresh_all_materialized_views()`: Refreshes statistical views after data updates
- `maintain_indexes()`: Identifies and rebuilds bloated indexes
- `create_playstats_partition()`: Creates new partitions for upcoming seasons

## Performance Optimization

The schema includes multiple layers of performance optimization:

### 1. Storage Optimizations

```sql
-- Optimize core tables for read performance
ALTER TABLE teams SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.4);
ALTER TABLE players SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.4);

-- Historical data compression
ALTER TABLE plays_2019 SET (toast_tuple_target = 8160);
```

### 2. Statistics Collection Tuning

```sql
-- Increase statistics gathering for frequently filtered columns
ALTER TABLE plays ALTER COLUMN is_red_zone SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_leading SET STATISTICS 1000;
```

### 3. Clustering Optimizations

```sql
-- Cluster plays table by game_id and play_id
CREATE INDEX idx_plays_2023_game_play ON plays_2023(game_id, play_id);
CLUSTER plays_2023 USING idx_plays_2023_game_play;
```

### 4. Parallel Query Optimization

```sql
-- Enable aggressive parallelism for analytical queries
CREATE OR REPLACE FUNCTION enable_parallel_analysis() 
RETURNS VOID AS $$
BEGIN
    SET max_parallel_workers_per_gather = 4;
    SET parallel_tuple_cost = 0.01;
    SET parallel_setup_cost = 100;
    SET min_parallel_table_scan_size = '8MB';
    SET min_parallel_index_scan_size = '512kB';
END;
$$ LANGUAGE plpgsql;
```

### 5. Materialized View Strategy

The use of materialized views for game-level statistics provides significant performance advantages:
- Pre-computed aggregations for common queries
- Reduced runtime calculation overhead
- Efficient refresh cycles during data updates

The view structure uses a two-level approach:
1. Base materialized view with aggregated statistics
2. Derived view with calculated metrics

This separation allows efficient refreshing of the base data while keeping derived calculations performant. 

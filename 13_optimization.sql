-- PostgreSQL Database Optimizations for Read-Heavy NFL Stats Workload
-- This script configures table storage parameters, autovacuum settings,
-- and other optimizations to maximize read performance

------ TABLE STORAGE OPTIMIZATIONS ------

-- Optimize core tables for read performance (high fill-factor since data is rarely updated)
ALTER TABLE teams SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.4);
ALTER TABLE players SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.4);
ALTER TABLE games SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.4);

-- For partitioned play-by-play tables (historical seasons rarely change)
-- Set 100% fill factor for historical seasons (pre-2024)
DO $$
DECLARE
    partition_name TEXT;
    year_val INTEGER;
BEGIN
    FOR year_val IN 1999..2023 LOOP
        partition_name := 'plays_' || year_val;
        EXECUTE format('ALTER TABLE %s SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.5)', partition_name);
        
        partition_name := 'playstats_' || year_val;
        EXECUTE format('ALTER TABLE %s SET (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.5)', partition_name);
    END LOOP;
END $$;

-- Current season partition might have more updates, so slightly lower fill factor
ALTER TABLE plays_2024 SET (fillfactor = 95, autovacuum_vacuum_scale_factor = 0.2);
ALTER TABLE playstats_2024 SET (fillfactor = 95, autovacuum_vacuum_scale_factor = 0.2);

------ TABLE ACCESS METHOD AND STORAGE OPTIMIZATIONS ------

-- For historical data (older than 5 years), set to compress
-- This example compresses historical seasons prior to 2020
DO $$
DECLARE
    partition_name TEXT;
    year_val INTEGER;
BEGIN
    FOR year_val IN 1999..2019 LOOP
        -- Use TOAST compression settings for old partitions
        partition_name := 'plays_' || year_val;
        EXECUTE format('ALTER TABLE %s SET (toast_tuple_target = 8160)', partition_name);
        
        partition_name := 'playstats_' || year_val;
        EXECUTE format('ALTER TABLE %s SET (toast_tuple_target = 8160)', partition_name);
    END LOOP;
END $$;

------ STATISTICS COLLECTION OPTIMIZATIONS ------

-- Increase statistics gathering for frequently filtered columns
-- This helps the query planner make better decisions
ALTER TABLE plays ALTER COLUMN is_red_zone SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_leading SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_trailing SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_early_down SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_late_down SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_likely_pass SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN posteam SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN season SET STATISTICS 1000;

-- Increase statistics for player and team IDs which are common join columns
ALTER TABLE playstats ALTER COLUMN player_id SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN team SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN season SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN is_red_zone SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN is_leading SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN is_trailing SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN is_early_down SET STATISTICS 1000;
ALTER TABLE playstats ALTER COLUMN is_late_down SET STATISTICS 1000;
ALTER TABLE plays ALTER COLUMN is_likely_pass SET STATISTICS 1000;


------ CLUSTERING OPTIMIZATIONS ------

-- Cluster plays table by game_id and play_id
-- This physically organizes rows on disk for sequential access
DO $$
DECLARE
    partition_name TEXT;
    year_val INTEGER;
    idx_name TEXT;
BEGIN
    FOR year_val IN 1999..2024 LOOP
        partition_name := 'plays_' || year_val;
        idx_name := 'idx_plays_' || year_val || '_game_play';
        
        -- Create index if it doesn't exist
        EXECUTE format('
            CREATE INDEX IF NOT EXISTS %s ON %s(game_id, play_id)', 
            idx_name, partition_name);
        
        -- Cluster the table based on this index
        EXECUTE format('CLUSTER %s USING %s', 
            partition_name, idx_name);
    END LOOP;
END $$;

------ OPTIMIZATION PLAN 2.2: INDEX MAINTENANCE ------

-- Create function to identify and rebuild bloated indexes
CREATE OR REPLACE FUNCTION maintain_indexes()
RETURNS TABLE (index_name TEXT, table_name TEXT, action TEXT) AS $$
BEGIN
    -- Reindex bloated indexes
    RETURN QUERY
    WITH bloated_indexes AS (
        SELECT
            schemaname || '.' || indexrelname AS index_name,
            schemaname || '.' || relname AS table_name,
            pg_relation_size(indexrelid)::numeric / 
                (pg_stat_get_numscans(indexrelid) + 1) AS bloat_per_scan
        FROM pg_stat_user_indexes
        WHERE schemaname = 'nfl_stats'
    )
    SELECT 
        index_name, 
        table_name, 
        'REINDEX' AS action
    FROM bloated_indexes
    WHERE bloat_per_scan > 1000000  -- 1MB per scan threshold
    ORDER BY bloat_per_scan DESC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION maintain_indexes IS
'Identifies indexes that have become bloated and should be rebuilt.
Returns a table with index_name, table_name, and action columns.
Should be run periodically (weekly) to maintain optimal index performance.';

------ PARALLEL QUERY OPTIMIZATIONS ------

-- Create function to adjust parallel query settings for large analytical queries
CREATE OR REPLACE FUNCTION enable_parallel_analysis() 
RETURNS VOID AS $$
BEGIN
    -- Enable aggressive parallelism for analytical queries
    SET max_parallel_workers_per_gather = 4;
    SET parallel_tuple_cost = 0.01;
    SET parallel_setup_cost = 100;
    SET min_parallel_table_scan_size = '8MB';
    SET min_parallel_index_scan_size = '512kB';
END;
$$ LANGUAGE plpgsql;

-- Create function to return to normal settings
CREATE OR REPLACE FUNCTION disable_parallel_analysis() 
RETURNS VOID AS $$
BEGIN
    RESET max_parallel_workers_per_gather;
    RESET parallel_tuple_cost;
    RESET parallel_setup_cost;
    RESET min_parallel_table_scan_size;
    RESET min_parallel_index_scan_size;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION enable_parallel_analysis IS 
'Adjusts PostgreSQL settings to enable aggressive parallelism for large analytical queries.
Call this before running resource-intensive analytics.';

COMMENT ON FUNCTION disable_parallel_analysis IS 
'Resets PostgreSQL parallel query settings to default values.
Call this after completing resource-intensive analytics.';

------ OPTIMIZATION PLAN 2.1: MONITORING IMPLEMENTATION ------

-- Create a view to monitor table and index sizes
CREATE OR REPLACE VIEW table_sizes AS
SELECT
    schemaname || '.' || relname AS relation,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_size,
    pg_total_relation_size(relid) AS total_bytes
FROM 
    pg_catalog.pg_statio_user_tables
WHERE 
    schemaname = 'nfl_stats'
ORDER BY 
    pg_total_relation_size(relid) DESC;

-- Create a view to monitor index usage
CREATE OR REPLACE VIEW index_usage_stats AS
SELECT
    schemaname || '.' || relname AS table_name,
    indexrelname AS index_name,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'nfl_stats'
ORDER BY idx_scan DESC;

COMMENT ON VIEW index_usage_stats IS
'Tracks index usage statistics to identify unused or inefficient indexes.
Review this view monthly to identify indexes that can be removed or improved.
Low scan count with high size indicates an index that may not be worth maintaining.';

------ SYSTEM-LEVEL OPTIMIZATIONS ------

-- Optimize for SSD storage
ALTER SYSTEM SET random_page_cost = 1.1;

-- Enable parallel query
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 8;
ALTER SYSTEM SET parallel_tuple_cost = 0.1;
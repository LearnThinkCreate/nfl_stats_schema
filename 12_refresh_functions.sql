-- Refresh Functions for Materialized Views
-- These functions keep the materialized views up-to-date when data changes

-- Function to refresh all materialized views
-- Can be called after weekly updates during the season
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW player_game_stats;
END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION refresh_all_materialized_views IS 
'Refreshes all materialized views in the proper dependency order.
Call this function after loading new game data to update all statistics.';
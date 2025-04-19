-- Create playstats table
-- This table stores detailed play-level statistics with player attribution
-- It enables dynamic filtering when combined with the plays table
CREATE TABLE playstats (
    -- Core identifiers
    play_id FLOAT NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    stat_id INTEGER,                    -- Statistical event identifier
    
    -- Play details
    yards INTEGER,                      -- Yards gained/lost on play
    team VARCHAR(10),                   -- Team abbreviation
    player_name VARCHAR(100),           -- Player name
    player_id VARCHAR(50) NOT NULL DEFAULT 'TEAM',              -- Player ID (matches gsis_id in players table)
    
    -- JSON/formatted strings with additional stats
    more_stats TEXT,                    -- Additional stats in semicolon-delimited format
    team_stats TEXT,                    -- Team stats in semicolon-delimited format
    
    -- Team aggregates for the play/game
    team_play_air_yards INTEGER,        -- Air yards on this play
    team_game_targets INTEGER,          -- Team targets in this game
    team_game_air_yards INTEGER,        -- Team air yards in this game
    
    -- Play context
    off VARCHAR(10),                    -- Offensive team
    def VARCHAR(10),                    -- Defensive team
    special INTEGER,                    -- Special teams indicator
    season_type VARCHAR(10),            -- REG, POST, etc.
    
    -- Pre-computed binary flags for statistical events
    is_comp BOOLEAN,                    -- Pass completion
    is_att BOOLEAN,                     -- Pass attempt
    is_pass_td BOOLEAN,                 -- Passing touchdown
    air_yards_complete INTEGER,         -- Air yards on completions
    is_int BOOLEAN,                     -- Interception
    is_sack BOOLEAN,                    -- Sack
    is_air_yards BOOLEAN,               -- Air yards indicator
    qb_target BOOLEAN,                  -- QB targeted a reciever
    is_carry BOOLEAN,                   -- Rushing attempt
    is_rush_yards BOOLEAN,              -- Rushing yards
    is_rush_td BOOLEAN,                 -- Rushing touchdown
    is_rec BOOLEAN,                     -- Reception
    is_target BOOLEAN,                  -- Pass target
    is_rec_yards BOOLEAN,               -- Receiving yards
    is_rec_td BOOLEAN,                  -- Receiving touchdown
    is_yac BOOLEAN,                     -- Yards after catch
    is_pass_2pt BOOLEAN,                -- 2-point pass conversion
    is_rush_2pt BOOLEAN,                -- 2-point rush conversion
    is_rec_2pt BOOLEAN,                 -- 2-point reception conversion
    
    -- Complex flags combining multiple conditions
    has_fumble BOOLEAN,                 -- Fumble indicator
    has_fumble_lost BOOLEAN,            -- Lost fumble indicator
    has_rush_first_down BOOLEAN,        -- Rush first down indicator
    has_pass_first_down BOOLEAN,        -- Pass first down indicator
    is_sack_fumble BOOLEAN,             -- Sack resulting in fumble
    is_sack_fumble_lost BOOLEAN,        -- Sack resulting in lost fumble
    is_rush_fumble BOOLEAN,             -- Rush resulting in fumble
    is_rush_fumble_lost BOOLEAN,        -- Rush resulting in lost fumble
    is_rec_fumble BOOLEAN,              -- Reception resulting in fumble
    is_rec_fumble_lost BOOLEAN,         -- Reception resulting in lost fumble
    is_rush_first_down BOOLEAN,         -- Rush resulting in first down
    is_pass_first_down BOOLEAN,         -- Pass resulting in first down
    is_rec_first_down BOOLEAN,          -- Reception resulting in first down
    is_special_td BOOLEAN,              -- Special teams touchdown
    
    -- Calculated yards columns
    pass_yards INTEGER,                 -- Passing yards
    sack_yards INTEGER,                 -- Yards lost on sacks
    air_yards INTEGER,                  -- Air yards
    rush_yards INTEGER,                 -- Rushing yards
    rec_yards INTEGER,                  -- Receiving yards
    yac INTEGER,                        -- Yards after catch
    
    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Flags
    is_leading INTEGER,
    is_trailing INTEGER,
    is_red_zone INTEGER,
    is_early_down INTEGER,
    is_late_down INTEGER,
    is_likely_pass INTEGER,
    
    -- Primary key must include season since we're partitioning by it
    PRIMARY KEY (season, play_id, game_id, player_id, stat_id)
) PARTITION BY RANGE (season);

-- Create partitions for past seasons programmatically
-- DO $$
-- DECLARE
--     year_val INTEGER;
-- BEGIN
--     FOR year_val IN 1999..2023 LOOP
--         EXECUTE format('
--             CREATE TABLE playstats_%s PARTITION OF playstats
--             FOR VALUES FROM (%s) TO (%s)',
--             year_val, year_val, year_val + 1
--         );
--     END LOOP;
-- END $$;

-- -- Create partition for current season (2024)
-- CREATE TABLE playstats_2024 PARTITION OF playstats
-- FOR VALUES FROM (2024) TO (2025);

-- -- Create partition for next season
-- CREATE TABLE playstats_2025 PARTITION OF playstats
-- FOR VALUES FROM (2025) TO (2026);

-- Index on player_id: Supports filtering and joining with players
CREATE INDEX idx_playstats_player_id ON playstats (player_id);

-- Index on (play_id, game_id): Optimizes the join with filtered_plays
CREATE INDEX idx_playstats_play_game ON playstats (play_id, game_id);

-- Composite Index on (player_id, season, team, season_type): Aids grouping and filtering, potentially enabling index-only scans for playstats_agg
CREATE INDEX idx_playstats_grouping ON playstats (season, player_id, team, season_type);

-- Playstats / game relationship
CREATE INDEX idx_playstats_keys ON playstats (play_id, game_id, season);

-- Flag indexes
CREATE INDEX idx_playstats_is_leading ON playstats (is_leading);
CREATE INDEX idx_playstats_is_trailing ON playstats (is_trailing);
CREATE INDEX idx_playstats_is_red_zone ON playstats (is_red_zone);
CREATE INDEX idx_playstats_is_late_down ON playstats (is_late_down);
CREATE INDEX idx_playstats_is_likely_pass ON playstats (is_likely_pass);

-- Season type index
CREATE INDEX idx_playstats_season_type ON playstats (season_type);

-- -- Maintenance function to add new season partitions
CREATE OR REPLACE FUNCTION create_playstats_partition(p_season INTEGER)
RETURNS VOID AS $$
BEGIN
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS playstats_%s PARTITION OF playstats
        FOR VALUES FROM (%s) TO (%s)',
        p_season, p_season, p_season + 1
    );
END;
$$ LANGUAGE plpgsql;

-- -- Add flags to playstats
-- CREATE OR REPLACE FUNCTION update_playstats_from_plays_batch()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE playstats ps
--     SET is_leading = p.is_leading,
--         is_trailing = p.is_trailing,
--         is_red_zone = p.is_red_zone,
--         is_late_down = p.is_late_down,
--         is_likely_pass = p.is_likely_pass
--     FROM plays p
--     WHERE ps.play_id = p.play_id
--       AND ps.game_id = p.game_id
--       AND ps.season = p.season
--       AND (ps.play_id, ps.game_id, ps.season) IN (
--           SELECT play_id, game_id, season
--           FROM new_table
--       );
--     RETURN NULL;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER playstats_after_insert_batch
-- AFTER INSERT ON playstats
-- REFERENCING NEW TABLE AS new_table
-- FOR EACH STATEMENT EXECUTE FUNCTION update_playstats_from_plays_batch();
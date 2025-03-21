-- Snap Counts table
-- Stores NFL player snap count information per game
CREATE TABLE snap_counts (
    gsis_id VARCHAR(50) NOT NULL REFERENCES players(gsis_id), -- NFL GameStats and Information System player identifier
    game_id VARCHAR(50) NOT NULL REFERENCES games(game_id),   -- Unique identifier for each game
    season INTEGER NOT NULL,                                  -- NFL season year
    week INTEGER NOT NULL,                                    -- Week number of the NFL season
    offense_snaps INTEGER,                                      -- Number of offensive snaps played
    offense_pct FLOAT,                                        -- Percentage of offensive snaps played
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (gsis_id, game_id)                           -- Composite primary key
);

-- Index for player-based queries
CREATE INDEX idx_snap_counts_gsis_id ON snap_counts(gsis_id);

-- Index for player-season queries
CREATE INDEX idx_snap_counts_gsis_id_season ON snap_counts(gsis_id, season);

-- Note: No need to explicitly create an index for (gsis_id, game_id) as it's already indexed by the primary key

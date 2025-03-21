-- Play-by-Play fact table
-- Partitioned by season for better query performance
CREATE TABLE plays (
    play_id FLOAT NOT NULL,
    game_id VARCHAR(50) NOT NULL REFERENCES games(game_id),
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    season_type VARCHAR(10) NOT NULL,
    home_team VARCHAR(10) NOT NULL REFERENCES teams(team_abbr),
    away_team VARCHAR(10) NOT NULL REFERENCES teams(team_abbr),
    posteam VARCHAR(10) REFERENCES teams(team_abbr),  -- Team in possession
    defteam VARCHAR(10) REFERENCES teams(team_abbr),  -- Defensive team
    game_date DATE,
    qtr INTEGER,                          -- Quarter (1-4, 5+ for OT)
    game_seconds_remaining FLOAT,         -- Seconds remaining in game
    half_seconds_remaining FLOAT,         -- Seconds remaining in half
    time_of_day VARCHAR(24),              -- Time of day
    down INTEGER,                         -- Current down (1-4)
    ydstogo FLOAT,                        -- Yards to go for first down
    yardline_100 FLOAT,                   -- Distance from opponent's end zone (0-100)
    goal_to_go INTEGER,                   -- Binary: goal-to-go situation
    score_differential FLOAT,             -- Score difference (posteam - defteam)
    posteam_score FLOAT,                  -- Possession team score
    defteam_score FLOAT,                  -- Defensive team score
    total_home_score FLOAT,               -- Total home team score
    total_away_score FLOAT,               -- Total away team score
    play_type VARCHAR(30),                -- Type of play
    shotgun INTEGER,                      -- Binary: shotgun formation
    no_huddle INTEGER,                    -- Binary: no-huddle
    qb_dropback INTEGER,                  -- Binary: QB dropback
    qb_scramble INTEGER,                  -- Binary: QB scramble
    qb_kneel INTEGER,                     -- Binary: QB kneel
    qb_spike INTEGER,                     -- Binary: QB spike
    pass_length VARCHAR(10),              -- Pass length (short, deep)
    pass_location VARCHAR(10),            -- Pass location (left, middle, right)
    run_location VARCHAR(10),             -- Run location (left, middle, right)
    run_gap VARCHAR(10),                  -- Run gap (end, tackle, guard)
    
    -- Advanced metrics
    epa FLOAT,                            -- Expected Points Added
    qb_epa FLOAT,                         -- QB Expected Points Added
    wp FLOAT,                             -- Win Probability before play
    wpa FLOAT,                            -- Win Probability Added
    air_yards FLOAT,                      -- Air yards on pass
    yards_after_catch FLOAT,              -- Yards after catch
    cpoe FLOAT,                           -- Completion % Over Expected
    success INTEGER,                      -- Binary: play was successful
    xpass FLOAT,                          -- Pass expectation probability
    
    -- Play outcomes
    first_down_rush INTEGER,              -- Binary: first down by rush
    first_down_pass INTEGER,              -- Binary: first down by pass
    first_down INTEGER,                   -- Binary: first down
    rush_attempt INTEGER,                 -- Binary: rush attempt
    pass_attempt INTEGER,                 -- Binary: pass attempt
    complete_pass INTEGER,                -- Binary: complete pass
    incomplete_pass INTEGER,              -- Binary: incomplete pass
    sack INTEGER,                         -- Binary: sack
    touchdown INTEGER,                    -- Binary: touchdown
    interception INTEGER,                 -- Binary: interception
    fumble INTEGER,                       -- Binary: fumble
    fumble_lost INTEGER,                  -- Binary: fumble lost
    pass_touchdown FLOAT,                 -- Binary: pass touchdown
    rush_touchdown FLOAT,                 -- Binary: rush touchdown
    
    -- Player information
    passer_player_id VARCHAR(50) REFERENCES players(gsis_id),
    passer_player_name VARCHAR(100),
    passing_yards FLOAT,
    rusher_player_id VARCHAR(50) REFERENCES players(gsis_id),
    rusher_player_name VARCHAR(100),
    rushing_yards FLOAT,
    receiver_player_id VARCHAR(50) REFERENCES players(gsis_id),
    receiver_player_name VARCHAR(100),
    receiving_yards FLOAT,
    
    -- Game context information
    stadium VARCHAR(100),
    roof VARCHAR(30),
    surface VARCHAR(30),
    temp FLOAT,
    wind FLOAT,
    div_game INTEGER,                     -- Binary: divisional game
    special FLOAT,                        -- Binary: special teams play
    special_teams_play FLOAT,             -- Binary: special teams play
    
    -- Situation flags (pre-computed for fast filtering)
    is_trailing INTEGER,                  -- Binary: possession team trailing
    is_leading INTEGER,                   -- Binary: possession team leading
    is_red_zone INTEGER,                  -- Binary: play in red zone
    is_early_down INTEGER,                -- Binary: early down (1st or 2nd)
    is_late_down INTEGER,                 -- Binary: late down (3rd or 4th)
    is_likely_pass INTEGER,               -- Binary: likely pass situation
    is_dropback INTEGER,                  -- Binary: QB dropback
    is_success INTEGER,                   -- Binary: successful play
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Define compound primary key
    PRIMARY KEY (season, play_id, game_id)
) PARTITION BY RANGE (season);

-- Create partitions for all seasons using a function
DO $$
DECLARE
    year_val INTEGER;
BEGIN
    FOR year_val IN 1999..2023 LOOP
        EXECUTE format('
            CREATE TABLE plays_%s PARTITION OF plays
            FOR VALUES FROM (%s) TO (%s)',
            year_val, year_val, year_val + 1
        );
    END LOOP;
END $$;

-- Create partition for current season (2024)
CREATE TABLE plays_2024 PARTITION OF plays
FOR VALUES FROM (2024) TO (2025);

-- Create future partition for next season
CREATE TABLE plays_2025 PARTITION OF plays
FOR VALUES FROM (2025) TO (2026);

-- Basic indexes
CREATE INDEX idx_plays_season ON plays (season);

-- Index for game and play identification
CREATE INDEX idx_plays_game_play ON plays(game_id, play_id);

-- Player-based indexes
CREATE INDEX idx_plays_passer ON plays(passer_player_id) 
    WHERE passer_player_id IS NOT NULL;
CREATE INDEX idx_plays_rusher ON plays(rusher_player_id)
    WHERE rusher_player_id IS NOT NULL;
CREATE INDEX idx_plays_receiver ON plays(receiver_player_id)
    WHERE receiver_player_id IS NOT NULL;

-- Team-based indexes
CREATE INDEX idx_plays_posteam ON plays(season, week, posteam, season_type);
CREATE INDEX idx_plays_defteam ON plays(season, week, defteam, season_type);

-- Play type filtering
CREATE INDEX idx_plays_play_type ON plays(season, play_type);


-- Text search index for player names
CREATE INDEX idx_plays_player_names_trgm ON plays 
    USING GIN ((passer_player_name || ' ' || rusher_player_name || ' ' || receiver_player_name) gin_trgm_ops);

-- Partial Index for Passing Stats: Covers filtering and grouping in passing_stats
CREATE INDEX idx_plays_passing ON plays (season, posteam, passer_player_id)
INCLUDE (qb_epa, cpoe, success)
WHERE play_type IN ('pass', 'qb_spike') AND passer_player_id IS NOT NULL;

-- Partial Index for Rushing Stats: Covers filtering and grouping in rushing_stats
CREATE INDEX idx_plays_rushing ON plays (season, posteam, rusher_player_id)
INCLUDE (epa, success)
WHERE play_type IN ('run', 'qb_kneel') AND rusher_player_id IS NOT NULL;

-- Partial Index for Receiving Stats: Covers filtering and grouping in receiving_stats
CREATE INDEX idx_plays_receiving ON plays (season, posteam, receiver_player_id)
INCLUDE (epa, success)
WHERE receiver_player_id IS NOT NULL;

-- Partial Index for Scramble Stats: Covers filtering and grouping in scramble_stats
CREATE INDEX idx_plays_scrambles ON plays (season, posteam, rusher_player_id)
INCLUDE (qb_epa, success)
WHERE qb_scramble = 1 AND rusher_player_id IS NOT NULL;

-- Optional Indexes on Situational Flags: Add these if the flags are selective
CREATE INDEX idx_plays_is_leading ON plays (is_leading);
CREATE INDEX idx_plays_is_trailing ON plays (is_trailing);
CREATE INDEX idx_plays_is_red_zone ON plays (is_red_zone);
CREATE INDEX idx_plays_is_late_down ON plays (is_late_down);
CREATE INDEX idx_plays_is_likely_pass ON plays (is_likely_pass);

-- season type index
CREATE INDEX idx_plays_season_type ON plays (season_type);

-- For dropback_stats, the CASE expression complicates indexing
CREATE INDEX idx_plays_qb_dropback ON plays (qb_dropback);

-- Index for plays / game relationship
CREATE INDEX idx_plays_keys ON plays (play_id, game_id, season);

CREATE INDEX idx_plays_team_situation ON plays (season, posteam, is_leading, is_red_zone, is_late_down);

-- Create maintenance function to add new season partitions automatically
CREATE OR REPLACE FUNCTION create_season_partition(p_season INTEGER)
RETURNS VOID AS $$
BEGIN
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS plays_%s PARTITION OF plays
        FOR VALUES FROM (%s) TO (%s)',
        p_season, p_season, p_season + 1
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_season_partition IS 
'Creates a new partition for the specified season in the plays table.
Call this function before importing data for a new NFL season.';
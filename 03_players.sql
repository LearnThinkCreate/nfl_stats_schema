-- Players table
-- Stores NFL player information according to data dictionary
CREATE TABLE players (
    gsis_id VARCHAR(50) PRIMARY KEY,             -- NFL GameStats and Information System player identifier
    status VARCHAR(20),                          -- Current player status (e.g., Active, Inactive, Retired)
    display_name VARCHAR(100) NOT NULL,          -- Player's display name for public presentation
    first_name VARCHAR(50),                      -- Player's first name
    last_name VARCHAR(50),                       -- Player's last name
    esb_id VARCHAR(50),                          -- ESPN Sports Bureau player identifier
    birth_date DATE,                             -- Player's date of birth
    college_name VARCHAR(100),                   -- Name of college player attended
    position VARCHAR(5),                         -- Player's position (e.g., QB, RB, WR)
    jersey_number INTEGER,                         -- Player's jersey number
    height INTEGER,                                -- Player's height in inches
    weight INTEGER,                                -- Player's weight in pounds
    team_abbr VARCHAR(10) REFERENCES teams(team_abbr), -- Abbreviated code for player's team
    current_team_id VARCHAR(50),                 -- Identifier for player's current team
    entry_year INTEGER,                            -- Year player entered the NFL
    rookie_year INTEGER,                           -- Year of player's rookie season
    draft_club VARCHAR(10),                      -- Team that drafted the player
    college_conference VARCHAR(50),              -- College conference player competed in
    status_short_description VARCHAR(20),        -- Short description of player's status
    gsis_it_id INTEGER,                            -- Alternative GSIS player identifier
    short_name VARCHAR(50),                      -- Shortened version of player's name
    headshot VARCHAR(255),                       -- URL to player's headshot image
    draft_number INTEGER,                          -- Overall selection number in draft
    draftround INTEGER,                            -- Round player was selected in draft
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for player name searches with fuzzy matching
CREATE INDEX idx_players_display_name_trgm ON players USING GIN (display_name gin_trgm_ops);

-- Index for team-based player queries
CREATE INDEX idx_players_team ON players(team_abbr);

-- Index for position-based filtering
CREATE INDEX idx_players_position ON players(position);

-- Index for active players
CREATE INDEX idx_players_active ON players(status) WHERE status = 'Active';

-- Optimization Plan 3.1: Player Name Search Optimization
-- Case-insensitive search index for player names without altering the column
CREATE INDEX idx_players_name_search ON players(lower(display_name) varchar_pattern_ops);

-- Alternative collation index (optional - uncomment if needed after testing)
-- CREATE INDEX idx_players_name_collation ON players(display_name COLLATE "en-US-x-icu");
CREATE INDEX IF NOT EXISTS idx_players_name_collation ON players
(display_name COLLATE "en-US-x-icu");
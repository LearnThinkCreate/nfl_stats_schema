-- Games table
-- Stores NFL game information according to data dictionary
CREATE TABLE games (
    game_id VARCHAR(50) PRIMARY KEY,             -- Unique identifier for each game
    season INTEGER NOT NULL,                     -- NFL season year
    game_type VARCHAR(10) NOT NULL,              -- Type of game (e.g., regular season, playoff)
    week INTEGER NOT NULL,                       -- Week number of the NFL season
    gameday DATE NOT NULL,                       -- Date of the game
    weekday VARCHAR(10),                         -- Day of the week the game was played
    gametime TIME,                               -- Time the game started
    away_team VARCHAR(10) NOT NULL REFERENCES teams(team_abbr), -- Abbreviation of the away team
    away_score INTEGER,                          -- Points scored by the away team
    home_team VARCHAR(10) NOT NULL REFERENCES teams(team_abbr), -- Abbreviation of the home team
    home_score INTEGER,                          -- Points scored by the home team
    location VARCHAR(100),                       -- Location where the game was played
    result INTEGER,                              -- Game result indicator
    total INTEGER,                               -- Total points scored in the game
    overtime INTEGER,                            -- Binary indicator if the game went to overtime (1) or not (0)
    gsis INTEGER,                                -- NFL GameStats and Information System ID
    nfl_detail_id VARCHAR(50),                         -- NFL API detail identifier
    pfr VARCHAR(50),                             -- Pro Football Reference game identifier
    pff INTEGER,                                   -- Pro Football Focus game identifier
    espn INTEGER,                                -- ESPN game identifier
    ftn INTEGER,                                   -- Football Outsiders game identifier
    away_rest INTEGER,                           -- Days of rest for the away team before this game
    home_rest INTEGER,                           -- Days of rest for the home team before this game
    away_moneyline FLOAT,                        -- Moneyline odds for the away team
    home_moneyline FLOAT,                        -- Moneyline odds for the home team
    spread_line FLOAT,                           -- Point spread for the game
    away_spread_odds FLOAT,                      -- Odds for betting on the away team against the spread
    home_spread_odds FLOAT,                      -- Odds for betting on the home team against the spread
    total_line FLOAT,                            -- Over/under line for total points in the game
    under_odds FLOAT,                            -- Odds for betting under the total line
    over_odds FLOAT,                             -- Odds for betting over the total line
    div_game INTEGER,                            -- Binary indicator if the game is a division game (1) or not (0)
    roof VARCHAR(20),                            -- Stadium roof type (e.g., dome, open, retractable)
    surface VARCHAR(50),                         -- Type of playing surface
    temp INTEGER,                                  -- Temperature at game time (Fahrenheit)
    wind INTEGER,                                  -- Wind speed at game time (mph)
    away_qb_id VARCHAR(50) REFERENCES players(gsis_id), -- Identifier for the starting quarterback of the away team
    home_qb_id VARCHAR(50) REFERENCES players(gsis_id), -- Identifier for the starting quarterback of the home team
    away_qb_name VARCHAR(100),                   -- Name of the starting quarterback for the away team
    home_qb_name VARCHAR(100),                   -- Name of the starting quarterback for the home team
    away_coach VARCHAR(100),                     -- Name of the head coach for the away team
    home_coach VARCHAR(100),                     -- Name of the head coach for the home team
    stadium_id VARCHAR(50),                      -- Unique identifier for the stadium
    stadium VARCHAR(100),                        -- Name of the stadium
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for season/week filtering (most common query pattern)
CREATE INDEX idx_games_season_week ON games(season, week, game_type);

-- Index for team-based filtering
CREATE INDEX idx_games_teams ON games(home_team, away_team);

-- Index for date-based queries
CREATE INDEX idx_games_date ON games(gameday);

-- Game Type Index
CREATE INDEX idx_games_game_type ON games(game_type);

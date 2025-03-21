-- Teams table
-- Stores NFL team information according to data dictionary
CREATE TABLE teams (
    team_abbr VARCHAR(10) PRIMARY KEY,           -- Abbreviated team code (e.g., ARI, ATL, BAL)
    team_name VARCHAR(100) NOT NULL,             -- Full team name (e.g., Arizona Cardinals)
    team_id INTEGER,                             -- Unique numeric identifier for the team
    team_nick VARCHAR(50),                       -- Team nickname (e.g., Cardinals, Falcons)
    team_conf VARCHAR(5),                        -- Conference affiliation (AFC or NFC)
    team_division VARCHAR(10),                   -- Division within conference (North, South, East, West)
    team_color VARCHAR(7),                       -- Primary team color (hexadecimal code)
    team_color2 VARCHAR(7),                      -- Secondary team color (hexadecimal code)
    team_color3 VARCHAR(7),                      -- Tertiary team color (hexadecimal code)
    team_color4 VARCHAR(7),                      -- Quaternary team color (hexadecimal code)
    team_logo_wikipedia VARCHAR(255),            -- URL to the team's logo on Wikipedia
    team_logo_espn VARCHAR(255),                 -- URL to the team's logo on ESPN
    team_wordmark VARCHAR(255),                  -- URL to the team's wordmark logo
    team_conference_logo VARCHAR(255),           -- URL to the team's conference logo
    team_league_logo VARCHAR(255),               -- URL to the NFL league logo
    team_logo_squared VARCHAR(255),              -- URL to a squared version of the team logo
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for team name searches with fuzzy matching
CREATE INDEX idx_teams_name_trgm ON teams USING GIN (team_name gin_trgm_ops);

-- Index for conference/division filtering
CREATE INDEX idx_teams_conference_division ON teams(team_conf, team_division);

-- Create Player Game Stats Materialized View (Optimized)
-- This provides comprehensive game-level player statistics
-- Uses pre-computed team_game_targets and team_game_air_yards from the playstats table

CREATE MATERIALIZED VIEW player_game_stats AS
WITH playstats_agg AS (
    -- Aggregate playstats at player-game level (not play level)
    SELECT
        ps.player_id,
        ps.game_id,
        ps.season,
        ps.week,
        ps.team,
		MAX(pl.position) AS position,
        COUNT(DISTINCT ps.play_id) AS total_plays,
        
        -- Get SUM of all binary flags as in the Python code
        SUM(CASE WHEN ps.is_comp THEN 1 ELSE 0 END) AS completions,
        SUM(CASE WHEN ps.is_att THEN 1 ELSE 0 END) AS attempts,
        SUM(CASE WHEN ps.is_pass_td THEN 1 ELSE 0 END) AS passing_tds,
        SUM(CASE WHEN ps.is_int THEN 1 ELSE 0 END) AS interceptions,
        SUM(CASE WHEN ps.is_sack THEN 1 ELSE 0 END) AS sacks,
        SUM(CASE WHEN ps.is_sack_fumble THEN 1 ELSE 0 END) AS sack_fumbles,
        SUM(CASE WHEN ps.is_sack_fumble_lost THEN 1 ELSE 0 END) AS sack_fumbles_lost,
        SUM(CASE WHEN ps.is_pass_first_down THEN 1 ELSE 0 END) AS passing_first_downs,
        SUM(CASE WHEN ps.is_pass_2pt THEN 1 ELSE 0 END) AS passing_2pt_conversions,
        SUM(CASE WHEN ps.qb_target THEN 1 ELSE 0 END) AS qb_targets,
        
        -- Rushing stats
        SUM(CASE WHEN ps.is_carry THEN 1 ELSE 0 END) AS carries,
        SUM(CASE WHEN ps.is_rush_td THEN 1 ELSE 0 END) AS rushing_tds,
        SUM(CASE WHEN ps.is_rush_fumble THEN 1 ELSE 0 END) AS rushing_fumbles,
        SUM(CASE WHEN ps.is_rush_fumble_lost THEN 1 ELSE 0 END) AS rushing_fumbles_lost,
        SUM(CASE WHEN ps.is_rush_first_down THEN 1 ELSE 0 END) AS rushing_first_downs,
        SUM(CASE WHEN ps.is_rush_2pt THEN 1 ELSE 0 END) AS rushing_2pt_conversions,
        
        -- Receiving stats
        SUM(CASE WHEN ps.is_target THEN 1 ELSE 0 END) AS targets,
        SUM(CASE WHEN ps.is_rec THEN 1 ELSE 0 END) AS receptions,
        SUM(CASE WHEN ps.is_rec_td THEN 1 ELSE 0 END) AS receiving_tds,
        SUM(CASE WHEN ps.is_rec_fumble THEN 1 ELSE 0 END) AS receiving_fumbles,
        SUM(CASE WHEN ps.is_rec_fumble_lost THEN 1 ELSE 0 END) AS receiving_fumbles_lost,
        SUM(CASE WHEN ps.is_rec_first_down THEN 1 ELSE 0 END) AS receiving_first_downs,
        SUM(CASE WHEN ps.is_rec_2pt THEN 1 ELSE 0 END) AS receiving_2pt_conversions,
        
        -- Special teams
        SUM(CASE WHEN ps.is_special_td THEN 1 ELSE 0 END) AS special_teams_tds,
        
        -- Sum cumulative stats
        SUM(ps.pass_yards) AS passing_yards,
        SUM(ps.sack_yards) AS sack_yards,
        SUM(ps.air_yards) AS passing_air_yards,
        SUM(ps.air_yards_complete) AS qb_air_yards,
        SUM(ps.rush_yards) AS rushing_yards,
        SUM(ps.rec_yards) AS receiving_yards,
        SUM(ps.yac) AS receiving_yards_after_catch,
        
        -- Special handling for receiving air yards (as in Python code)
        SUM(CASE WHEN ps.is_target THEN ps.team_play_air_yards ELSE 0 END) AS receiving_air_yards,
        
        -- Team metrics (preserved for later calculations)
        MAX(ps.team_game_targets) AS team_game_targets,
        MAX(ps.team_game_air_yards) AS team_game_air_yards,
        
        -- Player name and other metadata
        MAX(ps.player_name) AS player_name,
        MAX(ps.season_type) AS season_type,
        
        -- Determine opponent team
        MAX(CASE 
            WHEN ps.team = ps.off THEN ps.def
            ELSE ps.off
        END) AS opponent_team
    FROM
        playstats ps
    JOIN
        (
            SELECT
                gsis_id,
                position
            FROM
                players
            WHERE
                position IN ('QB', 'RB', 'WR', 'TE')
        ) pl ON ps.player_id = pl.gsis_id
    WHERE 
        ps.player_id != 'TEAM'
    GROUP BY
        ps.player_id, ps.game_id, ps.season, ps.week, ps.team
),

-- CTEs directly from the plays table, matching the Python functions in pbp_off_stats.py

-- 1. Passing stats (matches get_passing_stats function)
passing_stats AS (
    SELECT
        p.season,
        p.week,
        p.game_id,
        p.posteam AS team,
        p.passer_player_id AS player_id,
        SUM(p.qb_epa) AS passing_epa,
        -- Use AVG with NULL handling to match Python's np.mean behavior
        AVG(NULLIF(p.cpoe, 'NaN')) AS passing_cpoe,
        SUM(NULLIF(p.cpoe, 'NaN')) AS total_cpoe,
        SUM(NULLIF(p.success::float, 'NaN')) AS successful_passes,
        COUNT(p.passer_player_id) AS passes
    FROM
        plays p
    WHERE
        p.play_type IN ('pass', 'qb_spike')
        AND p.passer_player_id IS NOT NULL
    GROUP BY
        p.season, p.week, p.game_id, p.posteam, p.passer_player_id
),

-- 2. Rushing stats (matches get_rushing_stats function)
rushing_stats AS (
    SELECT
        p.season,
        p.week,
        p.game_id,
        p.posteam AS team,
        p.rusher_player_id AS player_id,
        SUM(p.epa) AS rushing_epa,
        SUM(NULLIF(p.success::float, 'NaN')) AS successful_rushes
    FROM
        plays p
    WHERE
        p.play_type IN ('run', 'qb_kneel')
        AND p.rusher_player_id IS NOT NULL
    GROUP BY
        p.season, p.week, p.game_id, p.posteam, p.rusher_player_id
),

-- 3. Receiving stats (matches get_receiving_stats function)
receiving_stats AS (
    SELECT
        p.season,
        p.week,
        p.game_id,
        p.posteam AS team,
        p.receiver_player_id AS player_id,
        SUM(p.epa) AS receiving_epa,
        SUM(NULLIF(p.success::float, 'NaN')) AS successful_receptions
    FROM
        plays p
    WHERE
        p.receiver_player_id IS NOT NULL
    GROUP BY
        p.season, p.week, p.game_id, p.posteam, p.receiver_player_id
),

-- 4. Dropback stats (matches get_dropback_stats function)
dropback_stats AS (
    SELECT
        p.season,
        p.week,
        p.game_id,
        p.posteam AS team,
        -- Determine player_id based on scramble flag, exactly as in Python
        CASE WHEN p.qb_scramble = 1 THEN p.rusher_player_id ELSE p.passer_player_id END AS player_id,
        COUNT(*) AS dropbacks,
        SUM(p.qb_epa) AS dropback_epa,
        SUM(NULLIF(p.success::float, 'NaN')) AS successful_dropbacks
    FROM
        plays p
    WHERE
        p.qb_dropback = 1
        AND (
            (p.qb_scramble = 1 AND p.rusher_player_id IS NOT NULL) 
            OR 
            (p.passer_player_id IS NOT NULL)
        )
    GROUP BY
        p.season, p.week, p.game_id, p.posteam,
        CASE WHEN p.qb_scramble = 1 THEN p.rusher_player_id ELSE p.passer_player_id END
),

-- 5. Scramble stats (matches get_scramble_stats function)
scramble_stats AS (
    SELECT
        p.season,
        p.week,
        p.game_id,
        p.posteam AS team,
        p.rusher_player_id AS player_id,
        COUNT(*) AS scrambles,
        SUM(p.qb_epa) AS scramble_epa,
        SUM(NULLIF(p.success::float, 'NaN')) AS successful_scrambles
    FROM
        plays p
    WHERE
        p.qb_scramble = 1
        AND p.rusher_player_id IS NOT NULL
    GROUP BY
        p.season, p.week, p.game_id, p.posteam, p.rusher_player_id
)

-- Final query that merges all CTEs
SELECT
    -- Identifiers and metadata
    pa.game_id,
    pa.season,
    pa.week,
    pa.player_id,
    pa.player_name,
    pa.team,
    pa.season_type,
    pa.opponent_team,
    pa.position,
    pa.total_plays,
    
    -- Snap counts data
    COALESCE(sc.offense_snaps, 0) AS plays,
    COALESCE(ps.passes, 0) + pa.carries AS qb_plays,
    
    -- Passing stats
    pa.completions,
    pa.attempts,
    pa.passing_tds,
    pa.interceptions,
    pa.sacks,
    pa.passing_yards,
    pa.sack_yards,
    pa.passing_air_yards,
    pa.passing_first_downs,
    COALESCE(ps.passing_cpoe, 0) AS passing_cpoe,
    COALESCE(ps.passing_cpoe, 0) AS cpoe,
    COALESCE(ps.passes, 0) AS passes,
    -- QB-specific air yards stats for ADOT
    pa.qb_targets,
    
    -- Yards after catch for passing
    pa.qb_air_yards,
    
    -- Advanced passing stats  
    COALESCE(ps.passing_epa, 0) AS passing_epa,
    COALESCE(ps.successful_passes, 0) AS successful_passes,
    COALESCE(ps.total_cpoe, 0) AS total_cpoe,
    
    -- Dropback stats
    COALESCE(ds.dropbacks, 0) AS dropbacks,
    COALESCE(ds.dropback_epa, 0) AS dropback_epa,
    COALESCE(ds.successful_dropbacks, 0) AS successful_dropbacks,
    
    -- Scramble stats
    COALESCE(ss.scrambles, 0) AS scrambles,
    COALESCE(ss.scramble_epa, 0) AS scramble_epa,
    COALESCE(ss.successful_scrambles, 0) AS successful_scrambles,
    
    -- Rushing stats
    pa.carries,
    pa.rushing_tds,
    pa.rushing_yards,
    pa.rushing_first_downs,
    COALESCE(rs.rushing_epa, 0) AS rushing_epa,
    COALESCE(rs.successful_rushes, 0) AS successful_rushes,

    -- Receiving stats
    pa.targets,
    pa.receptions,
    pa.receiving_tds,
    pa.receiving_yards,
    pa.receiving_yards_after_catch,
    pa.receiving_first_downs,
    pa.receiving_air_yards,
    COALESCE(recs.receiving_epa, 0) AS receiving_epa,
    COALESCE(recs.successful_receptions, 0) AS successful_receptions,
    
    -- Ball protection stats
    pa.sack_fumbles,
    pa.sack_fumbles_lost,
    pa.rushing_fumbles,
    pa.rushing_fumbles_lost,
    pa.receiving_fumbles,
    pa.receiving_fumbles_lost,
    
    -- 2-point conversion stats
    pa.passing_2pt_conversions,
    pa.rushing_2pt_conversions,
    pa.receiving_2pt_conversions,
    
    -- Special teams
    pa.special_teams_tds,
    
    -- Team metrics for share calculations
    pa.team_game_targets,
    pa.team_game_air_yards,
    pa.carries + pa.receptions AS touches,
    pa.carries + pa.targets as opportunities,

    COALESCE(ps.passing_epa, 0) + COALESCE(rs.rushing_epa, 0) + COALESCE(recs.receiving_epa, 0) AS total_epa
FROM
    playstats_agg pa
LEFT JOIN
    snap_counts sc ON pa.player_id = sc.gsis_id AND pa.game_id = sc.game_id
LEFT JOIN
    passing_stats ps ON pa.player_id = ps.player_id AND pa.game_id = ps.game_id
LEFT JOIN
    rushing_stats rs ON pa.player_id = rs.player_id AND pa.game_id = rs.game_id
LEFT JOIN
    receiving_stats recs ON pa.player_id = recs.player_id AND pa.game_id = recs.game_id
LEFT JOIN
    dropback_stats ds ON pa.player_id = ds.player_id AND pa.game_id = ds.game_id
LEFT JOIN
    scramble_stats ss ON pa.player_id = ss.player_id AND pa.game_id = ss.game_id
WITH DATA;

-- Add derived metrics as a separate step
CREATE OR REPLACE VIEW player_game_stats_with_metrics AS
SELECT 
    pgs.*,
    
    -- Derived stats - Passing
    CASE 
        WHEN attempts > 0 THEN ROUND((completions::numeric / attempts) * 100, 1)
        ELSE NULL
    END AS completion_percentage,
    
    CASE 
        WHEN attempts > 0 THEN ROUND(passing_yards::numeric / attempts, 1)
        ELSE NULL
    END AS yards_per_attempt,
    
    CASE 
        WHEN dropbacks > 0 THEN ROUND((sacks::numeric / dropbacks) * 100, 1)
        ELSE NULL
    END AS sack_rate,
    
    CASE 
        WHEN dropbacks > 0 THEN ROUND((scrambles::numeric / dropbacks) * 100, 1)
        ELSE NULL
    END AS scramble_rate,
    
    -- ADOT metrics for QBs and receivers
    CASE 
        WHEN qb_targets > 0 THEN ROUND(qb_air_yards::numeric / qb_targets, 1)
        ELSE NULL
    END AS qb_adot,
    
    CASE 
        WHEN targets > 0 THEN ROUND(receiving_air_yards::numeric / targets, 1)
        ELSE NULL
    END AS receiver_adot,
    
    -- Derived stats - Rushing
    CASE 
        WHEN carries > 0 THEN ROUND(rushing_yards::numeric / carries, 1)
        ELSE NULL
    END AS yards_per_carry,
    
    -- Derived stats - Receiving
    CASE 
        WHEN targets > 0 THEN ROUND((receptions::numeric / targets) * 100, 1)
        ELSE NULL
    END AS catch_rate,
    
    CASE 
        WHEN receptions > 0 THEN ROUND(receiving_yards::numeric / receptions, 1)
        ELSE NULL
    END AS yards_per_reception,
    
    -- Air yards conversion rates
    CASE 
        WHEN passing_air_yards > 0 THEN ROUND(passing_yards::numeric / passing_air_yards, 3)
        ELSE NULL
    END AS pacr,
    
    CASE 
        WHEN receiving_air_yards > 0 THEN ROUND(receiving_yards::numeric / receiving_air_yards, 3)
        ELSE NULL
    END AS racr,
    
    -- Team share statistics
    CASE 
        WHEN team_game_targets > 0 THEN ROUND((targets::numeric / team_game_targets), 3)
        ELSE NULL
    END AS target_share,
    
    CASE 
        WHEN team_game_air_yards > 0 THEN ROUND((receiving_air_yards::numeric / team_game_air_yards), 3)
        ELSE NULL
    END AS air_yards_share,
    
    -- Weighted Opportunity Rating (WOPR)
    CASE 
        WHEN team_game_targets > 0 AND team_game_air_yards > 0 THEN 
            ROUND((1.5 * (targets::numeric / team_game_targets)) + 
                  (0.7 * (receiving_air_yards::numeric / team_game_air_yards)), 3)
        ELSE NULL
    END AS wopr,
    
    -- Success rates
    CASE 
        WHEN passes > 0 THEN ROUND((successful_passes::numeric / passes) * 100, 1)
        ELSE NULL
    END AS passing_success_rate,
    
    CASE 
        WHEN dropbacks > 0 THEN ROUND((successful_dropbacks::numeric / dropbacks) * 100, 1)
        ELSE NULL
    END AS dropback_success_rate,
    
    CASE 
        WHEN scrambles > 0 THEN ROUND((successful_scrambles::numeric / scrambles) * 100, 1)
        ELSE NULL
    END AS scramble_success_rate,
    
    CASE 
        WHEN carries > 0 THEN ROUND((successful_rushes::numeric / carries) * 100, 1)
        ELSE NULL
    END AS rushing_success_rate,

    -- Target Success Rate
    CASE 
        WHEN targets > 0 THEN ROUND((successful_receptions::numeric / targets) * 100, 1)
        ELSE NULL
    END AS target_success_rate,
    
    -- EPA per play metrics
    CASE 
        WHEN dropbacks > 0 THEN ROUND(dropback_epa::numeric / dropbacks, 2)
        ELSE NULL
    END AS epa_per_dropback,
    
    CASE 
        WHEN scrambles > 0 THEN ROUND(scramble_epa::numeric / scrambles, 2)
        ELSE NULL
    END AS epa_per_scramble,

    CASE 
        WHEN plays > 0 THEN ROUND((passing_epa + rushing_epa + receiving_epa)::numeric / plays, 2)
        ELSE NULL
    END AS epa_per_play,

    CASE 
        WHEN qb_plays > 0 THEN ROUND((passing_epa + rushing_epa)::numeric / qb_plays, 2)
        ELSE NULL
    END AS epa_per_qb_play,
    
    CASE 
        WHEN carries > 0 THEN ROUND(rushing_epa::numeric / carries, 2)
        ELSE NULL
    END AS epa_per_carry,
    
    CASE 
        WHEN targets > 0 THEN ROUND(receiving_epa::numeric / targets, 2)
        ELSE NULL
    END AS epa_per_target,
    
    -- Total offensive stats
    (passing_yards + rushing_yards + receiving_yards) AS total_yards,
    (passing_tds + rushing_tds + receiving_tds) AS total_touchdowns,

    case when
        carries > 0 THEN ROUND((rushing_tds::numeric / carries) * 100, 1)
        ELSE NULL
    END AS rush_td_percentage,
    case when
        targets > 0 THEN ROUND((receiving_tds::numeric / targets) * 100, 1)
        ELSE NULL
    END AS receiving_td_percentage,
    case when
        receptions > 0 then ROUND((receiving_epa::numeric / receptions), 2)
        ELSE NULL
    END AS epa_per_reception,
    case when
        attempts > 0 then
           ROUND(
                ((
                    ((completions::numeric / attempts) * 100 - 30) / 20 +
                    ((passing_yards::numeric / attempts) - 3) / 4 +
                    (passing_tds::numeric / attempts) * 20 +
                    2.375 - ((interceptions::numeric / attempts) * 25)
                ) / 6) * 100, 1
            )
        ELSE NULL
    END AS passer_rating,
    case when
        attempts > 0 then ROUND((passing_first_downs::numeric / attempts) * 100, 1)
        ELSE NULL
    END AS passing_first_down_rate,
    case when
        carries > 0 then ROUND((rushing_first_downs::numeric / carries) * 100, 1)
        ELSE NULL
    END AS rushing_first_down_rate,
    case when
        targets > 0 then ROUND((receiving_first_downs::numeric / targets) * 100, 1)
        ELSE NULL
    END AS receiving_first_down_rate,
    case when
        targets > 0 then ROUND(passing_yards::numeric / targets, 1)
        ELSE NULL
    END AS yards_per_target
FROM 
    player_game_stats pgs;

-- Create indexes for efficient querying
CREATE UNIQUE INDEX idx_player_game_stats_player_game 
ON player_game_stats(player_id, game_id);

CREATE INDEX idx_player_game_stats_season_week 
ON player_game_stats(season, week);

CREATE INDEX idx_player_game_stats_player_season 
ON player_game_stats(player_id, season);

CREATE INDEX idx_player_game_stats_team_season
ON player_game_stats(team, season);

CREATE INDEX idx_player_game_stats_position
ON player_game_stats(position);

-- Add composite indexes for optimizing aggregations (from optimization plan)
CREATE INDEX IF NOT EXISTS idx_player_game_stats_aggregation 
ON player_game_stats(player_id, season, season_type);

CREATE INDEX IF NOT EXISTS idx_player_game_stats_position_season 
ON player_game_stats(position, season, season_type);

-- Add indexes for position-based filtering (optimization plan 2.1)
-- Using only columns that exist in the base materialized view
CREATE INDEX IF NOT EXISTS idx_player_game_stats_qb_epa 
ON player_game_stats(position, season, team)
INCLUDE (dropback_epa, passing_yards, dropbacks)
WHERE position = 'QB';

CREATE INDEX IF NOT EXISTS idx_player_game_stats_rb_epa 
ON player_game_stats(position, season, team)
INCLUDE (rushing_epa, rushing_yards, carries)
WHERE position = 'RB';

CREATE INDEX IF NOT EXISTS idx_player_game_stats_wr_epa 
ON player_game_stats(position, season, team)
INCLUDE (receiving_epa, receiving_yards, targets)
WHERE position = 'WR';

CREATE INDEX IF NOT EXISTS idx_player_game_stats_te_epa 
ON player_game_stats(position, season, team)
INCLUDE (receiving_epa, receiving_yards, targets)
WHERE position = 'TE';

CREATE INDEX IF NOT EXISTS idx_player_game_stats_pos_team 
ON player_game_stats(position, team, season);

-- Comment on the view
COMMENT ON MATERIALIZED VIEW player_game_stats IS 
'Game-level player statistics for all offensive skill positions.
Aggregates raw data from plays and playstats tables.
Use player_game_stats_with_metrics for advanced metrics and derived stats.
Contains added indexes for optimizing season-level and career-level aggregations.';

COMMENT ON VIEW player_game_stats_with_metrics IS
'Complete player game statistics with all derived metrics.
Includes advanced metrics like CPOE, WOPR, target share, EPA/play, etc.
This view calculates all metrics from the raw materialized data.';
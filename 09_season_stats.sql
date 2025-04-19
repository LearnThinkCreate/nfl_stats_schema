-- Create Player Season Stats View (Optimized)
-- This provides comprehensive season-level player statistics
-- Uses pre-computed team share metrics for calculating target_share and air_yards_share
-- Converted from materialized view to regular view per optimization plan

CREATE VIEW player_season_stats AS
SELECT
    -- Identifiers
    player_id,
    MAX(player_name) AS player_name,
    season,
    season_type,  -- Keep season_type separate (REG vs POST)
    -- Use the team from the most recent game in the season
    (ARRAY_AGG(team ORDER BY week DESC))[1] AS team,
    MAX(position) AS position,
    
    -- Game counts - critical for per-game calculations
    COUNT(DISTINCT game_id) AS games_played,
    
    -- Snap counts
    SUM(plays) AS plays,
    SUM(qb_plays) AS qb_plays,
    
    -- Raw stat totals - Passing
    SUM(completions) AS completions,
    SUM(attempts) AS attempts,
    SUM(passing_yards) AS passing_yards,
    SUM(passing_tds) AS passing_tds,
    SUM(interceptions) AS interceptions,
    SUM(sacks) AS sacks,
    SUM(sack_yards) AS sack_yards,
    SUM(passing_air_yards) AS passing_air_yards,
    SUM(passing_yards) - SUM(qb_air_yards) AS passing_yards_after_catch,
    SUM(passes) AS passes,
    SUM(sack_fumbles) AS sack_fumbles,
    SUM(sack_fumbles_lost) AS sack_fumbles_lost,

    -- Raw stat totals - Rushing
    SUM(carries) AS carries,
    SUM(rushing_yards) AS rushing_yards,
    SUM(rushing_tds) AS rushing_tds,
    SUM(rushing_fumbles) AS rushing_fumbles,
    SUM(rushing_fumbles_lost) AS rushing_fumbles_lost,
    SUM(rushing_first_downs) AS rushing_first_downs,
    
    -- Raw stat totals - Receiving
    SUM(targets) AS targets,
    SUM(receptions) AS receptions,
    SUM(receiving_yards) AS receiving_yards,
    SUM(receiving_tds) AS receiving_tds,
    SUM(receiving_fumbles) AS receiving_fumbles,
    SUM(receiving_fumbles_lost) AS receiving_fumbles_lost,
    SUM(receiving_air_yards) AS receiving_air_yards,
    SUM(receiving_yards_after_catch) AS receiving_yards_after_catch,
    SUM(receiving_first_downs) AS receiving_first_downs,
    
    -- QB specific stats
    SUM(scrambles) AS scrambles,
    SUM(scramble_yards) AS scramble_yards,
    SUM(scramble_first_downs) AS scramble_first_downs,
    
    -- QB-specific air yards stats for ADOT calculation
    SUM(qb_targets) AS qb_targets,
    SUM(qb_air_yards) AS qb_air_yards,
    
    -- Advanced usage stats
    SUM(dropbacks) AS dropbacks,
    SUM(total_cpoe) AS total_cpoe,
    SUM(cpoe_count) as cpoe_count,

    -- Explosive stats
    SUM(explosive_passes) AS explosive_passes,
    SUM(explosive_runs) AS explosive_runs,
    SUM(explosive_receptions) AS explosive_receptions,
    SUM(explosive_scrambles) AS explosive_scrambles,
    
    -- Touches and opportunities totals
    SUM(touches) AS touches,
    SUM(opportunities) AS opportunities,
    
    -- First downs and success metrics
    SUM(passing_first_downs) AS passing_first_downs,
    SUM(successful_passes) AS successful_passes,
    SUM(successful_rushes) AS successful_rushes,
    SUM(successful_receptions) AS successful_receptions,
    SUM(successful_dropbacks) AS successful_dropbacks,
    SUM(successful_scrambles) AS successful_scrambles,
    
    -- EPA totals by play type
    SUM(total_epa) AS total_epa,
    SUM(passing_epa) AS passing_epa,
    SUM(rushing_epa) AS rushing_epa,
    SUM(receiving_epa) AS receiving_epa,
    SUM(dropback_epa) AS dropback_epa,
    SUM(scramble_epa) AS scramble_epa,
    
    -- Team totals for shares and rates calculations
    SUM(team_game_targets) AS team_season_targets,
    SUM(team_game_air_yards) AS team_season_air_yards
FROM
    player_game_stats
GROUP BY
    player_id,
    season,
    season_type;

-- Add derived metrics as a separate step
CREATE OR REPLACE VIEW player_season_stats_with_metrics AS
SELECT 
    ps.*,
    
    -- Per-game averages
    ROUND(completions::numeric / games_played, 1) AS completions_per_game,
    ROUND(attempts::numeric / games_played, 1) AS attempts_per_game,
    ROUND(passing_yards::numeric / games_played, 1) AS passing_yards_per_game,
    ROUND(carries::numeric / games_played, 1) AS carries_per_game,
    ROUND(rushing_yards::numeric / games_played, 1) AS rushing_yards_per_game,
    ROUND(targets::numeric / games_played, 1) AS targets_per_game,
    ROUND(receptions::numeric / games_played, 1) AS receptions_per_game,
    ROUND(receiving_yards::numeric / games_played, 1) AS receiving_yards_per_game,
    ROUND(touches::numeric / games_played, 1) AS touches_per_game,
    ROUND(opportunities::numeric / games_played, 1) AS opportunities_per_game,
    
    -- Passing efficiency metrics
    CASE 
        WHEN attempts > 0 THEN ROUND((completions::numeric / attempts) * 100, 1)
        ELSE NULL
    END AS completion_percentage,
    
    CASE 
        WHEN attempts > 0 THEN ROUND(passing_yards::numeric / attempts, 1)
        ELSE NULL
    END AS yards_per_attempt,
    
    CASE 
        WHEN completions > 0 THEN ROUND(passing_yards::numeric / completions, 1)
        ELSE NULL
    END AS yards_per_completion,
    
    CASE 
        WHEN attempts > 0 THEN ROUND((passing_tds::numeric / attempts) * 100, 1)
        ELSE NULL
    END AS td_percentage,
    
    CASE 
        WHEN attempts > 0 THEN ROUND((interceptions::numeric / attempts) * 100, 1)
        ELSE NULL
    END AS int_percentage,
    
    CASE 
        WHEN dropbacks > 0 THEN ROUND((sacks::numeric / dropbacks) * 100, 1)
        ELSE NULL
    END AS sack_rate,
    
    CASE 
        WHEN dropbacks > 0 THEN ROUND((scrambles::numeric / dropbacks) * 100, 1)
        ELSE NULL
    END AS scramble_rate,

    CASE WHEN cpoe_count > 0 THEN ROUND((total_cpoe::numeric / cpoe_count::numeric), 1) ELSE NULL END AS cpoe,
    
    -- ADOT metrics for QBs and receivers
    CASE 
        WHEN attempts > 0 THEN ROUND(passing_air_yards::numeric / attempts, 1)
        ELSE NULL
    END AS qb_adot,
    
    CASE 
        WHEN targets > 0 THEN ROUND(receiving_air_yards::numeric / targets, 1)
        ELSE NULL
    END AS receiver_adot,
    
    -- Rushing efficiency metrics
    CASE WHEN carries > 0 THEN ROUND(rushing_yards::numeric / carries, 1) ELSE NULL END AS yards_per_carry,
    
    CASE WHEN carries > 0 THEN ROUND(rushing_tds::numeric / carries * 100, 1) ELSE NULL END AS rush_td_percentage,
    
    -- Receiving efficiency metrics
    CASE WHEN targets > 0 THEN ROUND((receptions::numeric / targets) * 100, 1) ELSE NULL END AS catch_rate,
    
    CASE WHEN receptions > 0 THEN ROUND(receiving_yards::numeric / receptions, 1) ELSE NULL END AS yards_per_reception,
    
    CASE WHEN targets > 0 THEN ROUND(receiving_yards::numeric / targets, 1) ELSE NULL END AS yards_per_target,
    
    CASE WHEN targets > 0 THEN ROUND(receiving_tds::numeric / targets * 100, 1) ELSE NULL END AS receiving_td_percentage,
    
    -- Air yards conversion rates
    CASE WHEN passing_air_yards > 0 THEN ROUND(passing_yards::numeric / passing_air_yards, 3) ELSE NULL END AS pacr,
    
    CASE WHEN receiving_air_yards > 0 THEN ROUND(receiving_yards::numeric / receiving_air_yards, 3) ELSE NULL END AS racr,
    
    -- Team share statistics
    CASE 
        WHEN team_season_targets > 0 THEN ROUND((targets::numeric / team_season_targets), 3)
        ELSE NULL
    END AS target_share,
    
    CASE 
        WHEN team_season_air_yards > 0 THEN ROUND((receiving_air_yards::numeric / team_season_air_yards), 3)
        ELSE NULL
    END AS air_yards_share,
    
    -- Weighted Opportunity Rating (WOPR)
    CASE 
        WHEN team_season_targets > 0 AND team_season_air_yards > 0 THEN 
            ROUND((1.5 * (targets::numeric / team_season_targets)) + 
                  (0.7 * (receiving_air_yards::numeric / team_season_air_yards)), 3)
        ELSE NULL
    END AS wopr,
    
    -- Success rates
    CASE WHEN qb_plays > 0 THEN ROUND(((successful_passes::numeric + successful_rushes::numeric) / qb_plays) * 100, 1) ELSE NULL END AS success_rate,

    CASE WHEN passes > 0 THEN ROUND((successful_passes::numeric / passes) * 100, 1) ELSE NULL END AS passing_success_rate,
    
    CASE WHEN dropbacks > 0 THEN ROUND((successful_dropbacks::numeric / dropbacks) * 100, 1) ELSE NULL END AS dropback_success_rate,
    
    CASE WHEN scrambles > 0 THEN ROUND((successful_scrambles::numeric / scrambles) * 100, 1) ELSE NULL END AS scramble_success_rate,
    
    CASE WHEN carries > 0 THEN ROUND((successful_rushes::numeric / carries) * 100, 1) ELSE NULL END AS rushing_success_rate,

    CASE WHEN targets > 0 THEN ROUND((successful_receptions::numeric / targets) * 100, 1) ELSE NULL END AS target_success_rate,
    
    
    -- EPA per play metrics
    CASE WHEN dropbacks > 0 THEN ROUND(dropback_epa::numeric / dropbacks, 3) ELSE NULL END AS epa_per_dropback,
    
    CASE WHEN scrambles > 0 THEN ROUND(scramble_epa::numeric / scrambles, 3) ELSE NULL END AS epa_per_scramble,
    
    CASE WHEN carries > 0 THEN ROUND(rushing_epa::numeric / carries, 3) ELSE NULL END AS epa_per_carry,
    
    CASE WHEN targets > 0 THEN ROUND(receiving_epa::numeric / targets, 3) ELSE NULL END AS epa_per_target,
    
    CASE WHEN receptions > 0 THEN ROUND(receiving_epa::numeric / receptions, 3) ELSE NULL END AS epa_per_reception,
    
    CASE WHEN plays > 0 THEN ROUND(total_epa::numeric / plays, 3) ELSE NULL END AS epa_per_play,

    CASE WHEN qb_plays > 0 THEN ROUND((passing_epa + rushing_epa)::numeric / qb_plays, 3) ELSE NULL END AS epa_per_qb_play,

    CASE WHEN passes > 0 THEN ROUND(passing_epa::numeric / passes, 3) ELSE NULL END AS epa_per_pass,
    
    -- Total offensive stats
    (passing_yards + rushing_yards + receiving_yards) AS total_yards,
    (passing_tds + rushing_tds + receiving_tds) AS total_touchdowns,
    ROUND((passing_yards + rushing_yards + receiving_yards)::numeric / games_played, 1) AS yards_per_game,
    
    -- QB passer rating (traditional formula)
    CASE 
        WHEN attempts >= 1 THEN
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
    
    -- First down rates
    CASE WHEN attempts > 0 THEN ROUND((passing_first_downs::numeric / attempts) * 100, 1) ELSE NULL END AS passing_first_down_rate,
    
    CASE WHEN carries > 0 THEN ROUND((rushing_first_downs::numeric / carries) * 100, 1) ELSE NULL END AS rushing_first_down_rate,
    
    CASE WHEN targets > 0 THEN ROUND((receiving_first_downs::numeric / targets) * 100, 1) ELSE NULL END AS receiving_first_down_rate,

    CASE WHEN scrambles > 0 THEN ROUND((scramble_first_downs::numeric / scrambles) * 100, 1) ELSE NULL END AS scramble_first_down_rate,

    -- explosive rates
    CASE WHEN passes > 0 THEN ROUND((explosive_passes::numeric / passes) * 100, 1) ELSE NULL END AS explosive_pass_rate,

    CASE WHEN carries > 0 THEN ROUND((explosive_runs::numeric / carries) * 100, 1) ELSE NULL END AS explosive_run_rate,

    CASE WHEN targets > 0 THEN ROUND((explosive_receptions::numeric / targets) * 100, 1) ELSE NULL END AS explosive_target_rate,

    CASE WHEN receptions > 0 THEN ROUND((explosive_receptions::numeric / receptions) * 100, 1) ELSE NULL END AS explosive_catch_rate,

    CASE WHEN scrambles > 0 THEN ROUND((explosive_scrambles::numeric / scrambles) * 100, 1) ELSE NULL END AS explosive_scramble_rate
    
    

FROM 
    player_season_stats ps;

-- Comment on the views
COMMENT ON VIEW player_season_stats IS 
'Season-level player statistics, separated by regular season and postseason.
Includes games_played count and all raw statistics.
Use player_season_stats_with_metrics for advanced metrics and derived statistics.
Converted from materialized view to regular view for optimization.';

COMMENT ON VIEW player_season_stats_with_metrics IS
'Complete player season statistics with all derived metrics.
Includes per-game averages, efficiency metrics, EPA metrics, and success rates.
Maintains separation between regular season and postseason stats.';
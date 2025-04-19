CREATE OR REPLACE FUNCTION get_filtered_season_stats(
    p_season INTEGER,
    p_team VARCHAR(10) DEFAULT NULL,
    p_is_leading INTEGER DEFAULT NULL,
    p_is_trailing INTEGER DEFAULT NULL,
    p_is_red_zone INTEGER DEFAULT NULL,
    p_is_late_down INTEGER DEFAULT NULL,
    p_is_likely_pass INTEGER DEFAULT NULL,
    p_season_type VARCHAR(10) DEFAULT NULL,
    p_player_id VARCHAR(50) DEFAULT NULL,
    p_position VARCHAR(10) DEFAULT NULL,
    p_sort_column VARCHAR(50) DEFAULT 'total_plays',
    p_sort_direction VARCHAR(4) DEFAULT 'DESC',
    p_limit INTEGER DEFAULT 32,
    p_offset INTEGER DEFAULT 0,
    p_min_plays INTEGER DEFAULT 1,
    p_min_attempts INTEGER DEFAULT 0,
    p_min_receptions INTEGER DEFAULT 0,
    p_min_carries INTEGER DEFAULT 0,
    p_remove_zero_rows BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    -- Keep all the original columns
    player_id VARCHAR(50),
    player_name VARCHAR(100),
    "position" VARCHAR(10),
    team VARCHAR(10),
    season INTEGER,
    season_type VARCHAR(10),
    total_plays BIGINT,
    games_played BIGINT,
    completions BIGINT,
    attempts BIGINT,
    passing_tds BIGINT,
    interceptions BIGINT,
    sacks BIGINT,
    passing_yards BIGINT,
    sack_yards BIGINT,
    passing_air_yards BIGINT,
    passing_first_downs BIGINT,
    passing_cpoe FLOAT,
    passes BIGINT,
    qb_targets BIGINT,
    qb_air_yards BIGINT,
    passing_epa FLOAT,
    successful_passes FLOAT,
    dropbacks BIGINT,
    dropback_epa FLOAT,
    successful_dropbacks FLOAT,
    scrambles BIGINT,
    scramble_epa FLOAT,
    successful_scrambles FLOAT,
    carries BIGINT,
    rushing_tds BIGINT,
    rushing_yards BIGINT,
    rushing_first_downs BIGINT,
    rushing_epa FLOAT,
    successful_rushes FLOAT,
    targets BIGINT,
    receptions BIGINT,
    receiving_tds BIGINT,
    receiving_yards BIGINT,
    receiving_yards_after_catch BIGINT,
    receiving_first_downs BIGINT,
    receiving_air_yards BIGINT,
    receiving_epa FLOAT,
    sack_fumbles BIGINT,
    sack_fumbles_lost BIGINT,
    rushing_fumbles BIGINT,
    rushing_fumbles_lost BIGINT,
    receiving_fumbles BIGINT,
    receiving_fumbles_lost BIGINT,
    passing_2pt_conversions BIGINT,
    rushing_2pt_conversions BIGINT,
    receiving_2pt_conversions BIGINT,
    special_teams_tds BIGINT,
    team_season_targets BIGINT,
    team_season_air_yards BIGINT
) AS $$
DECLARE
    valid_columns TEXT[] := ARRAY[
        'player_id', 'player_name', 'team', 'season', 'season_type', 'player_position',
        'total_plays', 'games_played', 'completions', 'attempts', 'passing_tds'
        /* all other columns from your original function */
    ];
    sql_query TEXT;
    columns_list TEXT;
    selected_columns TEXT[];
BEGIN
    -- Validate sort column
    IF p_sort_column NOT IN (
        'player_id', 'player_name', 'team', 'season', 'season_type', 'player_position',
        'total_plays', 'games_played', 'completions', 'attempts', 'passing_tds',
        'interceptions', 'sacks', 'passing_yards', 'sack_yards', 'passing_air_yards',
        'passing_first_downs', 'passing_cpoe', 'passes', 'qb_targets', 'qb_air_yards',
        'passing_epa', 'successful_passes', 'dropbacks', 'dropback_epa', 'successful_dropbacks',
        'scrambles', 'scramble_epa', 'successful_scrambles', 'carries', 'rushing_tds',
        'rushing_yards', 'rushing_first_downs', 'rushing_epa', 'successful_rushes',
        'targets', 'receptions', 'receiving_tds', 'receiving_yards', 'receiving_yards_after_catch',
        'receiving_first_downs', 'receiving_air_yards', 'receiving_epa',
        'sack_fumbles', 'sack_fumbles_lost', 'rushing_fumbles', 'rushing_fumbles_lost',
        'receiving_fumbles', 'receiving_fumbles_lost',
        'passing_2pt_conversions', 'rushing_2pt_conversions', 'receiving_2pt_conversions',
        'special_teams_tds', 'team_season_targets', 'team_season_air_yards'
    ) THEN
        p_sort_column := 'total_plays'; -- Default if invalid
    END IF;
    
    IF UPPER(p_sort_direction) NOT IN ('ASC', 'DESC') THEN
        p_sort_direction := 'DESC'; -- Default if invalid
    ELSE
        p_sort_direction := UPPER(p_sort_direction);
    END IF;


    -- Construct the core query with all CTEs from your original function
   RETURN QUERY EXECUTE '
    WITH filtered_plays AS (
        SELECT *
        FROM plays p
        WHERE p.season = $1
        AND ($2 IS NULL OR p.posteam = $2)
        AND ($3 IS NULL OR p.is_leading = $3)
        AND ($4 IS NULL OR p.is_trailing = $4)
        AND ($5 IS NULL OR p.is_red_zone = $5)
        AND ($6 IS NULL OR p.is_late_down = $6)
        AND ($7 IS NULL OR p.is_likely_pass = $7)
        AND ($8 IS NULL OR p.season_type = $8)
    ),
    playstats_agg AS (
        SELECT
            ps.player_id,
            ps.season,
            ps.team,
            MAX(pl.position)::VARCHAR(10) AS position,
            COUNT(DISTINCT ps.play_id) AS total_plays,
            COUNT(DISTINCT ps.game_id) AS games_played,

            -- Passing stats
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

            -- Cumulative stats
            SUM(ps.pass_yards) AS passing_yards,
            SUM(ps.sack_yards) AS sack_yards,
            SUM(ps.air_yards) AS passing_air_yards,
            SUM(ps.air_yards_complete) AS qb_air_yards,
            SUM(ps.rush_yards) AS rushing_yards,
            SUM(ps.rec_yards) AS receiving_yards,
            SUM(ps.yac) AS receiving_yards_after_catch,
            SUM(CASE WHEN ps.is_target THEN ps.team_play_air_yards ELSE 0 END) AS receiving_air_yards,

            -- Team metrics (season-level)
            SUM(ps.team_game_targets) AS team_season_targets,
            SUM(ps.team_game_air_yards) AS team_season_air_yards,

            -- Metadata
            MAX(ps.player_name)::VARCHAR(100) AS player_name,
            MAX(ps.season_type)::VARCHAR(10) AS season_type
        FROM playstats ps
        JOIN (
            SELECT gsis_id, position
            FROM players
            WHERE 
                case when
                    $10 IS NULL then
                    position IN (''QB'', ''RB'', ''WR'', ''TE'')
                else
                    position = $10
                end
                and (
                    $9 IS NULL
                    or players.gsis_id = $9
                )
        ) pl ON ps.player_id = pl.gsis_id
        WHERE 
            ps.player_id != ''TEAM''
            and ps.season = $1
            AND ($2 IS NULL OR ps.team = $2)
            AND ($3 IS NULL OR ps.is_leading = $3)
            AND ($4 IS NULL OR ps.is_trailing = $4)
            AND ($5 IS NULL OR ps.is_red_zone = $5)
            AND ($6 IS NULL OR ps.is_late_down = $6)
            AND ($7 IS NULL OR ps.is_likely_pass = $7)
            AND ($8 IS NULL OR ps.season_type = $8)
            
        GROUP BY ps.season, ps.player_id, ps.team

        HAVING
            CASE WHEN $11 IS NOT NULL THEN COUNT(DISTINCT ps.play_id) >= $11 ELSE TRUE END
            AND CASE WHEN $12 IS NOT NULL THEN SUM(CASE WHEN ps.is_att THEN 1 ELSE 0 END) >= $12 ELSE TRUE END
            AND CASE WHEN $13 IS NOT NULL THEN SUM(CASE WHEN ps.is_rec THEN 1 ELSE 0 END) >= $13 ELSE TRUE END
            AND CASE WHEN $14 IS NOT NULL THEN SUM(CASE WHEN ps.is_carry THEN 1 ELSE 0 END) >= $14 ELSE TRUE END
    ),
    passing_stats AS (
        SELECT
            p.season,
            p.posteam AS team,
            p.passer_player_id AS player_id,
            SUM(p.qb_epa) AS passing_epa,
            AVG(NULLIF(p.cpoe, ''NaN'')) AS passing_cpoe,
            SUM(NULLIF(p.success::float, ''NaN'')) AS successful_passes,
            COUNT(p.passer_player_id) AS passes
        FROM filtered_plays p
        WHERE p.play_type IN (''pass'', ''qb_spike'')
          AND p.passer_player_id IS NOT NULL
        GROUP BY p.season, p.posteam, p.passer_player_id
    ),
    rushing_stats AS (
        SELECT
            p.season,
            p.posteam AS team,
            p.rusher_player_id AS player_id,
            SUM(p.epa) AS rushing_epa,
            SUM(NULLIF(p.success::float, ''NaN'')) AS successful_rushes
        FROM filtered_plays p
        WHERE p.play_type IN (''run'', ''qb_kneel'')
          AND p.rusher_player_id IS NOT NULL
        GROUP BY p.season, p.posteam, p.rusher_player_id
    ),
    receiving_stats AS (
        SELECT
            p.season,
            p.posteam AS team,
            p.receiver_player_id AS player_id,
            SUM(p.epa) AS receiving_epa
        FROM filtered_plays p
        WHERE p.receiver_player_id IS NOT NULL
        GROUP BY p.season, p.posteam, p.receiver_player_id
    ),
    dropback_stats AS (
        SELECT
            p.season,
            p.posteam AS team,
            CASE WHEN p.qb_scramble = 1 THEN p.rusher_player_id ELSE p.passer_player_id END AS player_id,
            COUNT(*) AS dropbacks,
            SUM(p.qb_epa) AS dropback_epa,
            SUM(NULLIF(p.success::float, ''NaN'')) AS successful_dropbacks
        FROM filtered_plays p
        WHERE p.qb_dropback = 1
          AND (
              (p.qb_scramble = 1 AND p.rusher_player_id IS NOT NULL)
              OR (p.passer_player_id IS NOT NULL)
          )
        GROUP BY p.season, p.posteam,
                 CASE WHEN p.qb_scramble = 1 THEN p.rusher_player_id ELSE p.passer_player_id END
    ),
    scramble_stats AS (
        SELECT
            p.season,
            p.posteam AS team,
            p.rusher_player_id AS player_id,
            COUNT(*) AS scrambles,
            SUM(p.qb_epa) AS scramble_epa,
            SUM(NULLIF(p.success::float, ''NaN'')) AS successful_scrambles
        FROM filtered_plays p
        WHERE p.qb_scramble = 1
          AND p.rusher_player_id IS NOT NULL
        GROUP BY p.season, p.posteam, p.rusher_player_id
    )
    SELECT
        -- Identifiers and metadata
        pa.player_id,
        pa.player_name,
        pa.position,
        pa.team,
        pa.season,
        pa.season_type,
        
        pa.total_plays,
        pa.games_played,

        -- Passing stats
        COALESCE(pa.completions, 0) AS completions,
        COALESCE(pa.attempts, 0) AS attempts,
        COALESCE(pa.passing_tds, 0) AS passing_tds,
        COALESCE(pa.interceptions, 0) AS interceptions,
        COALESCE(pa.sacks, 0) AS sacks,
        COALESCE(pa.passing_yards, 0) AS passing_yards,
        COALESCE(pa.sack_yards, 0) AS sack_yards,
        COALESCE(pa.passing_air_yards, 0) AS passing_air_yards,
        COALESCE(pa.passing_first_downs, 0) AS passing_first_downs,
        COALESCE(ps.passing_cpoe, 0) AS passing_cpoe,
        COALESCE(ps.passes, 0) AS passes,
        COALESCE(pa.qb_targets, 0) AS qb_targets,
        COALESCE(pa.qb_air_yards, 0) AS qb_air_yards,
        COALESCE(ps.passing_epa, 0) AS passing_epa,
        COALESCE(ps.successful_passes, 0) AS successful_passes,

        -- Dropback stats
        COALESCE(ds.dropbacks, 0) AS dropbacks,
        COALESCE(ds.dropback_epa, 0) AS dropback_epa,
        COALESCE(ds.successful_dropbacks, 0) AS successful_dropbacks,

        -- Scramble stats
        COALESCE(ss.scrambles, 0) AS scrambles,
        COALESCE(ss.scramble_epa, 0) AS scramble_epa,
        COALESCE(ss.successful_scrambles, 0) AS successful_scrambles,

        -- Rushing stats
        COALESCE(pa.carries, 0) AS carries,
        COALESCE(pa.rushing_tds, 0) AS rushing_tds,
        COALESCE(pa.rushing_yards, 0) AS rushing_yards,
        COALESCE(pa.rushing_first_downs, 0) AS rushing_first_downs,
        COALESCE(rs.rushing_epa, 0) AS rushing_epa,
        COALESCE(rs.successful_rushes, 0) AS successful_rushes,

        -- Receiving stats
        COALESCE(pa.targets, 0) AS targets,
        COALESCE(pa.receptions, 0) AS receptions,
        COALESCE(pa.receiving_tds, 0) AS receiving_tds,
        COALESCE(pa.receiving_yards, 0) AS receiving_yards,
        COALESCE(pa.receiving_yards_after_catch, 0) AS receiving_yards_after_catch,
        COALESCE(pa.receiving_first_downs, 0) AS receiving_first_downs,
        COALESCE(pa.receiving_air_yards, 0) AS receiving_air_yards,
        COALESCE(recs.receiving_epa, 0) AS receiving_epa,

        -- Ball protection stats
        COALESCE(pa.sack_fumbles, 0) AS sack_fumbles,
        COALESCE(pa.sack_fumbles_lost, 0) AS sack_fumbles_lost,
        COALESCE(pa.rushing_fumbles, 0) AS rushing_fumbles,
        COALESCE(pa.rushing_fumbles_lost, 0) AS rushing_fumbles_lost,
        COALESCE(pa.receiving_fumbles, 0) AS receiving_fumbles,
        COALESCE(pa.receiving_fumbles_lost, 0) AS receiving_fumbles_lost,

        -- 2-point conversion stats
        COALESCE(pa.passing_2pt_conversions, 0) AS passing_2pt_conversions,
        COALESCE(pa.rushing_2pt_conversions, 0) AS rushing_2pt_conversions,
        COALESCE(pa.receiving_2pt_conversions, 0) AS receiving_2pt_conversions,

        -- Special teams stats
        COALESCE(pa.special_teams_tds, 0) AS special_teams_tds,

        -- Team metrics
        COALESCE(pa.team_season_targets, 0) AS team_season_targets,
        COALESCE(pa.team_season_air_yards, 0) AS team_season_air_yards
    FROM playstats_agg pa
    LEFT JOIN passing_stats ps ON pa.player_id = ps.player_id AND pa.season = ps.season AND pa.team = ps.team
    LEFT JOIN rushing_stats rs ON pa.player_id = rs.player_id AND pa.season = rs.season AND pa.team = rs.team
    LEFT JOIN receiving_stats recs ON pa.player_id = recs.player_id AND pa.season = recs.season AND pa.team = recs.team
    LEFT JOIN dropback_stats ds ON pa.player_id = ds.player_id AND pa.season = ds.season AND pa.team = ds.team
    LEFT JOIN scramble_stats ss ON pa.player_id = ss.player_id AND pa.season = ss.season AND pa.team = ss.team

    ' 
    || CASE WHEN p_remove_zero_rows THEN 'WHERE ' || p_sort_column || ' != 0' ELSE '' END

    || ' ORDER BY ' || quote_ident(p_sort_column) || ' ' || p_sort_direction || 
    CASE WHEN p_limit IS NOT NULL THEN ' LIMIT ' || p_limit::TEXT ELSE '' END || 
    ' OFFSET ' || p_offset::TEXT
    USING p_season, p_team, p_is_leading, p_is_trailing, p_is_red_zone, 
        p_is_late_down, p_is_likely_pass, p_season_type, p_player_id, p_position,
        p_min_plays, p_min_attempts, p_min_receptions, p_min_carries;
END;
$$ LANGUAGE plpgsql;
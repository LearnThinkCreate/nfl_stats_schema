CREATE MATERIALIZED VIEW player_relevance AS
WITH 
recent_szn as (
	SELECT max(season) as szn
	FROM games
	WHERE game_type = 'SB'	
), career_stats AS (
  SELECT 
    player_id,
    position,
    /* Career metrics: using seasons prior to the most recent Super Bowl season */
    log(GREATEST(SUM(total_yards), 0) + 1.0) * 0.125 * 
      CASE WHEN position = 'QB' THEN 0.9 ELSE 1 END AS career_l1,
    log(SUM(total_touchdowns) + 1.0) * 2.5 * 
      CASE WHEN position = 'QB' THEN 0.85 ELSE 1 END AS career_l2,
    log(GREATEST(SUM(total_epa), 0) + 1.0) * 2.0 AS career_l3,
    log(SUM(touches) + 1.0) * 1.5 AS career_l4
    /* Sum of the career components for an overall career score */
    -- (
    --   log(GREATEST(SUM(total_yards), 0) + 1.0) * 0.125 * CASE WHEN position = 'QB' THEN 0.9 ELSE 1 END +
    --   log(SUM(total_touchdowns) + 1.0) * 2.5 * CASE WHEN position = 'QB' THEN 0.85 ELSE 1 END +
    --   log(GREATEST(SUM(total_epa), 0) + 1.0) * 2.0 +
    --   log(SUM(touches) + 1.0) * 1.5
    -- ) AS career_score
  FROM player_season_stats_with_metrics
  WHERE season < (select szn from recent_szn)
  GROUP BY player_id, position
),
recent_stats AS (
  SELECT 
    player_id,
    position,
    /* Recent season metrics: using only the most recent Super Bowl season */
    log(GREATEST(SUM(total_yards), 0) + 1.0) * 0.75 * 
      CASE WHEN position = 'QB' THEN 0.9 ELSE 1 END AS recent_l1,
    log(SUM(total_touchdowns) + 1.0) * 1.25 * 
      CASE WHEN position = 'QB' THEN 0.85 ELSE 1 END AS recent_l2,
    log(GREATEST(SUM(total_epa), 0) + 1.0) * 1.0 AS recent_l3,
    log(SUM(touches) + 1.0) * 0.67 AS recent_l4
    /* Sum of the recent components for an overall recent season score */
    -- (
    --   log(GREATEST(SUM(total_yards), 0) + 1.0) * 0.75 * CASE WHEN position = 'QB' THEN 0.9 ELSE 1 END +
    --   log(SUM(total_touchdowns) + 1.0) * 1.25 * CASE WHEN position = 'QB' THEN 0.85 ELSE 1 END +
    --   log(GREATEST(SUM(total_epa), 0) + 1.0) * 1.0 +
    --   log(SUM(touches) + 1.0) * 0.67
    -- ) AS recent_score
  FROM player_season_stats_with_metrics
  WHERE season = (select szn from recent_szn)
  GROUP BY player_id, position
), composite_score AS (
  SELECT 
    p.gsis_id AS player_id,
    p.display_name,
	p.last_name,
    COALESCE(r.position, c.position) AS position,
    /* Composite score: weight the recent season performance more heavily than career stats */
    CASE 
		when 
			p.rookie_year > (select szn from recent_szn) and p.draftround = 1 then 15
		when p.rookie_year > (select szn from recent_szn) and p.draftround = 2 then 10
		when p.rookie_year > (select szn from recent_szn) then 5
		else 
			(
        
      1.5 * COALESCE(
        (r.recent_l1 + r.recent_l2 + r.recent_l3 + r.recent_l4),
        0
      ) + 1.0 
      
      
      
      * 
      
      
      COALESCE(c.career_l1 + c.career_l2 + c.career_l3 + c.career_l4, 0)
      
      )
			*
			case
				when rookie_year = (select szn from recent_szn) THEN 2
				else 1
			end
		end
	AS composite_score
    -- p.status,
    -- Optionally include individual component breakdowns for analysis or debugging
    -- r.recent_l1, r.recent_l2, r.recent_l3, r.recent_l4,
    -- c.career_l1, c.career_l2, c.career_l3, c.career_l4
  FROM players p
  LEFT JOIN recent_stats r ON p.gsis_id = r.player_id
  LEFT JOIN career_stats c ON p.gsis_id = c.player_id
  LEFT JOIN player_career_team pct ON p.gsis_id = pct.gsis_id
), normalized_composite_score AS (
	SELECT
		rs.*,
		-- Calculate Normalized Score (0 to 1 range)
		CASE
			WHEN (MAX(composite_score) OVER () - MIN(composite_score) OVER ()) > 0 THEN
				(composite_score - MIN(composite_score) OVER ()) / (MAX(composite_score) OVER () - MIN(composite_score) OVER ())
			ELSE 0.5 -- Assign neutral score if all scores are identical
		END AS normalized_composite_score
	FROM
	    composite_score rs
)
select *
from normalized_composite_score
where normalized_composite_score is not null and normalized_composite_score != 0
WITH DATA;

CREATE UNIQUE INDEX idx_player_relevance_player_id ON player_relevance(player_id);

CREATE INDEX idx_player_relevance_name_trgm ON player_relevance USING GIN (display_name gin_trgm_ops);

CREATE INDEX idx_player_relevance_position ON player_relevance(position);

CREATE INDEX idx_player_ranking_normalized_score ON player_relevance (normalized_composite_score DESC);

COMMENT ON MATERIALIZED VIEW player_relevance IS 'This view provides a composite score for each player based on their career and recent season performance';


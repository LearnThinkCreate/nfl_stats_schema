CREATE or replace VIEW player_career_team AS
WITH player_team_games AS (
  SELECT
    player_id,
    team,
    ROW_NUMBER() OVER (
      PARTITION BY player_id
      ORDER BY 
        COUNT(*) DESC,
        MAX(season) DESC,
        MAX(CASE WHEN season = (select max(season) from games) THEN week ELSE 0 END) DESC
    ) AS team_rank
  FROM
    player_game_stats
  GROUP BY
    player_id, team
), ranked_teams AS (
	SELECT
		player_id,
		team
	FROM
		player_team_games
	where 
		team_rank = 1

)
select 
	c.player_id as gsis_id,
	case
		when last_season < (select max(season) from games) then h.team
		else p.team_abbr
	end as team,
	last_season
from
	player_career_stats c
JOIN
	ranked_teams h on c.player_id = h.player_id
JOIN
	players p on c.player_id = p.gsis_id
where 
	c.season_type = 'REG';


comment on view player_career_team is 'This view provides the most played team for a player in their career for inactive players, current team for active players';
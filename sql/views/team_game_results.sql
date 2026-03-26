-- View: public.team_game_results

-- DROP VIEW public.team_game_results;

CREATE OR REPLACE VIEW public.team_game_results
 AS
 SELECT g.season,
    g.season_type,
    g.game_id,
    g.game_date,
    g.home_team_id AS team_id,
    g.away_team_id AS opp_team_id,
    1 AS is_home,
    g.home_points AS team_points,
    g.away_points AS opp_points,
        CASE
            WHEN g.home_points > g.away_points THEN 1
            ELSE 0
        END AS is_win
   FROM games g
  WHERE g.season_type = 'Regular Season'::text
UNION ALL
 SELECT g.season,
    g.season_type,
    g.game_id,
    g.game_date,
    g.away_team_id AS team_id,
    g.home_team_id AS opp_team_id,
    0 AS is_home,
    g.away_points AS team_points,
    g.home_points AS opp_points,
        CASE
            WHEN g.away_points > g.home_points THEN 1
            ELSE 0
        END AS is_win
   FROM games g
  WHERE g.season_type = 'Regular Season'::text;

ALTER TABLE public.team_game_results
    OWNER TO postgres;


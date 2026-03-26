-- View: public.team_game_features_rolling

-- DROP VIEW public.team_game_features_rolling;

CREATE OR REPLACE VIEW public.team_game_features_rolling
 AS
 SELECT b.season,
    b.game_id,
    b.game_date,
    b.team_id,
    b.opp_team_id,
    b.is_home,
    b.is_win,
    b.point_diff,
    b.fgm,
    b.fga,
    b.fg_pct,
    b.fg3m,
    b.fg3a,
    b.fg3_pct,
    b.ftm,
    b.fta,
    b.ft_pct,
    b.oreb,
    b.dreb,
    b.reb,
    b.ast,
    b.stl,
    b.blk,
    b.turnovers,
    b.pf,
    b.pts,
    b.plus_minus,
    avg(b.is_win) OVER w5 AS win_pct_l5,
    avg(b.is_win) OVER w10 AS win_pct_l10,
    avg(b.point_diff) OVER w10 AS avg_point_diff_l10,
    avg(b.pts) OVER w10 AS avg_pts_l10,
    avg(b.turnovers) OVER w10 AS avg_tov_l10,
    avg(b.reb) OVER w10 AS avg_reb_l10,
    avg(b.ast) OVER w10 AS avg_ast_l10,
    avg(b.is_win) OVER s2d AS win_pct_s2d,
    avg(b.point_diff) OVER s2d AS avg_point_diff_s2d
   FROM team_game_features_base b
  WINDOW w5 AS (PARTITION BY b.season, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w10 AS (PARTITION BY b.season, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), s2d AS (PARTITION BY b.season, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING);

ALTER TABLE public.team_game_features_rolling
    OWNER TO postgres;


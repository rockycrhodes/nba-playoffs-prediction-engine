-- View: public.team_game_features_base

-- DROP VIEW public.team_game_features_base;

CREATE OR REPLACE VIEW public.team_game_features_base
 AS
 SELECT r.season,
    r.game_id,
    r.game_date,
    r.team_id,
    r.opp_team_id,
    r.is_home,
    r.is_win,
    r.team_points - r.opp_points AS point_diff,
    tgs.fgm,
    tgs.fga,
    tgs.fg_pct,
    tgs.fg3m,
    tgs.fg3a,
    tgs.fg3_pct,
    tgs.ftm,
    tgs.fta,
    tgs.ft_pct,
    tgs.oreb,
    tgs.dreb,
    tgs.reb,
    tgs.ast,
    tgs.stl,
    tgs.blk,
    tgs.turnovers,
    tgs.pf,
    tgs.pts,
    tgs.plus_minus
   FROM team_game_results r
     JOIN team_game_stats tgs ON r.game_id::text = tgs.game_id::text AND r.team_id = tgs.team_id;

ALTER TABLE public.team_game_features_base
    OWNER TO postgres;


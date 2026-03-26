-- View: public.team_game_training_model

-- DROP VIEW public.team_game_training_model;

CREATE OR REPLACE VIEW public.team_game_training_model
 AS
 SELECT team_game_training.season,
    team_game_training.game_id,
    team_game_training.game_date,
    team_game_training.team_id,
    team_game_training.opp_team_id,
    team_game_training.is_home,
    team_game_training.is_win,
    team_game_training.point_diff,
    team_game_training.fgm,
    team_game_training.fga,
    team_game_training.fg_pct,
    team_game_training.fg3m,
    team_game_training.fg3a,
    team_game_training.fg3_pct,
    team_game_training.ftm,
    team_game_training.fta,
    team_game_training.ft_pct,
    team_game_training.oreb,
    team_game_training.dreb,
    team_game_training.reb,
    team_game_training.ast,
    team_game_training.stl,
    team_game_training.blk,
    team_game_training.turnovers,
    team_game_training.pf,
    team_game_training.pts,
    team_game_training.plus_minus,
    team_game_training.win_pct_l5,
    team_game_training.win_pct_l10,
    team_game_training.avg_point_diff_l10,
    team_game_training.avg_pts_l10,
    team_game_training.avg_tov_l10,
    team_game_training.avg_reb_l10,
    team_game_training.avg_ast_l10,
    team_game_training.win_pct_s2d,
    team_game_training.avg_point_diff_s2d,
    team_game_training.made_playoffs
   FROM team_game_training
  WHERE team_game_training.win_pct_l10 IS NOT NULL AND team_game_training.avg_point_diff_l10 IS NOT NULL;

ALTER TABLE public.team_game_training_model
    OWNER TO postgres;


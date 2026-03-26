-- Cross-table consistency checks

-- Each game should have exactly 2 team_game_stats rows (one per team)
SELECT g.game_id, COUNT(*) AS team_rows
FROM games g
JOIN team_game_stats tgs ON g.game_id = tgs.game_id
GROUP BY g.game_id
HAVING COUNT(*) <> 2;

-- Team points in team_game_stats should match games table home/away points
SELECT
  g.game_id,
  g.home_team_id,
  g.home_points,
  tgs_home.pts AS tgs_home_pts,
  g.away_team_id,
  g.away_points,
  tgs_away.pts AS tgs_away_pts
FROM games g
JOIN team_game_stats tgs_home
  ON g.game_id = tgs_home.game_id AND g.home_team_id = tgs_home.team_id
JOIN team_game_stats tgs_away
  ON g.game_id = tgs_away.game_id AND g.away_team_id = tgs_away.team_id
WHERE g.home_points <> tgs_home.pts
   OR g.away_points <> tgs_away.pts;

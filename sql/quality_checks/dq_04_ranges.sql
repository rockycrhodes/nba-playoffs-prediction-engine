-- Range / sanity checks
-- Expect: 0 rows returned

-- team shooting percentages should be between 0 and 1 (if stored as fraction)
SELECT *
FROM team_game_stats
WHERE (fg_pct < 0 OR fg_pct > 1)
   OR (fg3_pct < 0 OR fg3_pct > 1)
   OR (ft_pct < 0 OR ft_pct > 1);

-- player shooting percentages
SELECT *
FROM player_game_stats
WHERE (fg_pct < 0 OR fg_pct > 1)
   OR (fg3_pct < 0 OR fg3_pct > 1)
   OR (ft_pct < 0 OR ft_pct > 1);

-- non-negative counting stats (allow NULLs)
SELECT *
FROM team_game_stats
WHERE (pts < 0) OR (reb < 0) OR (ast < 0) OR (turnovers < 0);

SELECT *
FROM player_game_stats
WHERE (pts < 0) OR (reb < 0) OR (ast < 0) OR (turnovers < 0);

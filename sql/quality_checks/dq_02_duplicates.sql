-- Duplicate key checks
-- Expect: 0 rows returned for each query

-- games should be unique per game_id
SELECT game_id, COUNT(*)
FROM games
GROUP BY game_id
HAVING COUNT(*) > 1;

-- team_game_stats unique per (game_id, team_id)
SELECT game_id, team_id, COUNT(*)
FROM team_game_stats
GROUP BY game_id, team_id
HAVING COUNT(*) > 1;

-- player_game_stats unique per (game_id, player_id)
SELECT game_id, player_id, COUNT(*)
FROM player_game_stats
GROUP BY game_id, player_id
HAVING COUNT(*) > 1;

-- standings unique per (season_id, team_id, run_date) if you store snapshots
-- Adjust columns as needed based on your schema.
SELECT season_id, team_id, run_date, COUNT(*)
FROM standings
GROUP BY season_id, team_id, run_date
HAVING COUNT(*) > 1;











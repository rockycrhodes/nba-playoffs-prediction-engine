-- Orphan checks (foreign-key-like validation)
-- Expect: 0 rows returned for each query

-- player_game_stats -> games
SELECT DISTINCT p.game_id
FROM player_game_stats p
LEFT JOIN games g ON p.game_id = g.game_id
WHERE g.game_id IS NULL;

-- player_game_stats -> players
SELECT DISTINCT p.player_id
FROM player_game_stats p
LEFT JOIN players pl ON p.player_id = pl.player_id
WHERE pl.player_id IS NULL;

-- player_game_stats -> teams
SELECT DISTINCT p.team_id
FROM player_game_stats p
LEFT JOIN teams t ON p.team_id = t.team_id
WHERE t.team_id IS NULL;

-- team_game_stats -> games
SELECT DISTINCT tgs.game_id
FROM team_game_stats tgs
LEFT JOIN games g ON tgs.game_id = g.game_id
WHERE g.game_id IS NULL;

-- team_game_stats -> teams
SELECT DISTINCT tgs.team_id
FROM team_game_stats tgs
LEFT JOIN teams t ON tgs.team_id = t.team_id
WHERE t.team_id IS NULL;

-- games -> teams
SELECT g.game_id
FROM games g
LEFT JOIN teams th ON g.home_team_id = th.team_id
LEFT JOIN teams ta ON g.away_team_id = ta.team_id
WHERE th.team_id IS NULL OR ta.team_id IS NULL;










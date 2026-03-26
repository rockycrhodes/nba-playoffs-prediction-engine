-- Completeness / missing core fields
-- Expect: 0 rows for these (or very small counts you intentionally allow)

-- games core fields
SELECT *
FROM games
WHERE game_id IS NULL
   OR season IS NULL
   OR game_date IS NULL
   OR home_team_id IS NULL
   OR away_team_id IS NULL
   OR home_points IS NULL
   OR away_points IS NULL;

-- team stats core fields
SELECT *
FROM team_game_stats
WHERE game_id IS NULL OR team_id IS NULL OR pts IS NULL OR season IS NULL;

-- player stats core fields
SELECT *
FROM player_game_stats
WHERE game_id IS NULL OR player_id IS NULL OR team_id IS NULL OR season IS NULL;

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""etl_scheduleleaguev2_to_postgres.py

Ingest NBA season schedules (including future games) using nba_api endpoint ScheduleLeagueV2
into Postgres, and run completeness checks.

Assumptions
- Your team_id values match NBA Stats numeric IDs (e.g., 1610612747). If they don't, you must
  create a mapping table and translate.

Install
  pip install nba_api pandas sqlalchemy psycopg2-binary python-dateutil

Env vars (same pattern as your modeling scripts)
  NBA_DB_USER, NBA_DB_PASSWORD, NBA_DB_HOST, NBA_DB_PORT, NBA_DB_NAME

Usage
  python etl_scheduleleaguev2_to_postgres.py --seasons 2021-22 2022-23 2023-24 2024-25 2025-26

Outputs
- public.schedule_games_api
- public.schedule_weeks_api

Then prints validation summaries:
- number of games per season
- team games per season distribution
- duplicates / missing ids
- % of games with status FINAL vs scheduled

"""

import os
import time
import argparse
from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text


def get_engine():
    db_user = os.getenv("NBA_DB_USER", "postgres")
    db_password = os.getenv("NBA_DB_PASSWORD", "yourpassword")
    db_host = os.getenv("NBA_DB_HOST", "localhost")
    db_port = os.getenv("NBA_DB_PORT", "5432")
    db_name = os.getenv("NBA_DB_NAME", "nba_data")
    url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(url)


def fetch_scheduleleaguev2(season: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch schedule + weeks for a given season string (e.g., '2024-25')."""
    # nba_api can be finicky on headers; it sets reasonable defaults.
    from nba_api.stats.endpoints import scheduleleaguev2

    resp = scheduleleaguev2.ScheduleLeagueV2(season=season)
    dfs = resp.get_data_frames()

    # Typical ordering: [season_games, season_weeks]
    if len(dfs) < 2:
        raise RuntimeError(f"Expected 2 data frames from ScheduleLeagueV2, got {len(dfs)}")

    season_games = dfs[0].copy()
    season_weeks = dfs[1].copy()

    season_games["season"] = season
    season_weeks["season"] = season

    return season_games, season_weeks


def normalize_games(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize schema down to what's useful for ETL + modeling."""
    out = pd.DataFrame({
        "season": df["season"].astype(str),
        "season_year": df.get("seasonYear"),
        "league_id": df.get("leagueId"),
        "game_id": pd.to_numeric(df.get("gameId"), errors="coerce").astype("Int64"),
        "game_code": df.get("gameCode"),
        "game_status": pd.to_numeric(df.get("gameStatus"), errors="coerce").astype("Int64"),
        "game_status_text": df.get("gameStatusText"),
        # Prefer UTC datetime if present; fall back to date strings
        "game_date_utc": pd.to_datetime(df.get("gameDateUTC"), errors="coerce"),
        "game_datetime_utc": pd.to_datetime(df.get("gameDateTimeUTC"), errors="coerce"),
        "game_date_est": pd.to_datetime(df.get("gameDateEst"), errors="coerce"),
        "game_datetime_est": pd.to_datetime(df.get("gameDateTimeEst"), errors="coerce"),
        "home_team_id": pd.to_numeric(df.get("homeTeam_teamId"), errors="coerce").astype("Int64"),
        "away_team_id": pd.to_numeric(df.get("awayTeam_teamId"), errors="coerce").astype("Int64"),
        "home_team_tricode": df.get("homeTeam_teamTricode"),
        "away_team_tricode": df.get("awayTeam_teamTricode"),
        "arena_name": df.get("arenaName"),
        "arena_city": df.get("arenaCity"),
        "arena_state": df.get("arenaState"),
        "is_neutral": df.get("isNeutral"),
        "postponed_status": df.get("postponedStatus"),
    })

    # A convenient DATE column for SQL joins
    # Prefer EST date, else UTC date
    out["game_date"] = (
        out["game_date_est"].dt.date
        .fillna(pd.Series(out["game_date_utc"].dt.date))
    )

    return out


def normalize_weeks(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "season": df["season"].astype(str),
        "season_year": df.get("seasonYear"),
        "league_id": df.get("leagueId"),
        "week_number": pd.to_numeric(df.get("weekNumber"), errors="coerce").astype("Int64"),
        "week_name": df.get("weekName"),
        "start_date": pd.to_datetime(df.get("startDate"), errors="coerce").dt.date,
        "end_date": pd.to_datetime(df.get("endDate"), errors="coerce").dt.date,
    })
    return out


def ensure_tables(engine):
    ddl_games = """
    CREATE TABLE IF NOT EXISTS public.schedule_games_api (
        season TEXT NOT NULL,
        season_year TEXT NULL,
        league_id TEXT NULL,
        game_id BIGINT NULL,
        game_code TEXT NULL,
        game_status INT NULL,
        game_status_text TEXT NULL,
        game_date DATE NULL,
        game_date_utc TIMESTAMP NULL,
        game_datetime_utc TIMESTAMP NULL,
        game_date_est TIMESTAMP NULL,
        game_datetime_est TIMESTAMP NULL,
        home_team_id BIGINT NULL,
        away_team_id BIGINT NULL,
        home_team_tricode TEXT NULL,
        away_team_tricode TEXT NULL,
        arena_name TEXT NULL,
        arena_city TEXT NULL,
        arena_state TEXT NULL,
        is_neutral BOOLEAN NULL,
        postponed_status TEXT NULL,
        inserted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_schedule_games_api_season ON public.schedule_games_api(season);
    CREATE INDEX IF NOT EXISTS idx_schedule_games_api_game_id ON public.schedule_games_api(game_id);
    CREATE INDEX IF NOT EXISTS idx_schedule_games_api_season_date ON public.schedule_games_api(season, game_date);
    CREATE INDEX IF NOT EXISTS idx_schedule_games_api_teams ON public.schedule_games_api(season, home_team_id, away_team_id);
    """

    ddl_weeks = """
    CREATE TABLE IF NOT EXISTS public.schedule_weeks_api (
        season TEXT NOT NULL,
        season_year TEXT NULL,
        league_id TEXT NULL,
        week_number INT NULL,
        week_name TEXT NULL,
        start_date DATE NULL,
        end_date DATE NULL,
        inserted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_schedule_weeks_api_season ON public.schedule_weeks_api(season);
    """

    with engine.begin() as conn:
        conn.execute(text(ddl_games))
        conn.execute(text(ddl_weeks))


def upsert_like_replace(engine, df_games: pd.DataFrame, df_weeks: pd.DataFrame, season: str):
    """Simplest idempotent approach: delete season then append."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM public.schedule_games_api WHERE season = :season"), {"season": season})
        conn.execute(text("DELETE FROM public.schedule_weeks_api WHERE season = :season"), {"season": season})

    df_games.to_sql("schedule_games_api", engine, schema="public", if_exists="append", index=False, method="multi", chunksize=2000)
    df_weeks.to_sql("schedule_weeks_api", engine, schema="public", if_exists="append", index=False, method="multi", chunksize=2000)


def validate_schedule(engine, seasons: List[str]):
    """Print validation checks for completeness and data quality."""

    print("\n=== VALIDATION: per-season game counts ===")
    q_games = """
    SELECT season,
           COUNT(*) AS games,
           COUNT(DISTINCT game_id) AS distinct_game_ids,
           SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS null_game_id,
           SUM(CASE WHEN home_team_id IS NULL OR away_team_id IS NULL THEN 1 ELSE 0 END) AS null_team_ids,
           SUM(CASE WHEN game_date IS NULL THEN 1 ELSE 0 END) AS null_game_date
    FROM public.schedule_games_api
    WHERE season = ANY(:seasons)
    GROUP BY 1
    ORDER BY 1;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q_games), {"seasons": seasons}).mappings().all()
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== VALIDATION: expected games (rough) ===")
    print("Regular season expected ≈ 1230 games/season (30 teams * 82 / 2).")
    print("If you see substantially more, it may include preseason/playoffs; if less, schedule pull is incomplete.")

    print("\n=== VALIDATION: team games per season distribution ===")
    q_team_games = """
    WITH team_games AS (
      SELECT season, home_team_id AS team_id, COUNT(*) AS n
      FROM public.schedule_games_api
      WHERE season = ANY(:seasons)
      GROUP BY 1,2
      UNION ALL
      SELECT season, away_team_id AS team_id, COUNT(*) AS n
      FROM public.schedule_games_api
      WHERE season = ANY(:seasons)
      GROUP BY 1,2
    ), agg AS (
      SELECT season, team_id, SUM(n) AS games
      FROM team_games
      GROUP BY 1,2
    )
    SELECT season,
           MIN(games) AS min_games,
           MAX(games) AS max_games,
           AVG(games)::numeric(10,2) AS avg_games,
           SUM(CASE WHEN games = 82 THEN 1 ELSE 0 END) AS teams_at_82,
           COUNT(*) AS teams
    FROM agg
    GROUP BY 1
    ORDER BY 1;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q_team_games), {"seasons": seasons}).mappings().all()
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== VALIDATION: duplicates by (season, game_id) ===")
    q_dupes = """
    SELECT season, game_id, COUNT(*) AS n
    FROM public.schedule_games_api
    WHERE season = ANY(:seasons)
      AND game_id IS NOT NULL
    GROUP BY 1,2
    HAVING COUNT(*) > 1
    ORDER BY n DESC
    LIMIT 20;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q_dupes), {"seasons": seasons}).mappings().all()
    df_dupes = pd.DataFrame(rows)
    if len(df_dupes) == 0:
        print("No duplicates found.")
    else:
        print(df_dupes.to_string(index=False))

    print("\n=== VALIDATION: game status mix ===")
    q_status = """
    SELECT season, game_status_text, COUNT(*) AS n
    FROM public.schedule_games_api
    WHERE season = ANY(:seasons)
    GROUP BY 1,2
    ORDER BY 1, n DESC;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q_status), {"seasons": seasons}).mappings().all()
    print(pd.DataFrame(rows).to_string(index=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", nargs="+", required=True, help="Season strings like 2021-22 2022-23")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep between API calls")
    args = ap.parse_args()

    seasons = args.seasons
    engine = get_engine()

    ensure_tables(engine)

    for season in seasons:
        print(f"\nFetching schedule for season={season}...")
        season_games_raw, season_weeks_raw = fetch_scheduleleaguev2(season)

        print(f"Raw rows: season_games={len(season_games_raw)}, season_weeks={len(season_weeks_raw)}")

        games = normalize_games(season_games_raw)
        weeks = normalize_weeks(season_weeks_raw)

        # Keep only rows that look like actual games with two teams
        games = games.dropna(subset=["home_team_id", "away_team_id"]).copy()

        # Optional: filter out non-regular-season via game_code if needed.
        # Many users find ScheduleLeagueV2 includes regular season + postseason; validate first.
        # You can later add a filter like:
        # games = games[games["game_code"].str.contains("/002", na=False)]  # heuristic

        print(f"Normalized rows: games={len(games)}, weeks={len(weeks)}")

        upsert_like_replace(engine, games, weeks, season)
        print(f"Loaded season={season} into Postgres.")

        time.sleep(args.sleep)

    validate_schedule(engine, seasons)


if __name__ == "__main__":
    main()

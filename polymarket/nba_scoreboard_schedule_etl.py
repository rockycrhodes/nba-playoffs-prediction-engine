#!/usr/bin/env python3
"""NBA upcoming schedule ETL via NBA CDN scoreboard JSON -> Postgres (nba_data)

Why
----
stats.nba.com frequently blocks/resets non-browser traffic. The NBA CDN
scoreboard JSON is typically much more reliable.

Source
------
Daily scoreboard JSON:
  https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{YYYYMMDD}.json

This script fetches a window of dates (default 14 days ahead), parses games,
then upserts into public.schedule_games_api.

What it writes
--------------
Fills these schedule_games_api columns (others left NULL):
- season (default 2024-25)
- league_id (default 00)
- game_id
- game_status, game_status_text
- game_date
- home_team_tricode, away_team_tricode
- arena_name, arena_city, arena_state
- inserted_at

Usage
-----
export DATABASE_URL='postgresql://user:pass@localhost:5432/nba_data'
python nba_scoreboard_schedule_etl.py --days 14 --season 2024-25
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg2
import requests

BASE = "https://cdn.nba.com/static/json/liveData/scoreboard"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


def fetch_day(d: date, timeout_s: int = 30) -> Dict[str, Any]:
    ymd = d.strftime("%Y%m%d")
    url = f"{BASE}/scoreboard_{ymd}.json"
    r = requests.get(url, headers=HEADERS, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def parse_games(payload: Dict[str, Any], d: date) -> List[Dict[str, Any]]:
    games = ((payload.get("scoreboard") or {}).get("games")) or []
    out: List[Dict[str, Any]] = []

    for g in games:
        game_id = g.get("gameId")
        if not game_id:
            continue

        home = g.get("homeTeam") or {}
        away = g.get("awayTeam") or {}
        arena = g.get("arena") or {}

        out.append(
            {
                "game_id": int(game_id) if str(game_id).isdigit() else str(game_id),
                "game_status": g.get("gameStatus"),
                "game_status_text": g.get("gameStatusText"),
                "game_date": d,
                "home_team_tricode": home.get("teamTricode"),
                "away_team_tricode": away.get("teamTricode"),
                "arena_name": arena.get("arenaName"),
                "arena_city": arena.get("arenaCity"),
                "arena_state": arena.get("arenaState"),
            }
        )

    return out


def upsert(conn, season: str, league_id: str, rows: List[Dict[str, Any]], inserted_at: datetime) -> None:
    sql = """
    INSERT INTO public.schedule_games_api (
      season, league_id, game_id,
      game_status, game_status_text,
      game_date,
      home_team_tricode, away_team_tricode,
      arena_name, arena_city, arena_state,
      inserted_at
    )
    VALUES (
      %(season)s, %(league_id)s, %(game_id)s,
      %(game_status)s, %(game_status_text)s,
      %(game_date)s,
      %(home_team_tricode)s, %(away_team_tricode)s,
      %(arena_name)s, %(arena_city)s, %(arena_state)s,
      %(inserted_at)s
    )
    ON CONFLICT (season, league_id, game_id)
    DO UPDATE SET
      game_status = EXCLUDED.game_status,
      game_status_text = EXCLUDED.game_status_text,
      game_date = EXCLUDED.game_date,
      home_team_tricode = EXCLUDED.home_team_tricode,
      away_team_tricode = EXCLUDED.away_team_tricode,
      arena_name = EXCLUDED.arena_name,
      arena_city = EXCLUDED.arena_city,
      arena_state = EXCLUDED.arena_state,
      inserted_at = EXCLUDED.inserted_at;
    """

    with conn.cursor() as cur:
        cur.executemany(
            sql,
            [
                {**r, "season": season, "league_id": league_id, "inserted_at": inserted_at}
                for r in rows
            ],
        )


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--season", default="2024-25")
    ap.add_argument("--league-id", default="00")
    args = ap.parse_args(argv)

    if not args.database_url:
        print("ERROR: set DATABASE_URL or pass --database-url", file=sys.stderr)
        return 2

    inserted_at = datetime.now(tz=timezone.utc)

    all_rows: List[Dict[str, Any]] = []
    for i in range(args.days + 1):
        d = date.today() + timedelta(days=i)
        try:
            payload = fetch_day(d)
        except requests.HTTPError as e:
            # Many days will 404 (no games). That's fine.
            continue
        all_rows.extend(parse_games(payload, d))

    print(f"Fetched {len(all_rows)} game rows from CDN scoreboard over {args.days} days")

    with psycopg2.connect(args.database_url) as conn:
        upsert(conn, args.season, args.league_id, all_rows, inserted_at)
        conn.commit()

    print(f"Upserted {len(all_rows)} rows into public.schedule_games_api")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Build mappings.csv (game_id, polymarket_slug) for upcoming NBA games.

Strategy (robust, no hand entry)
--------------------------------
For each game in public.schedule_games_api within a date window:
- Generate a small set of candidate slugs based on common Polymarket patterns.
- Validate candidates by calling Gamma GET /markets/slug/{slug}.
- Accept the first candidate whose market question/outcomes match both teams.

Why this works
--------------
Gamma provides a deterministic market payload for a given slug, so we can
"probe" likely slug candidates without needing a full markets list endpoint.

Inputs
------
- schedule table: public.schedule_games_api
  Required columns: game_id, game_date, home_team_tricode, away_team_tricode

Output
------
- CSV with columns: game_id, polymarket_slug

Usage
-----
export DATABASE_URL='postgresql://user:pass@localhost:5432/nba_data'
python build_mappings.py --days 2 --out mappings.csv

Notes
-----
- This script is conservative: it only writes rows when it can validate a slug.
- You can re-run it; it overwrites the output file.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Team name hints to match Gamma outcomes/question
TRICODE_TO_NAMES = {
    "ATL": ["Hawks"],
    "BOS": ["Celtics"],
    "BKN": ["Nets"],
    "BRK": ["Nets"],
    "CHA": ["Hornets"],
    "CHI": ["Bulls"],
    "CLE": ["Cavaliers"],
    "DAL": ["Mavericks"],
    "DEN": ["Nuggets"],
    "DET": ["Pistons"],
    "GSW": ["Warriors"],
    "GS": ["Warriors"],
    "HOU": ["Rockets"],
    "IND": ["Pacers"],
    "LAC": ["Clippers"],
    "LAL": ["Lakers"],
    "MEM": ["Grizzlies"],
    "MIA": ["Heat"],
    "MIL": ["Bucks"],
    "MIN": ["Timberwolves", "Wolves"],
    "NOP": ["Pelicans"],
    "NO": ["Pelicans"],
    "NYK": ["Knicks"],
    "NY": ["Knicks"],
    "OKC": ["Thunder"],
    "ORL": ["Magic"],
    "PHI": ["76ers", "Sixers"],
    "PHX": ["Suns"],
    "POR": ["Trail Blazers", "Blazers"],
    "SAC": ["Kings"],
    "SAS": ["Spurs"],
    "TOR": ["Raptors"],
    "UTA": ["Jazz"],
    "WAS": ["Wizards"],
}


def gamma_get_market_by_slug(slug: str, timeout_s: int = 15) -> Optional[Dict[str, Any]]:
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    r = requests.get(url, timeout=timeout_s)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def parse_json_list_string(s: Optional[str]) -> List[str]:
    if not s:
        return []
    s = s.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return []
    return [p.strip().strip('"').strip("'") for p in s[1:-1].split(",") if p.strip()]


def market_matches_teams(mkt: Dict[str, Any], away_tri: str, home_tri: str) -> bool:
    away_names = TRICODE_TO_NAMES.get(away_tri.upper(), [away_tri.upper()])
    home_names = TRICODE_TO_NAMES.get(home_tri.upper(), [home_tri.upper()])

    question = normalize(str(mkt.get("question") or ""))
    outcomes = [normalize(x) for x in parse_json_list_string(mkt.get("outcomes"))]

    def any_name_in(names: List[str], hay: str) -> bool:
        return any(normalize(n) in hay for n in names)

    # Prefer outcomes matching (most reliable)
    if outcomes:
        away_ok = any(normalize(n) in outcomes for n in away_names)
        home_ok = any(normalize(n) in outcomes for n in home_names)
        if away_ok and home_ok:
            return True

    # Fallback to question string
    return any_name_in(away_names, question) and any_name_in(home_names, question)


def candidate_slugs(away_tri: str, home_tri: str, game_date: date) -> List[str]:
    a = away_tri.lower()
    h = home_tri.lower()
    d = game_date.isoformat()

    # Observed pattern: nba-det-cle-2026-05-15
    # Try a few variations to be safe.
    return [
        f"nba-{a}-{h}-{d}",
        f"nba-{h}-{a}-{d}",
        f"nba-{a}-vs-{h}-{d}",
        f"nba-{h}-vs-{a}-{d}",
        f"nba-{a}-{h}-{d}-moneyline",
        f"nba-{h}-{a}-{d}-moneyline",
    ]


def fetch_games(conn, start: date, end: date) -> List[Tuple[str, date, str, str]]:
    sql = """
    SELECT
      game_id::text AS game_id,
      game_date,
      home_team_tricode,
      away_team_tricode
    FROM public.schedule_games_api
    WHERE game_date >= %(start)s
      AND game_date <= %(end)s
    ORDER BY game_date, game_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"start": start, "end": end})
        rows = cur.fetchall()
    return [(r[0], r[1], r[3], r[2]) for r in rows]  # return (game_id, game_date, away, home)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.getenv("DATABASE_URL"), help="postgresql://... (or set DATABASE_URL)")
    ap.add_argument("--days", type=int, default=2, help="How many days ahead to include")
    ap.add_argument("--out", default="mappings.csv")
    args = ap.parse_args(argv)

    if not args.database_url:
        print("ERROR: Provide --database-url or set DATABASE_URL", file=sys.stderr)
        return 2

    start = date.today()
    end = start + timedelta(days=args.days)

    with psycopg2.connect(args.database_url) as conn:
        games = fetch_games(conn, start, end)

    out_rows: List[Dict[str, str]] = []
    misses: List[Tuple[str, str]] = []

    for game_id, gdate, away_tri, home_tri in games:
        found = None
        for slug in candidate_slugs(away_tri, home_tri, gdate):
            mkt = gamma_get_market_by_slug(slug)
            if not mkt:
                continue
            if market_matches_teams(mkt, away_tri, home_tri):
                found = slug
                break
        if found:
            out_rows.append({"game_id": game_id, "polymarket_slug": found})
        else:
            misses.append((game_id, f"{away_tri}@{home_tri} {gdate.isoformat()}"))

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["game_id", "polymarket_slug"])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"Wrote {len(out_rows)} mappings to {args.out}")
    if misses:
        print(f"Unmatched games: {len(misses)} (showing up to 20)")
        for gid, desc in misses[:20]:
            print(f" - {gid}: {desc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

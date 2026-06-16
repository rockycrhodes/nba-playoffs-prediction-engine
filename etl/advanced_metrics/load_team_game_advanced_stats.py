#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, random
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import psycopg2
from psycopg2.extras import execute_values

from nba_api.stats.endpoints import boxscoreadvancedv3
from nba_api.stats.library.http import NBAStatsHTTP

DB_NAME = os.getenv("NBA_DB_NAME", "nba_data")
DB_USER = os.getenv("NBA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("NBA_DB_HOST", "localhost")
DB_PORT = os.getenv("NBA_DB_PORT", "5432")

RETRIES = 10
SLEEP_SECONDS = 2.5
COMMIT_EVERY_GAMES = 25
FAILURES_CSV = "advanced_stats_failures.csv"

KEEP_TYPE_DIGITS = {"2", "4"}  # regular season + playoffs (game_id[4])
REQUEST_TIMEOUT = 60
MIN_BACKOFF = 5.0
MAX_BACKOFF = 180.0
DEBUG_FIRST_N_FAILURES = 1


def make_engine():
    uri = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(uri)


def normalize_game_id_10(game_id) -> str:
    s = str(game_id).strip()
    return s.zfill(10) if s.isdigit() else s


def game_type_digit(game_id_10: str) -> str:
    s = normalize_game_id_10(game_id_10)
    return s[4] if len(s) >= 5 else ""


def configure_nba_http():
    NBAStatsHTTP.timeout = REQUEST_TIMEOUT
    base = dict(getattr(NBAStatsHTTP, "headers", {}) or {})
    base.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept-Language": "en-US,en;q=0.9",
    })
    NBAStatsHTTP.headers = base


def fetch_seasons(engine):
    with engine.connect() as c:
        df = pd.read_sql(text("SELECT DISTINCT season FROM public.games ORDER BY season;"), c)
    return df["season"].astype(str).tolist()


def fetch_games(engine, seasons):
    q = text("""
        SELECT season, game_id, game_date
        FROM public.games
        WHERE season IN :seasons
        ORDER BY game_date, game_id
    """).bindparams(bindparam("seasons", expanding=True))

    with engine.connect() as c:
        df = pd.read_sql(q, c, params={"seasons": list(seasons)})

    df["season"] = df["season"].astype(str)
    df["game_id"] = df["game_id"].apply(normalize_game_id_10)

    #df["type_digit"] = df["game_id"].apply(game_type_digit)
    #df = df[df["type_digit"].isin(KEEP_TYPE_DIGITS)].copy()
    #df.drop(columns=["type_digit"], inplace=True)

    return df


def fetch_existing_game_ids(engine, seasons):
    q = text("""
        SELECT DISTINCT game_id
        FROM public.team_game_advanced_stats
        WHERE season IN :seasons
    """).bindparams(bindparam("seasons", expanding=True))

    with engine.connect() as c:
        df = pd.read_sql(q, c, params={"seasons": list(seasons)})

    if df.empty:
        return set()
    return set(df["game_id"].apply(normalize_game_id_10).tolist())


def upsert_rows(cur, rows):
    sql = """
    INSERT INTO public.team_game_advanced_stats
      (game_id, team_id, season, game_date,
       off_rtg, def_rtg, net_rtg, pace, poss,
       efg_pct, ts_pct, tov_pct, oreb_pct, dreb_pct)
    VALUES %s
    ON CONFLICT (game_id, team_id) DO UPDATE SET
      season = EXCLUDED.season,
      game_date = EXCLUDED.game_date,
      off_rtg = EXCLUDED.off_rtg,
      def_rtg = EXCLUDED.def_rtg,
      net_rtg = EXCLUDED.net_rtg,
      pace = EXCLUDED.pace,
      poss = EXCLUDED.poss,
      efg_pct = EXCLUDED.efg_pct,
      ts_pct = EXCLUDED.ts_pct,
      tov_pct = EXCLUDED.tov_pct,
      oreb_pct = EXCLUDED.oreb_pct,
      dreb_pct = EXCLUDED.dreb_pct,
      updated_at = now();
    """
    execute_values(cur, sql, rows, page_size=500)


def call_endpoint(game_id_10: str):
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            ep = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id_10, timeout=REQUEST_TIMEOUT)
            _ = ep.get_data_frames()
            return ep
        except Exception as e:
            last_err = e
            backoff = min(MAX_BACKOFF, max(MIN_BACKOFF, (2 ** attempt) + random.uniform(0, 3)))
            print(f"[WARN] api error game_id={game_id_10} attempt={attempt}/{RETRIES}: {e} sleep={backoff:.1f}s")
            time.sleep(backoff)
    raise last_err


def parse_team_rows_from_endpoint(endpoint_obj):
    dfs = endpoint_obj.get_data_frames() or []
    if not dfs:
        return []

    def cols_lower(df):
        return {str(c).strip().lower() for c in df.columns}

    team_df = None
    for df in dfs:
        if "teamid" in cols_lower(df) and len(df) == 2:
            team_df = df.copy()
            break
    if team_df is None:
        return []

    def fnum(x):
        try:
            if x is None or x == "":
                return None
            return float(x)
        except Exception:
            return None

    out = []
    for _, r in team_df.iterrows():
        # columns may be lower/upper depending on nba_api version
        team_id = r.get("teamId", r.get("TEAM_ID", r.get("TEAMID")))
        if pd.isna(team_id):
            continue

        out.append({
            "team_id": int(team_id),
            "off_rtg": fnum(r.get("offensiveRating", r.get("OFF_RATING", r.get("OFFENSIVERATING")))),
            "def_rtg": fnum(r.get("defensiveRating", r.get("DEF_RATING", r.get("DEFENSIVERATING")))),
            "net_rtg": fnum(r.get("netRating", r.get("NET_RATING", r.get("NETRATING")))),
            "pace": fnum(r.get("pace", r.get("PACE"))),
            "poss": fnum(r.get("possessions", r.get("POSS", r.get("POSSESSIONS")))),
            "efg_pct": fnum(r.get("effectiveFieldGoalPercentage", r.get("EFG_PCT", r.get("EFFECTIVEFIELDGOALPERCENTAGE")))),
            "ts_pct": fnum(r.get("trueShootingPercentage", r.get("TS_PCT", r.get("TRUESHOOTINGPERCENTAGE")))),
            "tov_pct": fnum(r.get("turnoverRatio", r.get("TOV_PCT", r.get("TURNOVERRATIO")))),
            "oreb_pct": fnum(r.get("offensiveReboundPercentage", r.get("OREB_PCT", r.get("OFFENSIVEREBOUNDPERCENTAGE")))),
            "dreb_pct": fnum(r.get("defensiveReboundPercentage", r.get("DREB_PCT", r.get("DEFENSIVEREBOUNDPERCENTAGE")))),
        })
    return out


def main():
    configure_nba_http()
    engine = make_engine()

    seasons = fetch_seasons(engine)
    print("Seasons detected in games:", seasons)

    games = fetch_games(engine, seasons)
    existing = fetch_existing_game_ids(engine, seasons)

    to_load = games[~games["game_id"].isin(existing)].copy()
    print(f"Games in scope (RS+PO): {len(games)} | already loaded: {len(existing)} | to load: {len(to_load)}")

    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()

    batch, attempted, succeeded = [], 0, 0
    failed, debug_left = [], DEBUG_FIRST_N_FAILURES

    for _, row in to_load.iterrows():
        attempted += 1
        season = row["season"]
        game_id_10 = row["game_id"]
        game_date = row["game_date"]

        try:
            ep = call_endpoint(game_id_10)
            team_rows = parse_team_rows_from_endpoint(ep)

            if not team_rows:
                if debug_left > 0:
                    dfs = ep.get_data_frames() or []
                    print("[DEBUG] game_id", game_id_10, "dfs:", [(i, d.shape, list(d.columns)) for i, d in enumerate(dfs)])
                    debug_left -= 1
                raise RuntimeError("No team rows parsed from API payload")

            for tr in team_rows:
                batch.append((
                    game_id_10, tr["team_id"], season, game_date,
                    tr["off_rtg"], tr["def_rtg"], tr["net_rtg"], tr["pace"], tr["poss"],
                    tr["efg_pct"], tr["ts_pct"], tr["tov_pct"], tr["oreb_pct"], tr["dreb_pct"]
                ))
            succeeded += 1

        except Exception as e:
            failed.append({"season": season, "game_id": game_id_10, "error": str(e)})
            print(f"[FAIL] game_id={game_id_10} err={e}")

        if (succeeded > 0) and (succeeded % COMMIT_EVERY_GAMES == 0) and batch:
            upsert_rows(cur, batch)
            print(f"[OK] committed succeeded_games={succeeded} attempted={attempted} rows={len(batch)}")
            batch = []

        time.sleep(SLEEP_SECONDS)

    if batch:
        upsert_rows(cur, batch)
        print(f"[OK] final commit rows={len(batch)}")

    cur.close()
    conn.close()

    if failed:
        pd.DataFrame(failed).to_csv(FAILURES_CSV, index=False)
        print(f"[WARN] wrote {FAILURES_CSV} with {len(failed)} failures")

    print(f"Done. attempted={attempted} succeeded={succeeded} failed={len(failed)}")


if __name__ == "__main__":
    main()

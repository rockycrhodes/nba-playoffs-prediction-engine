#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 21:42:23 2026

@author: rrhodes
"""

import os
import math
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_NAME = os.getenv("NBA_DB_NAME", "nba_data")
DB_USER = os.getenv("NBA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("NBA_DB_HOST", "localhost")
DB_PORT = os.getenv("NBA_DB_PORT", "5432")

ELO_START = 1500.0
K_FACTOR = 20.0
HOME_ADV = 0.0  # set to ~50 later if you want

SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]


def expected_score(elo_a, elo_b):
    # Elo expected score
    return 1.0 / (1.0 + 10 ** (-(elo_a - elo_b) / 400.0))


def fetch_results(conn, season):
    q = """
    SELECT season, game_id, game_date, team_id, opp_team_id, is_home, is_win
    FROM elo_game_results
    WHERE season = %s
    ORDER BY game_date, game_id, team_id;
    """
    return pd.read_sql(q, conn, params=(season,))


def upsert_elo_rows(cur, rows):
    sql = """
    INSERT INTO team_elo_history
      (season, game_id, game_date, team_id, opp_team_id, is_home, elo_pre, elo_post, result, k_factor)
    VALUES %s
    ON CONFLICT (season, game_id, team_id) DO UPDATE SET
      game_date = EXCLUDED.game_date,
      opp_team_id = EXCLUDED.opp_team_id,
      is_home = EXCLUDED.is_home,
      elo_pre = EXCLUDED.elo_pre,
      elo_post = EXCLUDED.elo_post,
      result = EXCLUDED.result,
      k_factor = EXCLUDED.k_factor;
    """
    execute_values(cur, sql, rows, page_size=2000)


def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = False
    cur = conn.cursor()

    for season in SEASONS:
        df = fetch_results(conn, season)

        if df.empty:
            print(f"[WARN] No rows for season {season}. Skipping.")
            continue

        # Elo state per team for this season
        elo = {}

        out_rows = []

        # We process per game_id: need both teams together
        for game_id, g in df.groupby("game_id", sort=False):
            if len(g) != 2:
                # should be exactly two rows (one per team)
                continue

            r1 = g.iloc[0]
            r2 = g.iloc[1]

            # identify home/away row
            # identify home/away row (handle NULLs safely)
            r1_home = 1 if str(r1["is_home"]) == "1" else 0
            r2_home = 1 if str(r2["is_home"]) == "1" else 0
            
            if r1_home == 1:
                home, away = r1, r2
            elif r2_home == 1:
                home, away = r2, r1
            else:
                # fallback: treat first row as home if missing; HOME_ADV is 0 anyway
                home, away = r1, r2
                        

            home_id = int(home["team_id"])
            away_id = int(away["team_id"])

            # init elos
            elo_home = float(elo.get(home_id, ELO_START))
            elo_away = float(elo.get(away_id, ELO_START))

            # apply home advantage only to expected score computation
            exp_home = expected_score(elo_home + HOME_ADV, elo_away)
            exp_away = 1.0 - exp_home
            
            # Skip games where result is missing (e.g., missing points / malformed rows)
            if pd.isna(home["is_win"]) or pd.isna(away["is_win"]):
                continue
            
            res_home = int(home["is_win"])
            res_away = int(away["is_win"])

            # sanity: res_home + res_away should be 1
            # update
            new_home = elo_home + K_FACTOR * (res_home - exp_home)
            new_away = elo_away + K_FACTOR * (res_away - exp_away)

            game_date = str(home["game_date"])
            
            # store rows (elo_pre is before this game)
            out_rows.append((season, int(game_id), game_date, home_id, away_id, 1, elo_home, new_home, res_home, K_FACTOR))
            out_rows.append((season, int(game_id), game_date, away_id, home_id, 0, elo_away, new_away, res_away, K_FACTOR))

            # update in-memory state
            elo[home_id] = new_home
            elo[away_id] = new_away

        upsert_elo_rows(cur, out_rows)
        conn.commit()
        print(f"[OK] {season}: wrote {len(out_rows)} elo rows")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

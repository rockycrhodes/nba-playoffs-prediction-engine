#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:54:43 2026

@author: rrhodes
"""


from datetime import date
import time
import random
import pandas as pd
from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import LeagueStandings

SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
MAX_RETRIES = 5
BASE_SLEEP = 3  # seconds

OUTFILE = "raw_standings_backfill.csv"
PARTIAL_OUTFILE = "raw_standings_backfill_partial.csv"

def fetch_league_standings_with_retry(season: str) -> pd.DataFrame:
    for attempt in range(MAX_RETRIES):
        try:
            df = LeagueStandings(season=season).get_data_frames()[0]
            df["season"] = season
            df["run_date"] = date.today().isoformat()
            return df
        except ReadTimeout as e:
            sleep_s = BASE_SLEEP * (attempt + 1) + random.random()
            print(f"Timeout {season} (attempt {attempt+1}/{MAX_RETRIES}): {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
        except Exception as e:
            sleep_s = BASE_SLEEP * (attempt + 1) + random.random()
            print(f"Error {season} (attempt {attempt+1}/{MAX_RETRIES}): {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"FAILED {season} after {MAX_RETRIES} attempts")

if __name__ == "__main__":
    frames = []

    for s in SEASONS:
        try:
            df_s = fetch_league_standings_with_retry(s)
            print(f"{s}: {df_s.shape}")
            frames.append(df_s)

            # write partial progress each season (so you can resume)
            pd.concat(frames, ignore_index=True).to_csv(PARTIAL_OUTFILE, index=False)

        except Exception as e:
            print(str(e))

    df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df_all.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE}: {df_all.shape}")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, timedelta
import time
import random
import pandas as pd
from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3

MAX_RETRIES = 5
BASE_SLEEP = 3  # seconds
OUTFILE = "raw_team_stats_today.csv"

# Columns we expect from BoxScoreTraditionalV3.team_stats + your added metadata.
# (Even if the API returns slightly more/less, this prevents empty-file crashes.)
EXPECTED_COLS = [
    "gameId","teamId","teamCity","teamName","teamTricode","teamSlug",
    "minutes",
    "fieldGoalsMade","fieldGoalsAttempted","fieldGoalsPercentage",
    "threePointersMade","threePointersAttempted","threePointersPercentage",
    "freeThrowsMade","freeThrowsAttempted","freeThrowsPercentage",
    "reboundsOffensive","reboundsDefensive","reboundsTotal",
    "assists","steals","blocks","turnovers","foulsPersonal",
    "points","plusMinusPoints",
    "GAME_ID","SEASON","SEASON_TYPE"
]

def infer_season_str(d: date) -> str:
    start_year = d.year if d.month >= 9 else d.year - 1
    return f"{start_year}-{str(start_year+1)[-2:]}"

def get_game_ids_for_date(target_date: date, season_type: str) -> list[str]:
    date_str = target_date.strftime("%m/%d/%Y")
    try:
        df = leaguegamelog.LeagueGameLog(
            date_from_nullable=date_str,
            date_to_nullable=date_str,
            season_type_all_star=season_type
        ).get_data_frames()[0]
        if df.empty:
            return []
        return df["GAME_ID"].dropna().unique().tolist()
    except Exception as e:
        print(f"LeagueGameLog failed for {target_date} [{season_type}]: {e}")
        return []

def fetch_team_stats_for_game(game_id: str) -> pd.DataFrame:
    for attempt in range(MAX_RETRIES):
        try:
            bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
            return bs.team_stats.get_data_frame()
        except (ReadTimeout,) as e:
            sleep_s = BASE_SLEEP * (attempt + 1) + random.random()
            print(f"Timeout team boxscore {game_id} (attempt {attempt+1}/{MAX_RETRIES}): {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
        except Exception as e:
            sleep_s = BASE_SLEEP * (attempt + 1) + random.random()
            print(f"Error team boxscore {game_id} (attempt {attempt+1}/{MAX_RETRIES}): {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return pd.DataFrame()

def extract_team_stats_for_date(target_date: date) -> pd.DataFrame:
    season = infer_season_str(target_date)

    all_parts = []
    total_games = 0
    ok_games = 0
    failed_games = 0

    for season_type in ["Regular Season", "PlayIn", "Playoffs"]:
        game_ids = get_game_ids_for_date(target_date, season_type)
        if not game_ids:
            continue

        total_games += len(game_ids)
        print(f"{target_date} [{season_type}]: {len(game_ids)} games")

        for gid in game_ids:
            df_game = fetch_team_stats_for_game(gid)
            if df_game is None or df_game.empty:
                failed_games += 1
                continue

            df_game["GAME_ID"] = gid
            df_game["SEASON"] = season
            df_game["SEASON_TYPE"] = season_type
            all_parts.append(df_game)
            ok_games += 1

            time.sleep(BASE_SLEEP + random.random())

    if all_parts:
        out_df = pd.concat(all_parts, ignore_index=True)
    else:
        # IMPORTANT: write an empty dataframe WITH HEADERS so pandas can read it later
        out_df = pd.DataFrame(columns=EXPECTED_COLS)

    print(f"Games: total={total_games}, ok={ok_games}, failed={failed_games}")
    return out_df

if __name__ == "__main__":
    # Use yesterday to capture completed games reliably
    yday = date.today() - timedelta(days=1)

    df_daily_team_stats = extract_team_stats_for_date(yday)
    df_daily_team_stats.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE}: shape={df_daily_team_stats.shape}")

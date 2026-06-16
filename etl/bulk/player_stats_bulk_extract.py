#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:09:28 2026

@author: rrhodes
"""
from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3
import pandas as pd
import time
import random

MAX_RETRIES = 3
BASE_SLEEP = 3  # seconds
PROGRESS_EVERY = 50  # Save to partial CSV after this many games

def extract_bulk_player_game_stats(seasons, progress_every=PROGRESS_EVERY):
    all_stats = []
    try:
        for season in seasons:
            for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:
                games_df = leaguegamelog.LeagueGameLog(
                    season=season,
                    season_type_all_star=season_type
                ).get_data_frames()[0]
                game_ids = games_df["GAME_ID"].unique()
                print(f"{season} [{season_type}]: Found {len(game_ids)} games.")
                for idx, gid in enumerate(game_ids):
                    success = False
                    for attempt in range(MAX_RETRIES): 
                        try:
                            stats = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid)
                            stat_df = stats.player_stats.get_data_frame()
                            if not stat_df.empty:
                                stat_df['GAME_ID'] = gid
                                stat_df['SEASON'] = season
                                stat_df['SEASON_TYPE'] = season_type
                                all_stats.append(stat_df)
                            time.sleep(BASE_SLEEP + random.random())
                            success = True
                            break
                        except Exception as e:
                            print(f"Error for game {gid} (attempt {attempt+1}): {e}")
                            sleep_time = BASE_SLEEP * (attempt + 1) + random.random()
                            print(f"Sleeping {sleep_time:.1f}s before retry...")
                            time.sleep(sleep_time)
                    if not success:
                        print(f"FAILED for game {gid} after {MAX_RETRIES} attempts.")
                    # Progressive save
                    if (idx + 1) % progress_every == 0:
                        print(f"Progress: {idx+1}/{len(game_ids)} games done, saving partial progress...")
                        pd.concat(all_stats, ignore_index=True).to_csv(
                            f'partial_player_stats_{season}_{season_type}.csv', index=False
                        )
        if all_stats:
            big_df = pd.concat(all_stats, ignore_index=True)
        else:
            big_df = pd.DataFrame()
        return big_df
    except KeyboardInterrupt:
        print("Script interrupted. Saving current progress...")
        if all_stats:
            pd.concat(all_stats, ignore_index=True).to_csv('interrupted_player_stats.csv', index=False)
        raise  # Clean exit

if __name__ == "__main__":
    seasons = ['2022-23', '2023-24', '2024-25']  # Adjust as needed
    df_bulk_player_stats = extract_bulk_player_game_stats(seasons)
    df_bulk_player_stats.to_csv('raw_player_stats_bulk.csv', index=False)
    print(df_bulk_player_stats.head())


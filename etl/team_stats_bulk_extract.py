#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:00:27 2026

@author: rrhodes
"""


from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3
import pandas as pd
import time
import random
from requests.exceptions import ReadTimeout

MAX_RETRIES = 5
BASE_SLEEP = 3  # seconds

def get_games_df_with_retry(season, season_type, max_retries=MAX_RETRIES, base_sleep=BASE_SLEEP):
    """Fetch LeagueGameLog with retry/backoff."""
    for attempt in range(max_retries):
        try:
            glog = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star=season_type
            )
            df = glog.get_data_frames()[0]
            return df
        except ReadTimeout as e:
            sleep_s = base_sleep * (attempt + 1) + random.random()
            print(f"Timeout for LeagueGameLog {season} [{season_type}] (attempt {attempt+1}): {e}. "
                  f"Sleeping {sleep_s:.1f}s then retrying...")
            time.sleep(sleep_s)
        except Exception as e:
            print(f"Error for LeagueGameLog {season} [{season_type}] (attempt {attempt+1}): {e}")
            sleep_s = base_sleep * (attempt + 1) + random.random()
            time.sleep(sleep_s)
    print(f"FAILED to fetch LeagueGameLog for {season} [{season_type}] after {max_retries} attempts.")
    return pd.DataFrame()

def extract_team_game_stats_slice(
    seasons,
    season_types=('Regular Season', 'PlayIn', 'Playoffs'),
    game_slice=(0, 500),
    progress_every=50
):
    all_stats = []
    try:
        for season in seasons:
            for season_type in season_types:
                games_df = get_games_df_with_retry(season, season_type)
                if games_df.empty:
                    print(f"No games returned for {season} [{season_type}]. Skipping.")
                    continue
                game_ids = games_df["GAME_ID"].unique()
                print(f"{season} [{season_type}]: Found {len(game_ids)} games.")
                start_idx, end_idx = game_slice
                end_idx = min(end_idx, len(game_ids))
                sliced_game_ids = game_ids[start_idx:end_idx]
                print(f"Processing games {start_idx} to {end_idx} of {len(game_ids)}.")

                for idx, gid in enumerate(sliced_game_ids, start=start_idx):
                    success = False
                    for attempt in range(MAX_RETRIES):
                        try:
                            stats = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid)
                            stat_df = stats.team_stats.get_data_frame()
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
                    # Progressive save for this slice
                    if ((idx - start_idx + 1) % progress_every == 0) or (idx == end_idx - 1):
                        part_filename = f'partial_team_stats_{season}_{season_type}_games{start_idx}_{end_idx}.csv'
                        print(f"Progress: {idx+1-start_idx}/{end_idx-start_idx} games done, saving partial: {part_filename}")
                        pd.concat(all_stats, ignore_index=True).to_csv(part_filename, index=False)

        if all_stats:
            big_df = pd.concat(all_stats, ignore_index=True)
        else:
            big_df = pd.DataFrame()
        return big_df
    except KeyboardInterrupt:
        print("Script interrupted. Saving current progress...")
        part_filename = f'interrupted_team_stats_{game_slice[0]}_{game_slice[1]}.csv'
        if all_stats:
            pd.concat(all_stats, ignore_index=True).to_csv(part_filename, index=False)
        raise

if __name__ == "__main__":
    my_slice = (1010, 1360)
    my_seasons = ['2025-26']
    my_types = ['Regular Season']

    df_slice = extract_team_game_stats_slice(my_seasons, season_types=my_types, game_slice=my_slice)
    outname = f'bulk_team_stats_{my_seasons[0]}_{my_types[0].replace(" ", "")}_games{my_slice[0]}_{my_slice[1]}.csv'
    df_slice.to_csv(outname, index=False)
    print(df_slice.head())



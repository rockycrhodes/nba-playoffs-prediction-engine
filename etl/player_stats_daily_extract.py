#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:10:57 2026

@author: rrhodes
"""

from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3
import pandas as pd
import time
from datetime import date

def extract_today_player_game_stats():
    all_stats = []
    today_str = date.today().strftime('%m/%d/%Y')
    for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:
        games_df = leaguegamelog.LeagueGameLog(
            date_from_nullable=today_str,
            date_to_nullable=today_str,
            season_type_all_star=season_type
        ).get_data_frames()[0]
        if games_df.shape[0] == 0:
            continue
        game_ids = games_df["GAME_ID"].unique()
        for gid in game_ids:
            try:
                stats = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid)
                stat_df = stats.player_stats.get_data_frame()
                stat_df['GAME_ID'] = gid
                stat_df['SEASON'] = None
                stat_df['SEASON_TYPE'] = season_type
                all_stats.append(stat_df)
                time.sleep(0.5)
            except Exception as e:
                print(f"Error for game {gid}: {e}")
    if all_stats:
        return pd.concat(all_stats, ignore_index=True)
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    df_daily_player_stats = extract_today_player_game_stats()
    df_daily_player_stats.to_csv('raw_player_stats_today.csv', index=False)
    print(df_daily_player_stats.head())


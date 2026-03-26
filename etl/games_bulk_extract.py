#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:27:19 2026

@author: rrhodes
"""

from nba_api.stats.endpoints import leaguegamelog
import pandas as pd

def extract_historical_games(seasons):
    all_games = []
    for season in seasons:
        for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:  # Grab 'Playoffs' and 'PlayIn' flags
            glog = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type)
            df = glog.get_data_frames()[0]
            df['SEASON'] = season
            df['SEASON_TYPE'] = season_type  # add column
            all_games.append(df)
    combined = pd.concat(all_games, ignore_index=True)
    return combined

if __name__ == '__main__':
    # Bulk load 2021-22 to 2024-25
    seasons = ['2021-22', '2022-23', '2023-24', '2024-25', '2025-26']
    df_games = extract_historical_games(seasons)
    df_games.to_csv('raw_games_bulk.csv', index=False)
    print("Extracted historical games:", df_games.shape)


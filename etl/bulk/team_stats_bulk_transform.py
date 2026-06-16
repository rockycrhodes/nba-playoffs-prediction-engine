#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:03:00 2026

@author: rrhodes
"""

import pandas as pd
import glob

def transform_team_stats(df):
    # Use actual BoxScoreTraditionalV3 team_stats column names
    keep_cols = [
        'gameId', 'teamId', 'teamCity', 'teamName', 'teamTricode',
        'minutes',
        'fieldGoalsMade', 'fieldGoalsAttempted', 'fieldGoalsPercentage',
        'threePointersMade', 'threePointersAttempted', 'threePointersPercentage',
        'freeThrowsMade', 'freeThrowsAttempted', 'freeThrowsPercentage',
        'reboundsOffensive', 'reboundsDefensive', 'reboundsTotal',
        'assists', 'steals', 'blocks', 'turnovers', 'foulsPersonal',
        'points', 'plusMinusPoints',
        'SEASON', 'SEASON_TYPE'
    ]
    cols = [col for col in keep_cols if col in df.columns]
    df_clean = df[cols].copy()

    df_clean.rename(columns={
        'gameId': 'game_id',
        'teamId': 'team_id',
        'teamCity': 'team_city',
        'teamName': 'team_name',
        'teamTricode': 'team_abbreviation',
        'minutes': 'minutes',
        'fieldGoalsMade': 'fgm',
        'fieldGoalsAttempted': 'fga',
        'fieldGoalsPercentage': 'fg_pct',
        'threePointersMade': 'fg3m',
        'threePointersAttempted': 'fg3a',
        'threePointersPercentage': 'fg3_pct',
        'freeThrowsMade': 'ftm',
        'freeThrowsAttempted': 'fta',
        'freeThrowsPercentage': 'ft_pct',
        'reboundsOffensive': 'oreb',
        'reboundsDefensive': 'dreb',
        'reboundsTotal': 'reb',
        'assists': 'ast',
        'steals': 'stl',
        'blocks': 'blk',
        'turnovers': 'turnovers',
        'foulsPersonal': 'pf',
        'points': 'pts',
        'plusMinusPoints': 'plus_minus',
        'SEASON': 'season',
        'SEASON_TYPE': 'season_type'
    }, inplace=True)

    return df_clean


if __name__ == '__main__':
    # Use glob to find all files that match the desired pattern
    file_list = glob.glob('partial_team_stats_202*.csv') 
    dfs = []
    for f in file_list:
        try:
            tempdf = pd.read_csv(f)
            if tempdf.shape[0] == 0:
                print(f"SKIPPING empty file: {f}")
                continue
            dfs.append(tempdf)
        except Exception as e:
            print(f"SKIPPING {f} due to error: {e}")
    if not dfs:
        print("No valid dataframes found. Exiting.")
    else:
        df = pd.concat(dfs, ignore_index=True)
        df_clean = transform_team_stats(df)
        # Drop possible duplicates based on game/team
        drop_dupe_cols = [col for col in ['game_id', 'team_id'] if col in df_clean.columns]
        if drop_dupe_cols:
            df_clean = df_clean.drop_duplicates(subset=drop_dupe_cols)
        df_clean.to_csv('team_stats_for_load_bulk.csv', index=False)
        print(df_clean.head())

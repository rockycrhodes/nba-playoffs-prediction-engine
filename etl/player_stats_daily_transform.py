#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:09:45 2026

@author: rrhodes
"""

import pandas as pd

def transform_player_stats(df):
    """
    Renames and filters columns for DB load.
    """
    keep_cols = [
        'GAME_ID', 'TEAM_ID', 'PLAYER_ID', 'PLAYER_NAME', 'START_POSITION', 'COMMENT',
        'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
        'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL',
        'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS', 'SEASON', 'SEASON_TYPE'
    ]
    cols = [col for col in keep_cols if col in df.columns]
    df_clean = df[cols].copy()
    df_clean.rename(columns={
        'GAME_ID': 'game_id',
        'TEAM_ID': 'team_id',
        'PLAYER_ID': 'player_id',
        'PLAYER_NAME': 'player_name',
        'START_POSITION': 'start_position',
        'COMMENT': 'comment',
        'MIN': 'minutes',
        'FGM': 'fgm',
        'FGA': 'fga',
        'FG_PCT': 'fg_pct',
        'FG3M': 'fg3m',
        'FG3A': 'fg3a',
        'FG3_PCT': 'fg3_pct',
        'FTM': 'ftm',
        'FTA': 'fta',
        'FT_PCT': 'ft_pct',
        'OREB': 'oreb',
        'DREB': 'dreb',
        'REB': 'reb',
        'AST': 'ast',
        'STL': 'stl',
        'BLK': 'blk',
        'TO': 'turnovers',
        'PF': 'pf',
        'PTS': 'pts',
        'PLUS_MINUS': 'plus_minus',
        'SEASON': 'season', 
        'SEASON_TYPE': 'season_type'
    }, inplace=True)
    return df_clean

if __name__ == '__main__':
    df = pd.read_csv('raw_player_stats_today.csv')
    df_clean = transform_player_stats(df)
    df_clean.to_csv('player_stats_for_load_today.csv', index=False)
    print(df_clean.head())

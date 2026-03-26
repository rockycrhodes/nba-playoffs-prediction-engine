#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:29:06 2026

@author: rrhodes
"""

import pandas as pd

def transform_games(df):
    keep_cols = ['GAME_ID','SEASON','GAME_DATE','MATCHUP','TEAM_ID','TEAM_ABBREVIATION','PTS','WL','SEASON_TYPE']
    df = df[keep_cols]

    games = []
    for game_id, group in df.groupby('GAME_ID'):
        if group['TEAM_ID'].nunique() < 2:
            continue

        group = group.sort_values('GAME_DATE').drop_duplicates(subset=['TEAM_ID']).head(2)
        season_type = group.iloc[0]['SEASON_TYPE']

        row = {
            'game_id': game_id,
            'season': group.iloc[0]['SEASON'],
            'game_date': group.iloc[0]['GAME_DATE'],
            'home_team_id': None,
            'away_team_id': None,
            'home_points': None,
            'away_points': None,
            'winner': None,
            'play_in_flag': season_type == 'PlayIn',
            'playoff_flag': season_type == 'Playoffs',
            'season_type': season_type
        }

        for _, rec in group.iterrows():
            if 'vs.' in rec['MATCHUP']:
                row['home_team_id'] = rec['TEAM_ID']
                row['home_points'] = rec['PTS']
                if rec['WL'] == 'W':
                    row['winner'] = rec['TEAM_ID']
            elif '@' in rec['MATCHUP']:
                row['away_team_id'] = rec['TEAM_ID']
                row['away_points'] = rec['PTS']
                if rec['WL'] == 'W':
                    row['winner'] = rec['TEAM_ID']

        games.append(row)

    return pd.DataFrame(games)


if __name__ == '__main__':
    # Read to csv for bulk upload
    df = pd.read_csv('raw_games_bulk.csv')
    df_clean = transform_games(df)
    df_clean.to_csv('games_for_load_bulk.csv', index=False)
    print(df_clean.head())

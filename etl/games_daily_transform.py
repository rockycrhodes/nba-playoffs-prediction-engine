#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:29:06 2026

@author: rrhodes
"""

import pandas as pd

def transform_games(df):
    keep_cols = [
        'GAME_ID', 'SEASON', 'GAME_DATE', 'MATCHUP',
        'TEAM_ID', 'TEAM_ABBREVIATION', 'PTS', 'WL', 'SEASON_TYPE'
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    games = []
    for game_id, group in df.groupby('GAME_ID'):
        if len(group) == 2:
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
                'play_in_flag': False,
                'playoff_flag': False,
                'season_type': group.iloc[0]['SEASON_TYPE']
            }
            #Set Season Type flags
            if season_type == 'Playoffs':
                row['playoff_flag'] = True
            if season_type == 'PlayIn':
                row['play_in_flag'] = True
            
            # Put games on a single row with both teams
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
    df_games = pd.DataFrame(games)
    return df_games

if __name__ == '__main__':
    # For daily update
    df_today = pd.read_csv('raw_games_today.csv')
    df_today_clean = transform_games(df_today)
    df_today_clean.to_csv('games_for_load_today.csv', index=False)
    print("Transformed games:", df_today_clean.head())

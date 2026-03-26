#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:03:00 2026

@author: rrhodes
"""
import os
import pandas as pd

RAW_PATH = "raw_team_stats_today.csv"
OUT_PATH = "team_stats_for_load_today.csv"

EXPECTED_COLS = [
  "game_id","team_id","team_city","team_name","team_abbreviation",
  "minutes","fgm","fga","fg_pct","fg3m","fg3a","fg3_pct",
  "ftm","fta","ft_pct","oreb","dreb","reb","ast","stl","blk",
  "turnovers","pf","pts","plus_minus","season","season_type"
]

if (not os.path.exists(RAW_PATH)) or os.path.getsize(RAW_PATH) == 0:
    print("No team stats extracted today; skipping transform.")
    pd.DataFrame(columns=EXPECTED_COLS).to_csv(OUT_PATH, index=False)
    raise SystemExit(0)


def transform_team_stats(df):
    keep_cols = [
        # identifiers
        'gameId','teamId','teamCity','teamName','teamTricode',
        # boxscore team stats (camelCase)
        'minutes',
        'fieldGoalsMade','fieldGoalsAttempted','fieldGoalsPercentage',
        'threePointersMade','threePointersAttempted','threePointersPercentage',
        'freeThrowsMade','freeThrowsAttempted','freeThrowsPercentage',
        'reboundsOffensive','reboundsDefensive','reboundsTotal',
        'assists','steals','blocks','turnovers','foulsPersonal',
        'points','plusMinusPoints',
        # your added metadata
        'GAME_ID','SEASON','SEASON_TYPE'
    ]
    cols = [c for c in keep_cols if c in df.columns]
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
        'SEASON_TYPE': 'season_type',
        # (optional) if you prefer GAME_ID over gameId:
        # 'GAME_ID': 'game_id',
    }, inplace=True)

    # ensure final column order matches EXPECTED_COLS (optional but nice)
    df_clean = df_clean.reindex(columns=[c for c in EXPECTED_COLS if c in df_clean.columns])

    return df_clean


if __name__ == '__main__':
    df = pd.read_csv(RAW_PATH)
    df_clean = transform_team_stats(df)
    df_clean.to_csv(OUT_PATH, index=False)
    print(df_clean.head())


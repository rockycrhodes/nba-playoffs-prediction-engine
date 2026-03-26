#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:28:26 2026

@author: rrhodes
"""

from datetime import date, timedelta
import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

def infer_season_str(d: date) -> str:
    # NBA season generally starts in Oct; use Sept(9) as safe cutoff
    start_year = d.year if d.month >= 9 else d.year - 1
    return f"{start_year}-{str(start_year+1)[-2:]}"

def extract_games_for_date(target_date: date) -> pd.DataFrame:
    """
    Tries Regular Season, then PlayIn, then Playoffs for the given date,
    and returns the first non-empty LeagueGameLog dataframe found.
    Adds SEASON and SEASON_TYPE columns.
    """
    date_str = target_date.strftime('%m/%d/%Y')
    season = infer_season_str(target_date)

    for season_type in ['Regular Season', 'PlayIn', 'Playoffs']:
        glog = leaguegamelog.LeagueGameLog(
            date_from_nullable=date_str,
            date_to_nullable=date_str,
            season_type_all_star=season_type
        )
        df = glog.get_data_frames()[0]
        if df.shape[0] > 0:
            df['SEASON'] = season
            df['SEASON_TYPE'] = season_type
            return df

    # If nothing found, return empty df with expected columns so downstream won't crash
    empty = pd.DataFrame(columns=[
        'GAME_ID','SEASON','GAME_DATE','MATCHUP','TEAM_ID','TEAM_ABBREVIATION','PTS','WL','SEASON_TYPE'
    ])
    return empty

if __name__ == '__main__':
    # Typically we want "yesterday" since games finish late
    yday = date.today() - timedelta(days=1)

    df = extract_games_for_date(yday)
    df.to_csv('raw_games_today.csv', index=False)  # rename if your pipeline expects a different name
    print(f"Extracted games for {yday}: {df.shape}")


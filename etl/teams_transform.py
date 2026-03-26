#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 31 16:03:12 2025

@author: rrhodes
"""

import pandas as pd

def transform_teams(df):
    """
    Standardizes and prepares teams dataframe for loading.
    Keeps only key columns and renames them for DB schema consistency.
    """
    # Keep only needed fields
    cols = ['id', 'full_name', 'abbreviation', 'nickname', 'city', 'state', 'year_founded']
    df_clean = df[cols]
    # Rename for DB clarity
    df_clean = df_clean.rename(columns={
        'id': 'team_id',
        'full_name': 'team_name',
        'abbreviation': 'team_abbreviation',
        'nickname': 'team_nickname',
        'city': 'team_city',
        'state': 'team_state',
        'year_founded': 'team_year_founded'
    })
    # Drop duplicates, sort
    df_clean = df_clean.drop_duplicates(subset='team_id').sort_values('team_id').reset_index(drop=True)
    return df_clean

if __name__ == '__main__':
    df = pd.read_csv('raw_teams.csv')
    df_clean = transform_teams(df)
    df_clean.to_csv('teams_for_load.csv', index=False)
    print("Transformed teams:\n", df_clean)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:15:27 2026

@author: rrhodes
"""

import pandas as pd

def transform_players(df):
    """
    Prepares the players dataframe for loading to the DB.
    Keeps only key fields and renames them.
    """
    # Keep and rename relevant columns
    keep_cols = ['id', 'full_name', 'first_name', 'last_name', 'is_active']
    df_clean = df[keep_cols].rename(columns={
        'id': 'player_id',
        'full_name': 'player_name',
        'first_name': 'first_name',
        'last_name': 'last_name',
        'is_active': 'is_active'
    })
    # Remove duplicates just in case
    df_clean = df_clean.drop_duplicates(subset='player_id').sort_values('player_id').reset_index(drop=True)
    return df_clean

if __name__ == '__main__':
    df = pd.read_csv('raw_players.csv')
    df_clean = transform_players(df)
    df_clean.to_csv('players_for_load.csv', index=False)
    print("Transformed players:\n", df_clean.head())

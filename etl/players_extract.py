#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:15:08 2026

@author: rrhodes
"""

from nba_api.stats.static import players
import pandas as pd

def extract_nba_players():
    """
    Extracts NBA player metadata as a pandas DataFrame.
    """
    players_list = players.get_players()  # This returns all players (not just active ones)
    df = pd.DataFrame(players_list)
    return df

if __name__ == '__main__':
    df_players = extract_nba_players()
    # Save raw for exploration or backup
    df_players.to_csv('raw_players.csv', index=False)
    print("Extracted players:\n", df_players.head())

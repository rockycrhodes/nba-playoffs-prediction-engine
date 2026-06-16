#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 31 16:00:09 2025

@author: rrhodes
"""

from nba_api.stats.static import teams
import pandas as pd

def extract_nba_teams():
    """
    Extracts current NBA teams metadata as a pandas DataFrame.
    """
    teams_list = teams.get_teams()
    df = pd.DataFrame(teams_list)
    return df

if __name__ == '__main__':
    df_teams = extract_nba_teams()
    df_teams.to_csv('raw_teams.csv', index=False)
    print("Extracted teams:\n", df_teams.head())

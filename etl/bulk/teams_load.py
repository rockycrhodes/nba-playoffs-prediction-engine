#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 13:01:18 2026

@author: rrhodes
"""

import pandas as pd
import psycopg2

# Database connection parameters 
DB_NAME = 'nba_data'
DB_USER = 'postgres'         
DB_PASSWORD = 'yourpassword' 
DB_HOST = 'localhost'        
DB_PORT = '5432'             

def load_teams_to_db(df):
    """
    Loads teams DataFrame into PostgreSQL. Uses UPSERT (insert or update on conflict).
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    # Create table if it doesn't exist
    cur.execute('''
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        team_abbreviation TEXT,
        team_nickname TEXT,
        team_city TEXT,
        team_state TEXT,
        team_year_founded INTEGER
    );
    ''')
    # Upsert each row
    for _, row in df.iterrows():
        cur.execute('''
            INSERT INTO teams (
                team_id, team_name, team_abbreviation, team_nickname, team_city, team_state, team_year_founded
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id) DO UPDATE SET
                team_name = EXCLUDED.team_name,
                team_abbreviation = EXCLUDED.team_abbreviation,
                team_nickname = EXCLUDED.team_nickname,
                team_city = EXCLUDED.team_city,
                team_state = EXCLUDED.team_state,
                team_year_founded = EXCLUDED.team_year_founded;
        ''', (
            int(row['team_id']),
            row['team_name'],
            row['team_abbreviation'],
            row['team_nickname'],
            row['team_city'],
            row['team_state'],
            int(row['team_year_founded']) if not pd.isna(row['team_year_founded']) else None
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} teams into PostgreSQL database.")

if __name__ == '__main__':
    df_clean = pd.read_csv('teams_for_load.csv')
    load_teams_to_db(df_clean)

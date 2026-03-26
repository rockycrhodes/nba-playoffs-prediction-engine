#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:15:58 2026

@author: rrhodes
"""

import pandas as pd
import psycopg2

DB_NAME = 'nba_data'
DB_USER = 'postgres'
DB_PASSWORD = 'yourpassword'
DB_HOST = 'localhost'
DB_PORT = '5432'

def load_players_to_db(df):
    """
    Loads players DataFrame into PostgreSQL. Upserts by player_id.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    # Create table if it doesn't exists
    cur.execute('''
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        player_name TEXT,
        first_name TEXT,
        last_name TEXT,
        is_active BOOLEAN
    );
    ''')
    for _, row in df.iterrows():
        cur.execute('''
            INSERT INTO players (
                player_id, player_name, first_name, last_name, is_active
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_active = EXCLUDED.is_active;
        ''', (
            int(row['player_id']),
            row['player_name'],
            row['first_name'],
            row['last_name'],
            bool(row['is_active'])
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} players into PostgreSQL database.")

if __name__ == '__main__':
    df_clean = pd.read_csv('players_for_load.csv')
    load_players_to_db(df_clean)

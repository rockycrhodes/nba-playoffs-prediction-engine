#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:48:43 2026

@author: rrhodes
"""

import pandas as pd
import psycopg2

DB_NAME = 'nba_data'
DB_USER = 'postgres'
DB_PASSWORD = 'yourpassword'
DB_HOST = 'localhost'
DB_PORT = '5432'

def load_games_to_db(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS games (
        game_id VARCHAR PRIMARY KEY,
        season VARCHAR,
        game_date DATE,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_points INTEGER,
        away_points INTEGER,
        winner INTEGER,
        play_in_flag BOOLEAN,
        playoff_flag BOOLEAN,
        season_type TEXT
    );
    ''')
    for _, row in df.iterrows():
        cur.execute('''
            INSERT INTO games (
                game_id, season, game_date, home_team_id, away_team_id,
                home_points, away_points, winner, play_in_flag, playoff_flag, season_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                season = EXCLUDED.season,
                game_date = EXCLUDED.game_date,
                home_team_id = EXCLUDED.home_team_id,
                away_team_id = EXCLUDED.away_team_id,
                home_points = EXCLUDED.home_points,
                away_points = EXCLUDED.away_points,
                winner = EXCLUDED.winner,
                play_in_flag = EXCLUDED.play_in_flag,
                playoff_flag = EXCLUDED.playoff_flag,
                season_type = EXCLUDED.season_type;
        ''', (
            row['game_id'],
            row['season'],
            row['game_date'],
            int(row['home_team_id']) if not pd.isna(row['home_team_id']) else None,
            int(row['away_team_id']) if not pd.isna(row['away_team_id']) else None,
            int(row['home_points']) if not pd.isna(row['home_points']) else None,
            int(row['away_points']) if not pd.isna(row['away_points']) else None,
            int(row['winner']) if not pd.isna(row['winner']) else None,
            bool(row['play_in_flag']),
            bool(row['playoff_flag']),
            row['season_type']
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} games into PostgreSQL.")

if __name__ == '__main__':
    df_today = pd.read_csv('games_for_load_today.csv')
    load_games_to_db(df_today)

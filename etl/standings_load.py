#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 15:55:31 2026

@author: rrhodes
"""

import pandas as pd
import psycopg2

DB_NAME = 'nba_data'
DB_USER = 'postgres'
DB_PASSWORD = 'yourpassword'
DB_HOST = 'localhost'
DB_PORT = '5432'

def load_standings_to_db(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS standings (
        season_id VARCHAR,
        team_id INTEGER,
        team_city TEXT,
        team_name TEXT,
        conference TEXT,
        conference_record TEXT,
        playoff_rank INTEGER,
        clinch_indicator TEXT,
        division TEXT,
        division_record TEXT,
        division_rank INTEGER,
        wins INTEGER,
        losses INTEGER,
        win_pct NUMERIC,
        league_rank INTEGER,
        record TEXT,
        run_date DATE,
        PRIMARY KEY (season_id, team_id, run_date)
    );
    ''')
    for _, row in df.iterrows():
        cur.execute('''
            INSERT INTO standings (
                season_id, team_id, team_city, team_name, conference, conference_record, playoff_rank, clinch_indicator,
                division, division_record, division_rank, wins, losses, win_pct, league_rank, record, run_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (season_id, team_id, run_date) DO UPDATE SET
                team_city = EXCLUDED.team_city,
                team_name = EXCLUDED.team_name,
                conference = EXCLUDED.conference,
                conference_record = EXCLUDED.conference_record,
                playoff_rank = EXCLUDED.playoff_rank,
                clinch_indicator = EXCLUDED.clinch_indicator,
                division = EXCLUDED.division,
                division_record = EXCLUDED.division_record,
                division_rank = EXCLUDED.division_rank,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                win_pct = EXCLUDED.win_pct,
                league_rank = EXCLUDED.league_rank,
                record = EXCLUDED.record;
        ''', (
            row['SeasonID'],
            int(row['TeamID']),
            row['TeamCity'],
            row['TeamName'],
            row['Conference'],
            row['ConferenceRecord'],
            int(row['PlayoffRank']) if not pd.isna(row['PlayoffRank']) else None,
            row['ClinchIndicator'],
            row['Division'],
            row['DivisionRecord'],
            int(row['DivisionRank']) if not pd.isna(row['DivisionRank']) else None,
            int(row['WINS']) if not pd.isna(row['WINS']) else None,
            int(row['LOSSES']) if not pd.isna(row['LOSSES']) else None,
            float(row['WinPCT']) if not pd.isna(row['WinPCT']) else None,
            int(row['LeagueRank']) if not pd.isna(row['LeagueRank']) else None,
            row['Record'],
            row['RUN_DATE']
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} standings records into PostgreSQL.")

if __name__ == '__main__':
    df_clean = pd.read_csv('standings_for_load.csv')
    load_standings_to_db(df_clean)

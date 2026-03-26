#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  6 16:10:29 2026

@author: rrhodes
"""

import pandas as pd
import psycopg2

DB_NAME = 'nba_data'
DB_USER = 'postgres'
DB_PASSWORD = 'yourpassword'
DB_HOST = 'localhost'
DB_PORT = '5432'

def load_player_stats_to_db(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS player_game_stats (
        game_id VARCHAR,
        team_id INTEGER,
        player_id INTEGER,
        player_name TEXT,
        start_position TEXT,
        comment TEXT,
        minutes TEXT,
        fgm INTEGER,
        fga INTEGER,
        fg_pct NUMERIC,
        fg3m INTEGER,
        fg3a INTEGER,
        fg3_pct NUMERIC,
        ftm INTEGER,
        fta INTEGER,
        ft_pct NUMERIC,
        oreb INTEGER,
        dreb INTEGER,
        reb INTEGER,
        ast INTEGER,
        stl INTEGER,
        blk INTEGER,
        turnovers INTEGER,
        pf INTEGER,
        pts INTEGER,
        plus_minus NUMERIC,
        season VARCHAR,
        season_type TEXT,
        PRIMARY KEY (game_id, player_id)
    );
    ''')
    for _, row in df.iterrows():
        cur.execute('''
            INSERT INTO player_game_stats (
                game_id, team_id, player_id, player_name, start_position, comment, minutes,
                fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, plus_minus, season
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id, player_id) DO UPDATE SET
                team_id = EXCLUDED.team_id,
                player_name = EXCLUDED.player_name,
                start_position = EXCLUDED.start_position,
                comment = EXCLUDED.comment,
                minutes = EXCLUDED.minutes,
                fgm = EXCLUDED.fgm,
                fga = EXCLUDED.fga,
                fg_pct = EXCLUDED.fg_pct,
                fg3m = EXCLUDED.fg3m,
                fg3a = EXCLUDED.fg3a,
                fg3_pct = EXCLUDED.fg3_pct,
                ftm = EXCLUDED.ftm,
                fta = EXCLUDED.fta,
                ft_pct = EXCLUDED.ft_pct,
                oreb = EXCLUDED.oreb,
                dreb = EXCLUDED.dreb,
                reb = EXCLUDED.reb,
                ast = EXCLUDED.ast,
                stl = EXCLUDED.stl,
                blk = EXCLUDED.blk,
                turnovers = EXCLUDED.turnovers,
                pf = EXCLUDED.pf,
                pts = EXCLUDED.pts,
                plus_minus = EXCLUDED.plus_minus,
                season = EXCLUDED.season,
                season_type = EXCLUDED.season_type;
        ''', [
            row.get('game_id'),
            int(row.get('team_id', 0)) if not pd.isna(row.get('team_id')) else None,
            int(row.get('player_id', 0)) if not pd.isna(row.get('player_id')) else None,
            row.get('player_name'),
            row.get('start_position'),
            row.get('comment'),
            row.get('minutes'),
            int(row.get('fgm', 0)) if not pd.isna(row.get('fgm')) else None,
            int(row.get('fga', 0)) if not pd.isna(row.get('fga')) else None,
            float(row.get('fg_pct', 0)) if not pd.isna(row.get('fg_pct')) else None,
            int(row.get('fg3m', 0)) if not pd.isna(row.get('fg3m')) else None,
            int(row.get('fg3a', 0)) if not pd.isna(row.get('fg3a')) else None,
            float(row.get('fg3_pct', 0)) if not pd.isna(row.get('fg3_pct')) else None,
            int(row.get('ftm', 0)) if not pd.isna(row.get('ftm')) else None,
            int(row.get('fta', 0)) if not pd.isna(row.get('fta')) else None,
            float(row.get('ft_pct', 0)) if not pd.isna(row.get('ft_pct')) else None,
            int(row.get('oreb', 0)) if not pd.isna(row.get('oreb')) else None,
            int(row.get('dreb', 0)) if not pd.isna(row.get('dreb')) else None,
            int(row.get('reb', 0)) if not pd.isna(row.get('reb')) else None,
            int(row.get('ast', 0)) if not pd.isna(row.get('ast')) else None,
            int(row.get('stl', 0)) if not pd.isna(row.get('stl')) else None,
            int(row.get('blk', 0)) if not pd.isna(row.get('blk')) else None,
            int(row.get('turnovers', 0)) if not pd.isna(row.get('turnovers')) else None,
            int(row.get('pf', 0)) if not pd.isna(row.get('pf')) else None,
            int(row.get('pts', 0)) if not pd.isna(row.get('pts')) else None,
            float(row.get('plus_minus', 0)) if not pd.isna(row.get('plus_minus')) else None,
            row.get('season'),
            row.get('season_type')
        ])
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} player game stats records into PostgreSQL.")

if __name__ == '__main__':
    df_clean = pd.read_csv('player_stats_for_load_today.csv')
    load_player_stats_to_db(df_clean)

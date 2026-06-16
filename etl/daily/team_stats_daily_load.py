#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import psycopg2

DB_NAME = "nba_data"
DB_USER = "postgres"
DB_PASSWORD = "yourpassword"
DB_HOST = "localhost"
DB_PORT = "5432"

def load_team_stats_to_db(df: pd.DataFrame) -> None:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cur = conn.cursor()

    # IMPORTANT: minutes is TEXT because values look like "240:00"
    cur.execute("""
    CREATE TABLE IF NOT EXISTS team_game_stats (
        game_id VARCHAR,
        team_id INTEGER,
        team_city TEXT,
        team_name TEXT,
        team_abbreviation TEXT,
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
        PRIMARY KEY (game_id, team_id)
    );
    """)

    sql = """
    INSERT INTO team_game_stats (
        game_id, team_id, team_city, team_name, team_abbreviation,
        minutes, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk,
        turnovers, pf, pts, plus_minus, season, season_type
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (game_id, team_id) DO UPDATE SET
        team_city = EXCLUDED.team_city,
        team_name = EXCLUDED.team_name,
        team_abbreviation = EXCLUDED.team_abbreviation,
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
    """

    for _, row in df.iterrows():
        cur.execute(sql, [
            row.get("game_id"),
            int(row["team_id"]) if not pd.isna(row.get("team_id")) else None,
            row.get("team_city"),
            row.get("team_name"),
            row.get("team_abbreviation"),
            row.get("minutes"),  # keep as TEXT like "240:00"
            int(row["fgm"]) if not pd.isna(row.get("fgm")) else None,
            int(row["fga"]) if not pd.isna(row.get("fga")) else None,
            float(row["fg_pct"]) if not pd.isna(row.get("fg_pct")) else None,
            int(row["fg3m"]) if not pd.isna(row.get("fg3m")) else None,
            int(row["fg3a"]) if not pd.isna(row.get("fg3a")) else None,
            float(row["fg3_pct"]) if not pd.isna(row.get("fg3_pct")) else None,
            int(row["ftm"]) if not pd.isna(row.get("ftm")) else None,
            int(row["fta"]) if not pd.isna(row.get("fta")) else None,
            float(row["ft_pct"]) if not pd.isna(row.get("ft_pct")) else None,
            int(row["oreb"]) if not pd.isna(row.get("oreb")) else None,
            int(row["dreb"]) if not pd.isna(row.get("dreb")) else None,
            int(row["reb"]) if not pd.isna(row.get("reb")) else None,
            int(row["ast"]) if not pd.isna(row.get("ast")) else None,
            int(row["stl"]) if not pd.isna(row.get("stl")) else None,
            int(row["blk"]) if not pd.isna(row.get("blk")) else None,
            int(row["turnovers"]) if not pd.isna(row.get("turnovers")) else None,
            int(row["pf"]) if not pd.isna(row.get("pf")) else None,
            int(row["pts"]) if not pd.isna(row.get("pts")) else None,
            float(row["plus_minus"]) if not pd.isna(row.get("plus_minus")) else None,
            row.get("season"),
            row.get("season_type"),
        ])

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(df)} team game stats records into PostgreSQL.")

if __name__ == "__main__":
    df_clean = pd.read_csv("team_stats_for_load_today.csv")
    load_team_stats_to_db(df_clean)

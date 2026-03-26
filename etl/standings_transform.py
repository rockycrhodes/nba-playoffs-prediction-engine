#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

def transform_standings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter and standardize standings fields for DB load.
    Accepts either RUN_DATE or run_date from different extract scripts.
    """
    # Normalize run_date column name
    if "RUN_DATE" not in df.columns and "run_date" in df.columns:
        df = df.rename(columns={"run_date": "RUN_DATE"})

    # (Optional) keep season string if present (useful for joins)
    base_cols = [
        "SeasonID",
        "TeamID",
        "TeamCity",
        "TeamName",
        "Conference",
        "ConferenceRecord",
        "PlayoffRank",
        "ClinchIndicator",
        "Division",
        "DivisionRecord",
        "DivisionRank",
        "WINS",
        "LOSSES",
        "WinPCT",
        "LeagueRank",
        "Record",
        "RUN_DATE",
    ]
    keep_cols = base_cols + (["season"] if "season" in df.columns else [])

    # Only select columns that exist (extra safety)
    keep_cols = [c for c in keep_cols if c in df.columns]
    df_clean = df.loc[:, keep_cols].copy()

    # Ensure types are correct
    for c in ["PlayoffRank", "WINS", "LOSSES", "WinPCT", "DivisionRank", "LeagueRank"]:
        if c in df_clean.columns:
            df_clean.loc[:, c] = pd.to_numeric(df_clean[c], errors="coerce")

    # Optional: parse RUN_DATE as date
    if "RUN_DATE" in df_clean.columns:
        df_clean.loc[:, "RUN_DATE"] = pd.to_datetime(df_clean["RUN_DATE"], errors="coerce").dt.date

    return df_clean

if __name__ == "__main__":
    df = pd.read_csv("raw_standings_backfill.csv")
    df_clean = transform_standings(df)
    df_clean.to_csv("standings_for_load.csv", index=False)
    print(df_clean.head())


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
next_game_champion_lr_scaled_l2_injury_travel.py

CHAMPION MODEL (current):
- Dataset: public.v_game_training_nextgame_adv_injury_travel
- Model: StandardScaler + L2 Logistic Regression
- Time-safe tuning: choose C on a calibration split that is the LAST 20% of training games
  (training games = all seasons < test_season; ordered by game_date, game_id)
- Evaluation: rolling backtest by season (train < test)

Outputs:
- ./outputs_nextgame/metrics_<EXPERIMENT>.csv
- ./outputs_nextgame/pred_<EXPERIMENT>_seasonYYYY.csv
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score

# -----------------------
# CONFIG
# -----------------------
DB_URL = "postgresql+psycopg2://postgres:password@localhost:5432/nba_data"
DATASET_VIEW = "public.v_game_training_nextgame_adv_injury_travel"

EXPERIMENT = "ng_champion_lr_scaled_l2_injury_travel"
OUT_DIR = "./outputs_nextgame"
os.makedirs(OUT_DIR, exist_ok=True)

CAL_SPLIT_FRAC = 0.80
MIN_TRAIN_ROWS = 500
C_GRID = [0.1, 0.3, 1.0, 3.0, 10.0]
MAX_ITER = 20000

FEATURES = [
    # base
    "elo_diff",
    "rest_adv_days", "home_is_b2b", "away_is_b2b",
    "win_pct_l10_diff", "margin_l10_diff",

    "net_rtg_l10_diff", "efg_l10_diff", "ts_l10_diff",
    "tov_l10_diff", "oreb_l10_diff", "dreb_l10_diff",

    "net_rtg_s2d_diff", "efg_s2d_diff", "ts_s2d_diff",
    "tov_s2d_diff", "oreb_s2d_diff", "dreb_s2d_diff",

    # injury proxies (diffs)
    "core_games_last5_adv",
    "core_minutes_last5_adv",

    # travel (diffs)
    "travel_miles_adv",
    "is_travel_game_adv",
]

# -----------------------
# Helpers
# -----------------------
def fetch_seasons(engine):
    with engine.connect() as conn:
        return pd.read_sql(
            text(f"select distinct season_start from {DATASET_VIEW} order by season_start;"),
            conn
        )["season_start"].astype(int).tolist()

def load_train_test(engine, test_season, cols):
    col_sql = ", ".join(cols)
    base_cols = f"game_id, game_date, season_start, home_team_id, away_team_id, home_win, {col_sql}"

    train_q = text(f"""
        select {base_cols}
        from {DATASET_VIEW}
        where season_start < :test_season
        order by game_date, game_id
    """)
    test_q = text(f"""
        select {base_cols}
        from {DATASET_VIEW}
        where season_start = :test_season
        order by game_date, game_id
    """)

    with engine.connect() as conn:
        tr = pd.read_sql(train_q, conn, params={"test_season": int(test_season)})
        te = pd.read_sql(test_q, conn, params={"test_season": int(test_season)})
    return tr, te

def time_split(df, frac=0.8):
    df = df.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    cut = int(len(df) * frac)
    cut = max(min(cut, len(df) - 1), 1)
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()

def prepare_Xy(df):
    X = df[FEATURES].copy().fillna(0.0).to_numpy()
    y = df["home_win"].astype(int).to_numpy()
    return X, y

def make_lr(C):
    return Pipeline(steps=[
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(
            penalty="l2",
            C=float(C),
            solver="lbfgs",
            max_iter=MAX_ITER
        ))
    ])

# -----------------------
# Main
# -----------------------
def main():
    engine = create_engine(DB_URL)
    seasons = fetch_seasons(engine)
    if len(seasons) < 2:
        raise RuntimeError("Need at least 2 seasons to run rolling backtest.")

    metrics = []
    pred_files = []

    for test_season in seasons[1:]:
        tr, te = load_train_test(engine, test_season, sorted(set(FEATURES)))
        if len(tr) < MIN_TRAIN_ROWS or len(te) == 0:
            continue

        tr_fit, tr_cal = time_split(tr, CAL_SPLIT_FRAC)
        X_fit, y_fit = prepare_Xy(tr_fit)
        X_cal, y_cal = prepare_Xy(tr_cal)
        X_te, y_te = prepare_Xy(te)

        # Tune C on cal split (time-safe)
        best = None
        for C in C_GRID:
            clf = make_lr(C)
            clf.fit(X_fit, y_fit)
            p_cal = clf.predict_proba(X_cal)[:, 1]
            ll = log_loss(y_cal, p_cal)
            if best is None or ll < best["cal_ll"]:
                best = {"C": float(C), "cal_ll": float(ll), "clf": clf}

        clf = best["clf"]
        p = clf.predict_proba(X_te)[:, 1]

        metrics.append({
            "experiment": EXPERIMENT,
            "model": "LR_scaled_L2_injury_travel",
            "test_season": int(test_season),
            "n": int(len(y_te)),
            "log_loss": float(log_loss(y_te, p)),
            "brier": float(brier_score_loss(y_te, p)),
            "auc": float(roc_auc_score(y_te, p)),
            "mean_p": float(np.mean(p)),
            "mean_y": float(np.mean(y_te)),
            "C_star": best["C"],
            "cal_ll": best["cal_ll"],
        })

        pred = te[["game_id","game_date","season_start","home_team_id","away_team_id","home_win"]].copy()
        pred["experiment"] = EXPERIMENT
        pred["model"] = "LR_scaled_L2_injury_travel"
        pred["p_home_win"] = p
        pred["C_star"] = best["C"]
        pred_path = os.path.join(OUT_DIR, f"pred_{EXPERIMENT}_season{int(test_season)}.csv")
        pred.to_csv(pred_path, index=False)
        pred_files.append(pred_path)

    metrics_df = pd.DataFrame(metrics).sort_values("test_season")
    metrics_path = os.path.join(OUT_DIR, f"metrics_{EXPERIMENT}.csv")
    metrics_df.to_csv(metrics_path, index=False)

    print("Wrote:", metrics_path)
    for pth in pred_files[:5]:
        print("Wrote:", pth)
    if len(pred_files) > 5:
        print(f"Wrote: ... ({len(pred_files)} prediction files total)")

    print("\n=== Metrics (per season) ===")
    print(metrics_df.to_string(index=False))

    w = metrics_df["n"].to_numpy()
    print("\n=== Weighted overall ===")
    print({
        "log_loss": float(np.average(metrics_df["log_loss"], weights=w)),
        "brier": float(np.average(metrics_df["brier"], weights=w)),
        "auc": float(np.average(metrics_df["auc"], weights=w)),
    })

if __name__ == "__main__":
    main()

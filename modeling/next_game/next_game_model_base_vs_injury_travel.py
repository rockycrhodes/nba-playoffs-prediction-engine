#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score

DB_URL = "postgresql+psycopg2://postgres:password@localhost:5432/nba_data"
EXPERIMENT = "ng_v14_injury_travel_ab"
OUT_DIR = "./outputs_nextgame"
os.makedirs(OUT_DIR, exist_ok=True)

CAL_SPLIT_FRAC = 0.80
MIN_TRAIN_ROWS = 500
C_GRID = [0.1, 0.3, 1.0, 3.0, 10.0]
MAX_ITER = 20000

DATASETS = {
  "base": "public.v_game_training_nextgame_adv",
  "injury_travel": "public.v_game_training_nextgame_adv_injury_travel",
}

BASE_FEATURES = [
    "elo_diff",
    "rest_adv_days", "home_is_b2b", "away_is_b2b",
    "win_pct_l10_diff", "margin_l10_diff",
    "net_rtg_l10_diff", "efg_l10_diff", "ts_l10_diff",
    "tov_l10_diff", "oreb_l10_diff", "dreb_l10_diff",
    "net_rtg_s2d_diff", "efg_s2d_diff", "ts_s2d_diff",
    "tov_s2d_diff", "oreb_s2d_diff", "dreb_s2d_diff",
]

NEW_FEATURES = [
    "core_games_last5_adv",
    "core_minutes_last5_adv",
    "travel_miles_adv",
    "is_travel_game_adv",
]

def fetch_seasons(engine, view):
    with engine.connect() as conn:
        return pd.read_sql(text(f"select distinct season_start from {view} order by season_start;"), conn)["season_start"].astype(int).tolist()

def load_train_test(engine, view, test_season, cols):
    col_sql = ", ".join(cols)
    base_cols = f"game_id, game_date, season_start, home_team_id, away_team_id, home_win, {col_sql}"
    train_q = text(f"""
        select {base_cols} from {view}
        where season_start < :test_season
        order by game_date, game_id
    """)
    test_q = text(f"""
        select {base_cols} from {view}
        where season_start = :test_season
        order by game_date, game_id
    """)
    with engine.connect() as conn:
        tr = pd.read_sql(train_q, conn, params={"test_season": int(test_season)})
        te = pd.read_sql(test_q, conn, params={"test_season": int(test_season)})
    return tr, te

def time_split(df, frac=0.8):
    df = df.sort_values(["game_date","game_id"]).reset_index(drop=True)
    cut = int(len(df)*frac)
    cut = max(min(cut, len(df)-1), 1)
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()

def prepare_Xy(df, features):
    X = df[features].copy().fillna(0.0).to_numpy()
    y = df["home_win"].astype(int).to_numpy()
    return X, y

def make_lr(C):
    return Pipeline(steps=[
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(penalty="l2", C=float(C), solver="lbfgs", max_iter=MAX_ITER))
    ])

def run(engine, label, view, features):
    seasons = fetch_seasons(engine, view)
    rows = []
    for test_season in seasons[1:]:
        tr, te = load_train_test(engine, view, test_season, sorted(set(features)))
        if len(tr) < MIN_TRAIN_ROWS or len(te) == 0:
            continue
        tr_fit, tr_cal = time_split(tr, CAL_SPLIT_FRAC)
        X_fit, y_fit = prepare_Xy(tr_fit, features)
        X_cal, y_cal = prepare_Xy(tr_cal, features)
        X_te, y_te = prepare_Xy(te, features)

        best = None
        for C in C_GRID:
            clf = make_lr(C)
            clf.fit(X_fit, y_fit)
            p_cal = clf.predict_proba(X_cal)[:, 1]
            ll = log_loss(y_cal, p_cal)
            if best is None or ll < best["cal_ll"]:
                best = {"C": float(C), "cal_ll": float(ll), "clf": clf}

        p = best["clf"].predict_proba(X_te)[:, 1]
        rows.append({
            "experiment": EXPERIMENT,
            "dataset": label,
            "view": view,
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
    return pd.DataFrame(rows)

def main():
    engine = create_engine(DB_URL)

    df_base = run(engine, "base", DATASETS["base"], BASE_FEATURES)
    df_it = run(engine, "injury_travel", DATASETS["injury_travel"], BASE_FEATURES + NEW_FEATURES)

    out = pd.concat([df_base, df_it], ignore_index=True).sort_values(["dataset","test_season"])
    path = os.path.join(OUT_DIR, f"metrics_{EXPERIMENT}.csv")
    out.to_csv(path, index=False)
    print("Wrote:", path)
    print(out.to_string(index=False))

    print("\n=== Weighted overall ===")
    summ = []
    for ds, g in out.groupby("dataset"):
        w = g["n"].to_numpy()
        summ.append({
            "dataset": ds,
            "n_total": int(g["n"].sum()),
            "log_loss": float(np.average(g["log_loss"], weights=w)),
            "brier": float(np.average(g["brier"], weights=w)),
            "auc": float(np.average(g["auc"], weights=w)),
        })
    print(pd.DataFrame(summ).sort_values("log_loss").to_string(index=False))

if __name__ == "__main__":
    main()


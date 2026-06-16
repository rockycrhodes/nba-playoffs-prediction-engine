#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
next_game_model_backtest_calibration.py

Changes made:
A) Convergence fix + fair comparison:
   - Uses StandardScaler + L2-regularized LogisticRegression in a Pipeline
   - Increases max_iter
B) Feature-set improvement test (Option 1):
   - Adds model: M5_adv_l10_plus_s2d (drops L5 + drops pace/poss to reduce redundancy)
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score
from scipy.special import expit, logit
from scipy.optimize import minimize

# -----------------------
# CONFIG
# -----------------------
DB_URL = "postgresql+psycopg2://postgres:password@localhost:5432/nba_data"
DATASET_VIEW = "public.v_game_training_nextgame_adv"
BINS = 10
APPLY_TEMPERATURE_SCALING = False
CAL_SPLIT_FRAC = 0.80
MIN_TRAIN_ROWS = 500

# Versioning tag + output folder
EXPERIMENT = "ng_v5_scaled_l2_m5_l10_plus_s2d"  # <-- change this each run
OUT_DIR = "./outputs_nextgame"
os.makedirs(OUT_DIR, exist_ok=True)

# LR config (scaled + regularized)
LR_C = 1.0
LR_MAX_ITER = 10000

# -----------------------
# Calibration utilities
# -----------------------
def calibration_table_and_ece(y, p, bins=10):
    y = np.asarray(y).astype(int)
    p = np.asarray(p).astype(float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    b = np.digitize(p, edges[1:-1], right=True)  # 0..bins-1

    out = []
    ece = 0.0
    n = len(y)
    for k in range(bins):
        mask = (b == k)
        nk = int(mask.sum())
        if nk == 0:
            continue
        avg_p = float(p[mask].mean())
        win_rate = float(y[mask].mean())
        w = nk / n
        ece += w * abs(avg_p - win_rate)
        out.append({"bin": k, "n": nk, "avg_p": avg_p, "win_rate": win_rate, "abs_gap": abs(avg_p - win_rate)})
    return pd.DataFrame(out), float(ece)

def fit_temperature(y_cal, p_cal, clip=1e-6):
    y_cal = np.asarray(y_cal).astype(int)
    p_cal = np.clip(np.asarray(p_cal).astype(float), clip, 1 - clip)
    z = logit(p_cal)

    def obj(logT):
        T = np.exp(logT[0])
        p = expit(z / T)
        eps = 1e-15
        return -np.mean(y_cal * np.log(p + eps) + (1 - y_cal) * np.log(1 - p + eps))

    res = minimize(obj, x0=np.array([np.log(1.5)]), method="Nelder-Mead")
    return float(np.exp(res.x[0]))

def apply_temperature(p, T, clip=1e-6):
    p = np.clip(np.asarray(p).astype(float), clip, 1 - clip)
    return expit(logit(p) / T)

# -----------------------
# Model definitions
# -----------------------
MODEL_SPECS = {
    # Existing model (kept for continuity) - includes pace/poss
    "M3_adv_l10": [
        "elo_diff",
        "rest_adv_days", "home_is_b2b", "away_is_b2b",
        "win_pct_l10_diff", "margin_l10_diff",
        "net_rtg_l10_diff", "efg_l10_diff", "ts_l10_diff",
        "tov_l10_diff", "oreb_l10_diff", "dreb_l10_diff",
        "pace_l10_diff", "poss_l10_diff",
    ],

    # Existing "kitchen sink" (kept so you can verify scaling+regularization behavior)
    "M4_adv_l10_l5_s2d": [
        "elo_diff",
        "rest_adv_days", "home_is_b2b", "away_is_b2b",
        "win_pct_l10_diff", "margin_l10_diff",

        # L10 advanced
        "net_rtg_l10_diff", "efg_l10_diff", "ts_l10_diff",
        "tov_l10_diff", "oreb_l10_diff", "dreb_l10_diff",
        "pace_l10_diff", "poss_l10_diff",

        # L5 advanced
        "net_rtg_l5_diff", "efg_l5_diff", "ts_l5_diff",
        "tov_l5_diff", "oreb_l5_diff", "dreb_l5_diff",
        "pace_l5_diff", "poss_l5_diff",

        # S2D advanced
        "net_rtg_s2d_diff", "efg_s2d_diff", "ts_s2d_diff",
        "tov_s2d_diff", "oreb_s2d_diff", "dreb_s2d_diff",
        "pace_s2d_diff", "poss_s2d_diff",
    ],

    # Option 1 (recommended): keep L10 + S2D, drop L5, drop pace/poss to reduce redundancy/noise
    "M5_adv_l10_plus_s2d": [
        "elo_diff",
        "rest_adv_days", "home_is_b2b", "away_is_b2b",
        "win_pct_l10_diff", "margin_l10_diff",

        # L10 advanced
        "net_rtg_l10_diff", "efg_l10_diff", "ts_l10_diff",
        "tov_l10_diff", "oreb_l10_diff", "dreb_l10_diff",

        # S2D advanced
        "net_rtg_s2d_diff", "efg_s2d_diff", "ts_s2d_diff",
        "tov_s2d_diff", "oreb_s2d_diff", "dreb_s2d_diff",
    ],
}

def make_scaled_l2_lr():
    return Pipeline(steps=[
        ("scaler", StandardScaler(with_mean=True, with_std=True)),
        ("lr", LogisticRegression(
            penalty="l2",
            C=LR_C,
            solver="lbfgs",
            max_iter=LR_MAX_ITER
        ))
    ])

def fetch_seasons(engine):
    q = text(f"select distinct season_start from {DATASET_VIEW} order by season_start;")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)["season_start"].astype(int).tolist()

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

def prepare_Xy(df, feature_cols):
    X = df[feature_cols].copy().fillna(0.0)
    y = df["home_win"].astype(int).to_numpy()
    return X.to_numpy(), y

def fit_predict_fold(tr, te, feature_cols):
    tr = tr.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    cut = int(len(tr) * CAL_SPLIT_FRAC)
    cut = max(min(cut, len(tr) - 1), 1)

    tr_fit = tr.iloc[:cut]
    tr_cal = tr.iloc[cut:]

    X_fit, y_fit = prepare_Xy(tr_fit, feature_cols)
    X_cal, y_cal = prepare_Xy(tr_cal, feature_cols)
    X_te, y_te = prepare_Xy(te, feature_cols)

    clf = make_scaled_l2_lr()
    clf.fit(X_fit, y_fit)

    p_te_raw = clf.predict_proba(X_te)[:, 1]
    p_te = p_te_raw
    T = None

    if APPLY_TEMPERATURE_SCALING and len(tr_cal) >= 200:
        p_cal_raw = clf.predict_proba(X_cal)[:, 1]
        T = fit_temperature(y_cal, p_cal_raw)
        p_te = apply_temperature(p_te_raw, T)

    return clf, p_te_raw, p_te, y_te, T

def main():
    engine = create_engine(DB_URL)
    seasons = fetch_seasons(engine)
    if len(seasons) < 2:
        raise RuntimeError("Need at least 2 seasons to do rolling backtest.")

    metrics_rows = []
    cal_tables = []
    pred_files = []

    for test_season in seasons[1:]:
        for model_name, feat_cols in MODEL_SPECS.items():
            tr, te = load_train_test(engine, test_season, feat_cols)

            if len(tr) < MIN_TRAIN_ROWS or len(te) == 0:
                continue

            clf, p_raw, p, y, T = fit_predict_fold(tr, te, feat_cols)

            # metrics
            m = {
                "experiment": EXPERIMENT,
                "model": model_name,
                "test_season": int(test_season),
                "n": int(len(y)),
                "log_loss_raw": float(log_loss(y, p_raw)),
                "brier_raw": float(brier_score_loss(y, p_raw)),
                "auc_raw": float(roc_auc_score(y, p_raw)),
                "mean_p_raw": float(np.mean(p_raw)),
                "log_loss": float(log_loss(y, p)),
                "brier": float(brier_score_loss(y, p)),
                "auc": float(roc_auc_score(y, p)),
                "mean_p": float(np.mean(p)),
                "mean_y": float(np.mean(y)),
                "ece": float(calibration_table_and_ece(y, p, bins=BINS)[1]),
                "T": (None if T is None else float(T)),
                # Coefs are inside pipeline; omit alpha/beta to avoid confusion
                "lr_C": LR_C,
            }
            metrics_rows.append(m)

            # calibration table
            cal_df, ece = calibration_table_and_ece(y, p, bins=BINS)
            cal_df["experiment"] = EXPERIMENT
            cal_df["model"] = model_name
            cal_df["test_season"] = int(test_season)
            cal_tables.append(cal_df)

            # predictions CSV
            pred_df = te[["game_id", "game_date", "season_start", "home_team_id", "away_team_id", "home_win"]].copy()
            pred_df["experiment"] = EXPERIMENT
            pred_df["model"] = model_name
            pred_df["p_home_win_raw"] = p_raw
            pred_df["p_home_win"] = p
            pred_df["T"] = T

            pred_path = os.path.join(
                OUT_DIR,
                f"pred_{EXPERIMENT}_{model_name}_season{int(test_season)}.csv"
            )
            pred_df.to_csv(pred_path, index=False)
            pred_files.append(pred_path)

    metrics_df = pd.DataFrame(metrics_rows).sort_values(["model", "test_season"])
    metrics_path = os.path.join(OUT_DIR, f"metrics_{EXPERIMENT}.csv")
    metrics_df.to_csv(metrics_path, index=False)

    cal_all = pd.concat(cal_tables, ignore_index=True) if cal_tables else pd.DataFrame()
    cal_path = os.path.join(OUT_DIR, f"calibration_{EXPERIMENT}.csv")
    cal_all.to_csv(cal_path, index=False)

    print("\nWrote:")
    print(" -", metrics_path)
    print(" -", cal_path)
    for pth in pred_files[:5]:
        print(" -", pth)
    if len(pred_files) > 5:
        print(f" - ... ({len(pred_files)} prediction files total)")

    print("\n=== Metrics (per season) ===")
    print(metrics_df.to_string(index=False))

    print("\n=== Weighted overall by model (weights=n) ===")
    out = []
    for model_name, g in metrics_df.groupby("model"):
        w = g["n"].to_numpy()
        out.append({
            "experiment": EXPERIMENT,
            "model": model_name,
            "n_total": int(g["n"].sum()),
            "log_loss": float(np.average(g["log_loss"], weights=w)),
            "brier": float(np.average(g["brier"], weights=w)),
            "auc": float(np.average(g["auc"], weights=w)),
            "ece": float(np.average(g["ece"], weights=w)),
        })
    print(pd.DataFrame(out).sort_values("log_loss").to_string(index=False))

if __name__ == "__main__":
    main()

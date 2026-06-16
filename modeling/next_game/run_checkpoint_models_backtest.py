#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_checkpoint_models_backtest.py

Rolling backtest for playoff qualification checkpoints.
Includes baselines for:
- Base rate
- 1D LR on avg_point_diff_s2d
- 1D LR on win_pct_s2d
- 2D LR on win_pct_s2d + win_pct_l10
- 1D LR on elo_pre
- 1D LR on net_rtg_s2d
- LR on (win_pct_s2d + avg_point_diff_s2d + net_rtg_s2d + elo_pre) when available
Plus full-feature LR / RF / GB.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score, log_loss, accuracy_score, brier_score_loss
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier


# -----------------------
# CONFIG: DB
# -----------------------
DB_USER = os.getenv("NBA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("NBA_DB_HOST", "localhost")
DB_PORT = os.getenv("NBA_DB_PORT", "5432")
DB_NAME = os.getenv("NBA_DB_NAME", "nba_data")

TARGET_COL = "made_playoffs"
META_COLS = ["season", "team_id", "team_conference"]

N_CAL_BINS = 10
RANDOM_STATE = 42


# -----------------------
# CONFIG: Experiment selector
# -----------------------
EXPERIMENT_TAG = "v5_new_baselines"

CHECKPOINT_VIEWS = {
    20: "model_v5_checkpoint_20",
    40: "model_v5_checkpoint_40",
    60: "model_v5_checkpoint_60",
}

FEATURES = [
    # baselines / team strength
    "win_pct_s2d", "win_pct_l10",
    "avg_point_diff_s2d", "avg_point_diff_l10",

    # v1 style
    "avg_pts_l10", "avg_ast_l10", "avg_reb_l10", "avg_tov_l10",
    "is_home",

    # engineered rate/efficiency
    "efg_s2d", "efg_l10",
    "ts_s2d", "ts_l10",
    "fg3a_rate_s2d", "fg3a_rate_l10",
    "fta_rate_s2d", "fta_rate_l10",
    "tov_rate_s2d", "tov_rate_l10",

    # new features
    "elo_pre",
    "net_rtg_s2d", "net_rtg_l10",
    "off_rtg_s2d", "off_rtg_l10",
    "def_rtg_s2d", "def_rtg_l10",
]

BACKTEST_TEST_SEASONS = ["2022-23", "2023-24", "2024-25", "2025-26"]
ALL_SEASONS_ORDERED = ["2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]


# -----------------------
# Helpers
# -----------------------
def outname(base: str) -> str:
    return f"{base}.csv" if EXPERIMENT_TAG is None else f"{base}_{EXPERIMENT_TAG}.csv"

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def load_checkpoint(view_name: str, engine) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {view_name};", engine)
    df[TARGET_COL] = df[TARGET_COL].astype(int)

    # enforce is_home numeric if present
    if "is_home" in df.columns:
        df["is_home"] = df["is_home"].fillna(0).astype(int)

    required = set(FEATURES + META_COLS + [TARGET_COL])
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{view_name} missing columns: {missing}")

    return df

def safe_auc(y_true, y_proba):
    return roc_auc_score(y_true, y_proba) if len(np.unique(y_true)) > 1 else np.nan

def top8_by_conference_hit_rate(df_preds: pd.DataFrame) -> dict:
    out_rows = []
    for (season, conf), g in df_preds.groupby(["season", "team_conference"], dropna=False):
        g = g.sort_values("y_prob", ascending=False).head(8)
        out_rows.append({"season": season, "conf": conf, "hits": int(g["y_true"].sum()), "n": len(g)})
    out = pd.DataFrame(out_rows)
    total_hits = int(out["hits"].sum())
    total_picks = int(out["n"].sum())
    return {
        "top8_conf_hits": total_hits,
        "top8_conf_picks": total_picks,
        "top8_conf_hit_rate": (total_hits / total_picks) if total_picks else np.nan,
    }

def top16_hit_rate(df_preds: pd.DataFrame) -> dict:
    g = df_preds.sort_values("y_prob", ascending=False).head(16)
    hits = int(g["y_true"].sum())
    return {"top16_hits": hits, "top16_hit_rate": hits / 16.0}

def calibration_bins_df(y_true, y_proba, n_bins=10) -> pd.DataFrame:
    dfc = pd.DataFrame({"y_true": y_true, "y_prob": y_proba})
    dfc["bin"] = pd.cut(dfc["y_prob"], bins=np.linspace(0, 1, n_bins + 1), include_lowest=True)
    out = (
        dfc.groupby("bin", observed=True)
           .agg(pred_mean=("y_prob", "mean"), actual_rate=("y_true", "mean"), n=("y_true", "size"))
           .reset_index()
    )
    return out

def build_preprocessor():
    numeric_transform = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])
    return ColumnTransformer([("num", numeric_transform, FEATURES)], remainder="drop")

def evaluate_block(model_name, checkpoint, test_season, y_true, y_prob, y_pred, df_preds):
    return {
        "checkpoint": checkpoint,
        "model": model_name,
        "test_season": test_season,
        "n_test": len(y_true),
        "auc": safe_auc(y_true, y_prob),
        "logloss": log_loss(y_true, y_prob, labels=[0, 1]),
        "accuracy": accuracy_score(y_true, y_pred),
        "brier": brier_score_loss(y_true, y_prob),
        **top16_hit_rate(df_preds),
        **top8_by_conference_hit_rate(df_preds),
    }


# -----------------------
# Main rolling backtest
# -----------------------
def main():
    engine = get_engine()
    preprocessor = build_preprocessor()

    all_results, all_cal, all_preds = [], [], []

    # Pre-load checkpoint data once
    checkpoint_data = {cp: load_checkpoint(view, engine) for cp, view in CHECKPOINT_VIEWS.items()}

    # Rolling folds
    for test_season in BACKTEST_TEST_SEASONS:
        idx = ALL_SEASONS_ORDERED.index(test_season)
        train_seasons = ALL_SEASONS_ORDERED[:idx]

        for checkpoint, df in checkpoint_data.items():
            train_df = df[df["season"].isin(train_seasons)].copy()
            test_df = df[df["season"] == test_season].copy()

            X_train = train_df[FEATURES].copy()
            y_train = train_df[TARGET_COL].copy()
            X_test = test_df[FEATURES].copy()
            y_test = test_df[TARGET_COL].copy()

            meta_test = test_df[META_COLS + [TARGET_COL]].copy().rename(columns={TARGET_COL: "y_true"})
            meta_test["checkpoint"] = checkpoint
            meta_test["test_season"] = test_season

            def record_probs(model_name: str, prob: np.ndarray):
                pred = (prob >= 0.5).astype(int)
                dfp = meta_test.copy()
                dfp["model"] = model_name
                dfp["y_prob"] = prob
                all_preds.append(dfp)

                all_results.append(evaluate_block(model_name, checkpoint, test_season, y_test, prob, pred, dfp))

                cal = calibration_bins_df(y_test, prob, n_bins=N_CAL_BINS)
                cal["checkpoint"] = checkpoint
                cal["model"] = model_name
                cal["test_season"] = test_season
                all_cal.append(cal)

            def lr_baseline(feats, model_name):
                prep = ColumnTransformer(
                    [("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), feats)],
                    remainder="drop"
                )
                pipe = Pipeline([
                    ("prep", prep),
                    ("scaler", StandardScaler(with_mean=False)),
                    ("clf", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=RANDOM_STATE))
                ])
                pipe.fit(X_train[feats], y_train)
                prob = pipe.predict_proba(X_test[feats])[:, 1]
                record_probs(model_name, prob)

            # ---- Baseline: base rate ----
            base_rate = float(y_train.mean())
            record_probs("Baseline_BaseRate", np.full(len(y_test), base_rate, dtype=float))

            # ---- Baselines ----
            if "avg_point_diff_s2d" in FEATURES:
                lr_baseline(["avg_point_diff_s2d"], "Baseline_LR_avg_point_diff_s2d")

            if "win_pct_s2d" in FEATURES:
                lr_baseline(["win_pct_s2d"], "Baseline_LR_win_pct_s2d")

            if ("win_pct_s2d" in FEATURES) and ("win_pct_l10" in FEATURES):
                lr_baseline(["win_pct_s2d", "win_pct_l10"], "Baseline_LR_win_pct_s2d_l10")

            # NEW: Elo and Net Rating baselines (these will run if the columns exist)
            if "elo_pre" in FEATURES:
                lr_baseline(["elo_pre"], "Baseline_LR_elo_pre")

            if "net_rtg_s2d" in FEATURES:
                lr_baseline(["net_rtg_s2d"], "Baseline_LR_net_rtg_s2d")

            combo_feats = [c for c in ["win_pct_s2d", "avg_point_diff_s2d", "net_rtg_s2d", "elo_pre"] if c in FEATURES]
            if len(combo_feats) >= 2:
                lr_baseline(combo_feats, "Baseline_LR_win_pct_s2d_plus_strength")

            # ---- Full Logistic Regression ----
            lr = Pipeline([
                ("prep", preprocessor),
                ("scaler", StandardScaler(with_mean=False)),
                ("clf", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=RANDOM_STATE))
            ])
            lr.fit(X_train, y_train)
            record_probs("LogisticRegression", lr.predict_proba(X_test)[:, 1])

            # ---- Random Forest ----
            rf = Pipeline([
                ("prep", preprocessor),
                ("clf", RandomForestClassifier(
                    n_estimators=400,
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                    class_weight="balanced_subsample",
                    min_samples_leaf=5
                ))
            ])
            rf.fit(X_train, y_train)
            record_probs("RandomForest", rf.predict_proba(X_test)[:, 1])

            # ---- Gradient Boosting ----
            gb = Pipeline([
                ("prep", preprocessor),
                ("clf", GradientBoostingClassifier(random_state=RANDOM_STATE))
            ])
            gb.fit(X_train, y_train)
            record_probs("GradientBoosting", gb.predict_proba(X_test)[:, 1])

        print(f"Finished fold: test_season={test_season}, train_seasons={train_seasons}")

    # ---- Save artifacts ----
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(outname("results_backtest_by_fold"), index=False)

    avg = (results_df
           .groupby(["checkpoint", "model"], as_index=False)
           .agg(
               folds=("test_season", "nunique"),
               auc_mean=("auc", "mean"),
               auc_std=("auc", "std"),
               logloss_mean=("logloss", "mean"),
               logloss_std=("logloss", "std"),
               brier_mean=("brier", "mean"),
               brier_std=("brier", "std"),
               acc_mean=("accuracy", "mean"),
               acc_std=("accuracy", "std"),
               top16_mean=("top16_hit_rate", "mean"),
               top16_std=("top16_hit_rate", "std"),
               top8conf_mean=("top8_conf_hit_rate", "mean"),
               top8conf_std=("top8_conf_hit_rate", "std"),
           ))
    avg.to_csv(outname("results_backtest_avg"), index=False)

    cal_df = pd.concat(all_cal, ignore_index=True)
    cal_df.to_csv(outname("calibration_bins_backtest_by_fold"), index=False)

    preds_df = pd.concat(all_preds, ignore_index=True)
    preds_df.to_csv(outname("preds_backtest_by_fold"), index=False)

    print("\nSaved:")
    print("-", outname("results_backtest_by_fold"))
    print("-", outname("results_backtest_avg"))
    print("-", outname("calibration_bins_backtest_by_fold"))
    print("-", outname("preds_backtest_by_fold"))


if __name__ == "__main__":
    main()

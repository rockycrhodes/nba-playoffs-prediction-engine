#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 21:36:02 2026

@author: rrhodes
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
from sklearn.inspection import permutation_importance


# -----------------------
# CONFIG
# -----------------------
DB_USER = os.getenv("NBA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("NBA_DB_HOST", "localhost")
DB_PORT = os.getenv("NBA_DB_PORT", "5432")
DB_NAME = os.getenv("NBA_DB_NAME", "nba_data")

TRAIN_SEASONS = ["2021-22", "2022-23", "2023-24", "2024-25"]
TEST_SEASON = "2025-26"

TARGET_COL = "made_playoffs"

CHECKPOINT_VIEWS = {
    20: "model_v2_checkpoint_20",
    40: "model_v2_checkpoint_40",
    60: "model_v2_checkpoint_60",
}

FEATURES = [
    "avg_point_diff_s2d",
    "avg_point_diff_l10",
    "efg_s2d",
    "efg_l10",
    "ts_s2d",
    "ts_l10",
    "fg3a_rate_s2d",
    "fg3a_rate_l10",
    "fta_rate_s2d",
    "fta_rate_l10",
    "tov_rate_s2d",
    "tov_rate_l10",
]

META_COLS = ["season", "team_id", "team_conference"]  # for reporting/decision metric

EXPERIMENT_TAG = "v2_rates"   # e.g., "v1_minimal", "v2_rates"

N_CAL_BINS = 10
PERM_IMPORTANCE_REPEATS = 30
RANDOM_STATE = 42


# -----------------------
# Helpers
# -----------------------
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def load_checkpoint(view_name: str, engine) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {view_name};", engine)
    df[TARGET_COL] = df[TARGET_COL].astype(int)
    if "is_home" in df.columns:
        df["is_home"] = df["is_home"].astype(int)

    required = set(FEATURES + META_COLS + [TARGET_COL])
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{view_name} missing columns: {missing}")
    return df

def split_xy(df: pd.DataFrame):
    train_df = df[df["season"].isin(TRAIN_SEASONS)].copy()
    test_df = df[df["season"] == TEST_SEASON].copy()

    if train_df.empty or test_df.empty:
        raise RuntimeError(
            f"Empty split. Train rows={len(train_df)}, Test rows={len(test_df)}. "
            f"Check season values in the view."
        )

    X_train = train_df[FEATURES].copy()
    y_train = train_df[TARGET_COL].copy()

    X_test = test_df[FEATURES].copy()
    y_test = test_df[TARGET_COL].copy()

    meta_test = test_df[META_COLS + [TARGET_COL]].copy()
    return X_train, y_train, X_test, y_test, meta_test

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
        dfc.groupby("bin", observed=True)   # only bins that actually appear
           .agg(pred_mean=("y_prob", "mean"),
                actual_rate=("y_true", "mean"),
                n=("y_true", "size"))
           .reset_index()
    )
    return out

def evaluate_block(model_name, checkpoint, y_true, y_prob, y_pred, df_preds):
    return {
        "checkpoint": checkpoint,
        "model": model_name,
        "test_season": TEST_SEASON,
        "n_test": len(y_true),
        "auc": safe_auc(y_true, y_prob),
        "logloss": log_loss(y_true, y_prob, labels=[0, 1]),
        "accuracy": accuracy_score(y_true, y_pred),
        "brier": brier_score_loss(y_true, y_prob),
        **top16_hit_rate(df_preds),
        **top8_by_conference_hit_rate(df_preds),
    }

def build_preprocessor():
    numeric_transform = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])
    preprocessor = ColumnTransformer(
        transformers=[("num", numeric_transform, FEATURES)],
        remainder="drop"
    )
    return preprocessor

def export_lr_coefficients(lr_pipeline: Pipeline, checkpoint: int, out_list: list):
    """
    Extracts standardized LR coefficients. Because we scale, these are interpretable as:
    1 SD increase in feature -> log-odds change by coef.
    """
    clf = lr_pipeline.named_steps["clf"]
    coefs = clf.coef_.ravel()
    for feat, coef in zip(FEATURES, coefs):
        out_list.append({
            "checkpoint": checkpoint,
            "model": "LogisticRegression",
            "feature": feat,
            "coef": float(coef),
            "abs_coef": float(abs(coef)),
        })

def export_permutation_importance(model_pipeline: Pipeline, model_name: str, checkpoint: int,
                                 X_test: pd.DataFrame, y_test: pd.Series, out_list: list):
    """
    Permutation importance on the full pipeline. Uses scoring='roc_auc' to align with main metric.
    """
    r = permutation_importance(
        model_pipeline,
        X_test,
        y_test,
        scoring="roc_auc",
        n_repeats=PERM_IMPORTANCE_REPEATS,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    for feat, mean_imp, std_imp in zip(FEATURES, r.importances_mean, r.importances_std):
        out_list.append({
            "checkpoint": checkpoint,
            "model": model_name,
            "feature": feat,
            "perm_importance_mean": float(mean_imp),
            "perm_importance_std": float(std_imp),
        })


# -----------------------
# Main experiment loop
# -----------------------
def main():
    engine = get_engine()
    preprocessor = build_preprocessor()

    all_results = []
    all_preds = []
    all_cal = []
    lr_coef_rows = []
    perm_rows = []

    for checkpoint, view in CHECKPOINT_VIEWS.items():
        df = load_checkpoint(view, engine)
        X_train, y_train, X_test, y_test, meta_test = split_xy(df)
        
        # -----------------------
        # BASELINES (Step 5)
        # -----------------------
        
        # Baseline 1: Base-rate only (constant probability = train positive rate)
        base_rate = float(y_train.mean())
        base_prob = np.full(len(y_test), base_rate, dtype=float)
        base_pred = (base_prob >= 0.5).astype(int)
        
        dfp = meta_test.rename(columns={TARGET_COL: "y_true"}).copy()
        dfp["y_prob"] = base_prob
        dfp["checkpoint"] = checkpoint
        dfp["model"] = "Baseline_BaseRate"
        all_preds.append(dfp)
        
        all_results.append(evaluate_block("Baseline_BaseRate", checkpoint, y_test, base_prob, base_pred, dfp))
        
        cal = calibration_bins_df(y_test, base_prob, n_bins=N_CAL_BINS)
        cal["checkpoint"] = checkpoint
        cal["model"] = "Baseline_BaseRate"
        all_cal.append(cal)
        
        # Baseline 2: Single-feature logistic regression using avg_point_diff_s2d only
        one_feat = ["avg_point_diff_s2d"]
        
        one_feat_preprocessor = ColumnTransformer(
            transformers=[
                ("num", Pipeline(steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ]), one_feat)
                ],
            remainder="drop"
            )
        
        lr_1d = Pipeline(steps=[
            ("prep", one_feat_preprocessor),
            ("scaler", StandardScaler(with_mean=False)),
            ("clf", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=RANDOM_STATE))
            ])
        lr_1d.fit(X_train[one_feat], y_train)
        
        lr1_prob = lr_1d.predict_proba(X_test[one_feat])[:, 1]
        lr1_pred = (lr1_prob >= 0.5).astype(int)
        
        dfp = meta_test.rename(columns={TARGET_COL: "y_true"}).copy()
        dfp["y_prob"] = lr1_prob
        dfp["checkpoint"] = checkpoint
        dfp["model"] = "Baseline_LR_avg_point_diff_s2d"
        all_preds.append(dfp)
        
        all_results.append(evaluate_block("Baseline_LR_avg_point_diff_s2d", checkpoint, y_test, lr1_prob, lr1_pred, dfp))
        
        cal = calibration_bins_df(y_test, lr1_prob, n_bins=N_CAL_BINS)
        cal["checkpoint"] = checkpoint
        cal["model"] = "Baseline_LR_avg_point_diff_s2d"
        all_cal.append(cal)
        

        # ---- 1) Logistic Regression ----
        lr = Pipeline(steps=[
            ("prep", preprocessor),
            ("scaler", StandardScaler(with_mean=False)),
            ("clf", LogisticRegression(max_iter=4000, class_weight="balanced", random_state=RANDOM_STATE))
        ])
        lr.fit(X_train, y_train)
        lr_prob = lr.predict_proba(X_test)[:, 1]
        lr_pred = (lr_prob >= 0.5).astype(int)

        dfp = meta_test.rename(columns={TARGET_COL: "y_true"}).copy()
        dfp["y_prob"] = lr_prob
        dfp["checkpoint"] = checkpoint
        dfp["model"] = "LogisticRegression"
        all_preds.append(dfp)

        all_results.append(evaluate_block("LogisticRegression", checkpoint, y_test, lr_prob, lr_pred, dfp))

        cal = calibration_bins_df(y_test, lr_prob, n_bins=N_CAL_BINS)
        cal["checkpoint"] = checkpoint
        cal["model"] = "LogisticRegression"
        all_cal.append(cal)

        export_lr_coefficients(lr, checkpoint, lr_coef_rows)
        # (optional) permutation importance for LR too; not necessary but could be added later

        # ---- 2) Random Forest ----
        rf = Pipeline(steps=[
            ("prep", preprocessor),
            ("clf", RandomForestClassifier(
                n_estimators=400,
                random_state=RANDOM_STATE,
                n_jobs=-1,
                class_weight="balanced_subsample",
                max_depth=None,
                min_samples_leaf=5
            ))
        ])
        rf.fit(X_train, y_train)
        rf_prob = rf.predict_proba(X_test)[:, 1]
        rf_pred = (rf_prob >= 0.5).astype(int)

        dfp = meta_test.rename(columns={TARGET_COL: "y_true"}).copy()
        dfp["y_prob"] = rf_prob
        dfp["checkpoint"] = checkpoint
        dfp["model"] = "RandomForest"
        all_preds.append(dfp)

        all_results.append(evaluate_block("RandomForest", checkpoint, y_test, rf_prob, rf_pred, dfp))

        cal = calibration_bins_df(y_test, rf_prob, n_bins=N_CAL_BINS)
        cal["checkpoint"] = checkpoint
        cal["model"] = "RandomForest"
        all_cal.append(cal)

        export_permutation_importance(rf, "RandomForest", checkpoint, X_test, y_test, perm_rows)

        # ---- 3) Gradient Boosting ----
        gb = Pipeline(steps=[
            ("prep", preprocessor),
            ("clf", GradientBoostingClassifier(random_state=RANDOM_STATE))
        ])
        gb.fit(X_train, y_train)
        gb_prob = gb.predict_proba(X_test)[:, 1]
        gb_pred = (gb_prob >= 0.5).astype(int)

        dfp = meta_test.rename(columns={TARGET_COL: "y_true"}).copy()
        dfp["y_prob"] = gb_prob
        dfp["checkpoint"] = checkpoint
        dfp["model"] = "GradientBoosting"
        all_preds.append(dfp)

        all_results.append(evaluate_block("GradientBoosting", checkpoint, y_test, gb_prob, gb_pred, dfp))

        cal = calibration_bins_df(y_test, gb_prob, n_bins=N_CAL_BINS)
        cal["checkpoint"] = checkpoint
        cal["model"] = "GradientBoosting"
        all_cal.append(cal)

        export_permutation_importance(gb, "GradientBoosting", checkpoint, X_test, y_test, perm_rows)

        print(f"Done checkpoint {checkpoint}: train={len(X_train)} test={len(X_test)}")

    # ---- Save artifacts ----
    results_df = pd.DataFrame(all_results).sort_values(["checkpoint", "auc"], ascending=[True, False])
    results_df.to_csv(f"results_by_checkpoint_{EXPERIMENT_TAG}.csv", index=False)

    preds_df = pd.concat(all_preds, ignore_index=True)
    preds_df.to_csv(f"preds_test_season_by_checkpoint_{EXPERIMENT_TAG}.csv", index=False)

    cal_df = pd.concat(all_cal, ignore_index=True)
    cal_df.to_csv(f"calibration_bins_by_checkpoint_{EXPERIMENT_TAG}.csv", index=False)

    lr_coef_df = pd.DataFrame(lr_coef_rows).sort_values(["checkpoint", "abs_coef"], ascending=[True, False])
    lr_coef_df.to_csv(f"lr_coefficients_by_checkpoint_{EXPERIMENT_TAG}.csv", index=False)

    perm_df = pd.DataFrame(perm_rows).sort_values(["checkpoint", "model", "perm_importance_mean"],
                                                  ascending=[True, True, False])
    perm_df.to_csv(f"permutation_importance_by_checkpoint_{EXPERIMENT_TAG}.csv", index=False)

    print("\nSaved:")
    print("- results_by_checkpoint.csv")
    print("- preds_test_season_by_checkpoint.csv")
    print("- calibration_bins_by_checkpoint.csv")
    print("- lr_coefficients_by_checkpoint.csv")
    print("- permutation_importance_by_checkpoint.csv")
    print("\nFeatures:", FEATURES)


if __name__ == "__main__":
    main()

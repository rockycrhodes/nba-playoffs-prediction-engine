#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 14:50:43 2026

@author: rrhodes
"""

import os
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    roc_auc_score, log_loss, accuracy_score, brier_score_loss, classification_report
)

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

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

SOURCE = "team_game_training"  # change to team_game_training if that's what you have
TARGET_COL = "made_playoffs"

# -----------------------
# Helpers
# -----------------------
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def load_data():
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {SOURCE};", engine)
    return df

def pick_feature_columns(df):
    """
    Adjust this to your actual columns.
    We'll automatically include numeric columns except obvious identifiers/target.
    """
    exclude = {
        TARGET_COL,
        "game_id", "game_date", "season", "team_id", "opp_team_id",
        "season_type"
    }
    numeric_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    # If your rolling features are numeric but stored as object, convert them in transform step or here.
    return numeric_cols

def evaluate(model_name, y_true, y_proba, y_pred):
    auc = roc_auc_score(y_true, y_proba) if len(np.unique(y_true)) > 1 else np.nan
    ll = log_loss(y_true, y_proba, labels=[0, 1])
    acc = accuracy_score(y_true, y_pred)
    brier = brier_score_loss(y_true, y_proba)
    print(f"\n=== {model_name} ===")
    print(f"AUC:      {auc:.4f}")
    print(f"LogLoss:  {ll:.4f}")
    print(f"Accuracy: {acc:.4f}")
    print(f"Brier:    {brier:.4f}")
    print(classification_report(y_true, y_pred, digits=4))
    return {"model": model_name, "auc": auc, "logloss": ll, "accuracy": acc, "brier": brier}

def calibration_bins(y_true, y_proba, n_bins=10):
    """
    Returns a dataframe with average predicted prob and empirical rate per bin.
    Useful for plotting calibration later.
    """
    dfc = pd.DataFrame({"y": y_true, "p": y_proba})
    dfc["bin"] = pd.cut(dfc["p"], bins=np.linspace(0, 1, n_bins + 1), include_lowest=True)
    out = dfc.groupby("bin").agg(pred_mean=("p", "mean"), actual_rate=("y", "mean"), n=("y", "size")).reset_index()
    return out

# -----------------------
# Main
# -----------------------
def main():
    df = load_data()

    # Split by season
    train_df = df[df["season"].isin(TRAIN_SEASONS)].copy()
    test_df = df[df["season"] == TEST_SEASON].copy()

    if train_df.empty or test_df.empty:
        raise RuntimeError(
            f"Train or test split is empty. Train rows={len(train_df)}, Test rows={len(test_df)}. "
            f"Check season values in {SOURCE}."
        )

    feature_cols = pick_feature_columns(df)
    if not feature_cols:
        raise RuntimeError("No numeric feature columns found. Check your training table columns.")

    X_train = train_df[feature_cols]
    y_train = train_df[TARGET_COL].astype(int)

    X_test = test_df[feature_cols]
    y_test = test_df[TARGET_COL].astype(int)

    # Preprocessing: impute missing numeric values; scale for LR only via pipeline
    numeric_transform = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])

    preprocessor = ColumnTransformer(
        transformers=[("num", numeric_transform, feature_cols)],
        remainder="drop"
    )

    results = []
    cal_frames = []

    # 1) Logistic Regression (with scaling)
    lr = Pipeline(steps=[
        ("prep", preprocessor),
        ("scaler", StandardScaler(with_mean=False)),  # sparse-safe
        ("clf", LogisticRegression(max_iter=2000, class_weight="balanced"))
    ])
    lr.fit(X_train, y_train)
    lr_proba = lr.predict_proba(X_test)[:, 1]
    lr_pred = (lr_proba >= 0.5).astype(int)
    results.append(evaluate("LogisticRegression", y_test, lr_proba, lr_pred))
    cal = calibration_bins(y_test, lr_proba)
    cal["model"] = "LogisticRegression"
    cal_frames.append(cal)

    # 2) Random Forest
    rf = Pipeline(steps=[
        ("prep", preprocessor),
        ("clf", RandomForestClassifier(
            n_estimators=400,
            random_state=42,
            n_jobs=-1,
            class_weight="balanced_subsample",
            max_depth=None,
            min_samples_leaf=5
        ))
    ])
    rf.fit(X_train, y_train)
    rf_proba = rf.predict_proba(X_test)[:, 1]
    rf_pred = (rf_proba >= 0.5).astype(int)
    results.append(evaluate("RandomForest", y_test, rf_proba, rf_pred))
    cal = calibration_bins(y_test, rf_proba)
    cal["model"] = "RandomForest"
    cal_frames.append(cal)

    # 3) Gradient Boosting (sklearn)
    gb = Pipeline(steps=[
        ("prep", preprocessor),
        ("clf", GradientBoostingClassifier(random_state=42))
    ])
    gb.fit(X_train, y_train)
    gb_proba = gb.predict_proba(X_test)[:, 1]
    gb_pred = (gb_proba >= 0.5).astype(int)
    results.append(evaluate("GradientBoosting", y_test, gb_proba, gb_pred))
    cal = calibration_bins(y_test, gb_proba)
    cal["model"] = "GradientBoosting"
    cal_frames.append(cal)

    # Save results
    results_df = pd.DataFrame(results).sort_values(["auc", "logloss"], ascending=[False, True])
    results_df.to_csv("model_results.csv", index=False)

    cal_df = pd.concat(cal_frames, ignore_index=True)
    cal_df.to_csv("model_calibration_bins.csv", index=False)

    print("\nSaved:")
    print("- model_results.csv")
    print("- model_calibration_bins.csv")
    print(f"\nFeatures used ({len(feature_cols)}): {feature_cols}")

if __name__ == "__main__":
    main()


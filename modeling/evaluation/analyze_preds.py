#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 22:18:53 2026

@author: rrhodes
"""

import os
import pandas as pd
import numpy as np

# ----------------------------
# CONFIG
# ----------------------------
PROJECT_DIR = "/Users/rrhodes/Documents/NBA"
os.chdir(PROJECT_DIR)

# v1 uses *no tag* (plain filenames). v2 uses a tag suffix.
EXPERIMENTS = [
    {"name": "v1_minimal", "tag": None},
    {"name": "v2_rates", "tag": "v2_rates"},
]

# ----------------------------
# Helpers
# ----------------------------
def fname(base: str, tag: str | None) -> str:
    """
    base examples:
      - "preds_test_season_by_checkpoint"
      - "calibration_bins_by_checkpoint"
    v1: base + ".csv"
    v2: base + "_" + tag + ".csv"
    """
    return f"{base}.csv" if tag is None else f"{base}_{tag}.csv"

def acc_at_threshold(df, thr):
    yhat = (df["y_prob"] >= thr).astype(int)
    return float((yhat == df["y_true"]).mean())

def analyze_experiment(name: str, tag: str | None):
    preds_path = fname("preds_test_season_by_checkpoint", tag)
    cal_path   = fname("calibration_bins_by_checkpoint", tag)

    preds = pd.read_csv(preds_path)
    cal = pd.read_csv(cal_path)

    print("\n" + "="*90)
    print(f"EXPERIMENT: {name}    (preds={preds_path}, cal={cal_path})")
    print("="*90)

    # ----------------------------
    # Mean predicted probability summary table
    # ----------------------------
    summary = (
        preds.groupby(["checkpoint", "model"])
        .agg(
            n=("y_true", "size"),
            actual_rate=("y_true", "mean"),
            mean_p=("y_prob", "mean"),
            p10=("y_prob", lambda s: np.quantile(s, 0.10)),
            p50=("y_prob", "median"),
            p90=("y_prob", lambda s: np.quantile(s, 0.90)),
            frac_over_05=("y_prob", lambda s: (s >= 0.5).mean()),
        )
        .reset_index()
    )

    summary[["actual_rate","mean_p","p10","p50","p90","frac_over_05"]] = summary[
        ["actual_rate","mean_p","p10","p50","p90","frac_over_05"]
    ].round(3)

    print("\n=== Mean predicted probability summary ===")
    print(summary.sort_values(["checkpoint","model"]).to_string(index=False))

    # ----------------------------
    # Calibration: Expected Calibration Error (ECE)
    # ----------------------------
    cal = cal.dropna(subset=["pred_mean", "actual_rate", "n"]).copy()
    cal["abs_gap"] = (cal["pred_mean"] - cal["actual_rate"]).abs()

    cal["weight"] = cal["n"] / cal.groupby(["checkpoint", "model"])["n"].transform("sum")
    cal["weighted_gap"] = cal["abs_gap"] * cal["weight"]

    ece = (
        cal.groupby(["checkpoint", "model"], as_index=False)["weighted_gap"]
           .sum()
           .rename(columns={"weighted_gap": "ece"})
    )
    ece["ece"] = ece["ece"].round(3)

    print("\n=== ECE (Expected Calibration Error) [lower is better] ===")
    print(ece.sort_values(["checkpoint","ece"]).to_string(index=False))

    # ----------------------------
    # Threshold sensitivity
    # ----------------------------
    rows = []
    for (cp, m), g in preds.groupby(["checkpoint","model"]):
        rows.append({
            "checkpoint": cp,
            "model": m,
            "acc@0.3": acc_at_threshold(g, 0.3),
            "acc@0.5": acc_at_threshold(g, 0.5),
            "acc@0.7": acc_at_threshold(g, 0.7),
        })

    thr_table = pd.DataFrame(rows)
    thr_table[["acc@0.3","acc@0.5","acc@0.7"]] = thr_table[["acc@0.3","acc@0.5","acc@0.7"]].round(3)

    print("\n=== Accuracy by threshold (0.3/0.5/0.7) ===")
    print(thr_table.sort_values(["checkpoint","model"]).to_string(index=False))

    # return for cross-experiment comparison
    summary["experiment"] = name
    ece["experiment"] = name
    thr_table["experiment"] = name
    return summary, ece, thr_table


# ----------------------------
# Run all experiments
# ----------------------------
all_summaries, all_eces, all_thr = [], [], []

for exp in EXPERIMENTS:
    s, e, t = analyze_experiment(exp["name"], exp["tag"])
    all_summaries.append(s)
    all_eces.append(e)
    all_thr.append(t)

# ----------------------------
# Optional: compare LR ECE across experiments
# ----------------------------
ece_all = pd.concat(all_eces, ignore_index=True)
lr_ece = ece_all[ece_all["model"] == "LogisticRegression"].copy()

if not lr_ece.empty:
    print("\n" + "-"*90)
    print("LR calibration comparison (ECE) across experiments:")
    print("-"*90)
    print(lr_ece.sort_values(["checkpoint","experiment"]).to_string(index=False))
    print("\n" + "="*90)
    print("BACKTEST (rolling) summary for v2")
    print("="*90)
    
    BACKTEST_TAG = "v2_rates"  # must match what you used in run_checkpoint_models_backtest.py
    backtest_path = f"results_backtest_avg_{BACKTEST_TAG}.csv"
    
    bt = pd.read_csv(backtest_path)
    
    # Focus on the key models
    keep_models = [
        "Baseline_BaseRate",
        "Baseline_LR_avg_point_diff_s2d",
        "LogisticRegression",
        "RandomForest",
        "GradientBoosting",
        ]
    bt = bt[bt["model"].isin(keep_models)].copy()
    
    # Pretty print key metrics
    cols = [
        "checkpoint","model","folds",
        "auc_mean","auc_std",
        "logloss_mean","logloss_std",
        "brier_mean","brier_std",
        "acc_mean","acc_std",
        "top16_mean","top16_std",
        "top8conf_mean","top8conf_std",
        ]
    print(bt[cols].sort_values(["checkpoint","auc_mean"], ascending=[True, False]).to_string(index=False))


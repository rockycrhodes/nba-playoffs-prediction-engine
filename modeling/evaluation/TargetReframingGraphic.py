#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 16:05:06 2026

@author: rrhodes
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# 1) Pull base rates from your checkpoint label views
df = pd.read_sql(text("""
    with labels as (
      select '2022-23'::text as season, made_top6_conf::int as top6, made_top10_conf::int as top10
      from public.model_v6b_checkpoint_60
      where season='2022-23'
      union all
      select season, made_top6_conf::int, made_top10_conf::int
      from public.model_v6b_checkpoint_60
      where season in ('2023-24','2024-25','2025-26')
    )
    select
      avg(top6)::float as top6_rate,
      avg(top10)::float as top10_rate
    from labels;
"""), engine)

top6_rate = float(df["top6_rate"].iloc[0])
top10_rate = float(df["top10_rate"].iloc[0])

# If you want the “true” theoretical base rates:
# top6_rate = 12/30
# top10_rate = 20/30

# 2) Winner-by-checkpoint callout (hard-code from your latest leaderboard)
# You can update these labels as your results evolve.
winners = pd.DataFrame({
    "Target": ["Top-10", "Top-10", "Top-10", "Top-6", "Top-6", "Top-6"],
    "Checkpoint": ["CP20", "CP40", "CP60", "CP20", "CP40", "CP60"],
    "Best model (logloss)": [
        "Baseline LR (net rating)",
        "Monte Carlo (EloSim, T=2.5)",
        "Monte Carlo (EloSim, T=2.5)",
        "Monte Carlo (EloSim, T=2.5)",
        "Monte Carlo (EloSim, T=2.5)",
        "Monte Carlo (EloSim, T=2.5)",
    ]
})

# ---- Make the figure ----
plt.rcParams.update({"font.size": 11})
fig = plt.figure(figsize=(12, 6), dpi=200)
gs = fig.add_gridspec(1, 2, width_ratios=[1, 1.35])

# Left: base rate table + bars
ax1 = fig.add_subplot(gs[0, 0])
ax1.set_title("Target base rates (per conference)")
ax1.bar(["Top-6", "Top-10"], [top6_rate, top10_rate], color=["#2E86AB", "#F18F01"])
ax1.set_ylim(0, 1)
for x, v in zip(["Top-6", "Top-10"], [top6_rate, top10_rate]):
    ax1.text(x, v + 0.03, f"{v:.3f}", ha="center", va="bottom", fontweight="bold")
ax1.set_ylabel("Base rate")
ax1.grid(axis="y", alpha=0.25)

# Right: winners table
ax2 = fig.add_subplot(gs[0, 1])
ax2.axis("off")
ax2.set_title("What wins by checkpoint (rolling backtest)\n(lower log loss is better)", pad=12)

tbl = ax2.table(
    cellText=winners.values,
    colLabels=winners.columns,
    loc="center",
    cellLoc="left",
    colLoc="left"
)
tbl.auto_set_font_size(False)
tbl.set_fontsize(10)
tbl.scale(1, 1.6)

# Header styling
for (r, c), cell in tbl.get_celld().items():
    if r == 0:
        cell.set_text_props(weight="bold")
        cell.set_facecolor("#F2F2F2")

fig.suptitle("Reframing the question: Playoffs → Top-6 / Top-10 (play-in era)", fontsize=14, fontweight="bold")
fig.tight_layout(rect=[0, 0, 1, 0.93])

out_path = "target_reframing_visual.png"
fig.savefig(out_path, bbox_inches="tight")
print("Saved:", out_path)

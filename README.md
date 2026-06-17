# NBA Analytics: End-to-End Prediction Pipeline

End-to-end NBA data pipeline and predictive modeling workflow using multi-season team, game, and player statistics. Special thanks to [Swar Patel](https://github.com/swar/nba_api) for creating and maintaining the excellent `nba_api` package, which powered much of this project's data extraction.

---

## Modeling Objectives

### 1) Playoff Qualification Probability (Team-Season)

**Goal:** Estimate, throughout the regular season, each team's probability of qualifying for the playoffs (top 8 in each conference, including play-in winners).

**Prediction cadence (production):**
After each team game, recompute rolling performance features (e.g., last-10 win%, last-10 point differential) and generate updated playoff probabilities.

**Evaluation protocol (offline):**
To avoid cherry-picking dates and to ensure reproducibility, model performance is evaluated at standardized season checkpoints: after game 20, game 40, and game 60 for each team.

**Success metrics:**
- ROC AUC (ranking / separation)
- Log Loss (probability quality)
- Brier Score + calibration curves (probability calibration)
- Baseline comparison (e.g., simple model using season-to-date win% and point differential)

---

### 2) Next-Game Win Probability (Game-Level)

**Goal:** Predict the probability that the home team wins a specific upcoming matchup.

**Prediction timing:**
Pre-game only; features are computed from historical games up to (but excluding) the target game date to prevent leakage.

**Champion model features:**
Elo differential, rolling net rating (last 10 games), rest advantage (days between games), back-to-back indicators (home and away), injury proxy differentials (approximate available roster strength), travel distance advantage, and home/away indicator. All features are computed strictly from games prior to the prediction date to prevent leakage.

**Success metrics:**
- Log Loss and Brier Score (primary)
- ROC AUC (secondary)
- Baseline comparisons:
  - Home-team baseline (predict home win at league-average home win rate)
  - Point-differential-only logistic regression baseline

---

## Baseline Models

To contextualize performance, the project benchmarks ML models against simple, explainable baselines:

**Playoff qualification baselines:**
- Win% / point differential baseline: Logistic regression using season-to-date win% and average point differential (or last-10 point differential) only.
- Rank heuristic baseline (optional): Classify "made playoffs" based on current conference rank at each checkpoint (e.g., top 8 at game 40).

**Next-game win probability baselines:**
- Home-win-rate baseline: Predict P(home_win) as the historical league-average home win rate (constant probability for all games).
- Point-differential-only baseline: Logistic regression using only rolling point differential and home/away indicator.

These baselines provide a lower bound on expected performance and ensure any lift from more complex models is meaningful.

---

## Data Leakage Prevention

All rolling features are computed using only games prior to the prediction point (e.g., last-10 games ending at `1 PRECEDING`), ensuring the model never "sees" the target game outcome or same-day boxscore stats when generating pre-game or mid-season predictions.

---

## Pipeline Overview

**Data ingestion → warehouse → features → models → outputs**

- **ETL (bulk + daily):** Python scripts extract NBA games, team stats, player stats, and standings from NBA endpoints with retry/backoff, logging, and incremental saves.
- **Storage:** Cleaned data is loaded into PostgreSQL (tables for games, teams, players, `team_game_stats`, `player_game_stats`, standings), with validation queries and consistency checks.
- **Feature engineering (SQL):** Postgres views compute rolling team performance features (e.g., win% L10, avg point diff L10, season-to-date aggregates) at each team-game observation.
- **Elo computation:** `compute_and_load_elo.py` calculates per-team Elo ratings from historical game outcomes (K-factor-based, home-advantage-adjusted) and upserts them into a `team_elo_history` table, making pre-game Elo difference available as a model feature.
- **Model training/evaluation:** Python (scikit-learn) trains and compares logistic regression, random forest, and gradient boosting models using:
  - Train seasons: 2021-22 to 2024-25
  - Test season: 2025-26
  - Checkpoints: game 20 / 40 / 60 (for playoff-probability evaluation)
- **Monte Carlo simulation:** `run_mc_sim.py` samples predicted next-game win probabilities across the remaining schedule at each season checkpoint to estimate each team's playoff probability and expected seed distribution.
- **Data quality:** Five SQL scripts (`sql/quality_checks/`) validate referential integrity, detect duplicates, check feature completeness, and flag out-of-range values before modeling.
- **Outputs:** Model artifacts include probability predictions, performance tables (AUC / log loss / Brier), and calibration summaries for reporting and visualization.

---

## Market Odds Integration (Polymarket)

`polymarket/` contains an ETL pipeline that fetches live NBA game odds from the [Polymarket](https://polymarket.com) prediction market (via Gamma and CLOB APIs), maps them to internal game IDs, and upserts bid/ask quotes into PostgreSQL. This enables downstream comparison of model-derived win probabilities against market-implied probabilities.

---

## Repository Structure

```
nba-analytics/
├── etl/
│   ├── daily/              # Daily extract/transform/load (games, stats, standings)
│   ├── bulk/               # Historical backfill scripts
│   └── advanced_metrics/            # Elo ratings, advanced stats, schedule
├── modeling/
│   ├── playoffs/           # Playoff qualification models and checkpoint evaluation
│   ├── next_game/          # Next-game win probability model and Monte Carlo sim
│   └── evaluation/         # Prediction analysis and visualizations
├── sql/
│   ├── tables/             # Table DDL (games, teams, players, stats, standings)
│   ├── views/              # Feature engineering views
│   └── quality_checks/     # Data quality validation queries
├── notebooks/              # Exploratory data analysis
├── polymarket/             # Prediction market odds ETL
├── .gitignore
├── requirements.txt
└── README.md
```

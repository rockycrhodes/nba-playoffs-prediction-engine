#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

N_SIMS = int(os.getenv("N_SIMS", "10000"))
TEST_SEASONS = [2022, 2023, 2024, 2025]     # season_start years
CHECKPOINTS = [20, 40, 60]

def ensure_output_table():
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists public.mc_team_probs (
              season text not null,                 -- e.g. '2022-23'
              season_start int not null,            -- e.g. 2022
              checkpoint int not null,
              team_id int not null,
              conference text,
              p_top6 double precision,
              p_top10 double precision,
              p_playoffs double precision,
              exp_wins double precision,
              exp_conf_seed double precision,
              n_sims int,
              seed int,
              alpha double precision,
              beta double precision,
              created_at timestamptz default now(),
              primary key (season_start, checkpoint, team_id)
            );
        """))

def load_params(season_start: int, checkpoint: int):
    df = pd.read_sql(
        text("""
            select alpha, beta, seed
            from public.mc_params
            where test_season = :ss and checkpoint = :cp
        """),
        engine, params={"ss": season_start, "cp": checkpoint}
    )
    if df.empty:
        raise RuntimeError(f"Missing mc_params for season_start={season_start}, checkpoint={checkpoint}")
    return float(df.alpha.iloc[0]), float(df.beta.iloc[0]), int(df.seed.iloc[0])

def load_team_state(season_start: int, checkpoint: int) -> pd.DataFrame:
    # team_state_at_checkpoint.season is text; derive season_start from it
    df = pd.read_sql(
        text("""
            select
              season,
              substring(season from 1 for 4)::int as season_start,
              checkpoint,
              team_id,
              conference,
              elo_cp::double precision as elo_cp,
              wins_cp::int as wins_cp
            from public.team_state_at_checkpoint
            where substring(season from 1 for 4)::int = :ss
              and checkpoint = :cp
        """),
        engine, params={"ss": season_start, "cp": checkpoint}
    )
    if df.empty:
        raise RuntimeError(f"No team_state rows for season_start={season_start}, checkpoint={checkpoint}")
    return df


EPS = 1e-15
TEMP = float(os.getenv("MC_TEMP", "2.0"))  # try 2.0 first
print("MC_TEMP =", TEMP)

def temp_scale(p, T=2.5, eps=1e-15):
    p = np.clip(p.astype(float), eps, 1 - eps)
    logit = np.log(p / (1 - p))
    return 1.0 / (1.0 + np.exp(-logit / T))

def load_games(season_start: int, checkpoint: int) -> pd.DataFrame:
    df = pd.read_sql(
        text("""
            select
              season,
              substring(season from 1 for 4)::int as season_start,
              checkpoint,
              game_id,
              home_team_id,
              away_team_id,
              p_home_win::double precision as p_home_win,
              alpha::double precision as alpha,
              beta::double precision as beta,
              seed::int as seed
            from public.sim_games_at_checkpoint
            where substring(season from 1 for 4)::int = :ss
              and checkpoint = :cp
        """),
        engine, params={"ss": season_start, "cp": checkpoint}
    )
    if df.empty:
        raise RuntimeError(f"No sim_games_at_checkpoint rows for season_start={season_start}, checkpoint={checkpoint}")

    # Temperature-scale game win probs to reduce overconfidence
    df["p_home_win"] = temp_scale(df["p_home_win"].to_numpy(), T=TEMP, eps=EPS)

    return df


def simulate_season_checkpoint(season_start: int, checkpoint: int) -> pd.DataFrame:
    alpha, beta, base_seed = load_params(season_start, checkpoint)
    rng = np.random.default_rng(base_seed + season_start * 100 + checkpoint)

    teams = load_team_state(season_start, checkpoint)
    games = load_games(season_start, checkpoint)

    season_txt = teams["season"].iloc[0]

    # team index mapping
    team_ids = teams["team_id"].astype(int).to_numpy()
    team_idx = {tid: i for i, tid in enumerate(team_ids)}
    n_teams = len(team_ids)

    conf = teams["conference"].astype(str).to_numpy()
    elo = teams["elo_cp"].astype(float).to_numpy()
    wins0 = teams["wins_cp"].astype(int).to_numpy()

    h = games["home_team_id"].astype(int).map(team_idx).to_numpy()
    a = games["away_team_id"].astype(int).map(team_idx).to_numpy()
    p = games["p_home_win"].astype(float).to_numpy()
    n_games = len(games)

    # Draw outcomes (chunk if memory becomes an issue)
    home_win = rng.random((N_SIMS, n_games)) < p

    wins_add = np.zeros((N_SIMS, n_teams), dtype=np.int16)
    np.add.at(wins_add, (np.arange(N_SIMS)[:, None], h[None, :]), home_win.astype(np.int16))
    np.add.at(wins_add, (np.arange(N_SIMS)[:, None], a[None, :]), (~home_win).astype(np.int16))
    wins_total = wins0[None, :] + wins_add

    out = []
    for conf_name in np.unique(conf):
        idxs = np.where(conf == conf_name)[0]
        w = wins_total[:, idxs]

        # tie-break: wins desc, elo desc, team_id asc
        elo_c = elo[idxs]
        tid_c = team_ids[idxs]
        order = np.lexsort(
            (tid_c[None, :].repeat(N_SIMS, axis=0),
             (-elo_c)[None, :].repeat(N_SIMS, axis=0),
             (-w))
        )
        seeds = np.empty_like(order, dtype=np.int8)
        seeds[np.arange(N_SIMS)[:, None], order] = (np.arange(order.shape[1])[None, :] + 1)

        p_top6 = (seeds <= 6).mean(axis=0)
        p_top10 = (seeds <= 10).mean(axis=0)
        p_playoffs = (seeds <= 10).mean(axis=0)
        exp_wins = w.mean(axis=0)
        exp_seed = seeds.mean(axis=0)

        for j, ti in enumerate(idxs):
            out.append({
                "season": season_txt,
                "season_start": season_start,
                "checkpoint": checkpoint,
                "team_id": int(team_ids[ti]),
                "conference": conf_name,
                "p_top6": float(p_top6[j]),
                "p_top10": float(p_top10[j]),
                "p_playoffs": float(p_playoffs[j]),
                "exp_wins": float(exp_wins[j]),
                "exp_conf_seed": float(exp_seed[j]),
                "n_sims": int(N_SIMS),
                "seed": int(base_seed + season_start * 100 + checkpoint),
                "alpha": float(alpha),
                "beta": float(beta),
            })

    return pd.DataFrame(out)

def write_results(df: pd.DataFrame):
    with engine.begin() as conn:
        conn.execute(text("""
            delete from public.mc_team_probs
            where season_start = :ss and checkpoint = :cp
        """), {"ss": int(df.season_start.iloc[0]), "cp": int(df.checkpoint.iloc[0])})

    df.to_sql("mc_team_probs", engine, schema="public",
              if_exists="append", index=False, method="multi", chunksize=2000)

def main():
    ensure_output_table()

    for ss in TEST_SEASONS:
        for cp in CHECKPOINTS:
            print(f"Simulating season_start={ss}, checkpoint={cp} with N_SIMS={N_SIMS} ...")
            df = simulate_season_checkpoint(ss, cp)
            write_results(df)
            print("  wrote", len(df), "rows")

if __name__ == "__main__":
    main()



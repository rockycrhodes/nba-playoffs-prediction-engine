#!/usr/bin/env python3
"""Polymarket ETL (Gamma + CLOB) -> Postgres (nba_data)

This version snapshots BOTH outcomes.

Important schema note
---------------------
Your current table public.polymarket_quotes has primary key:
  PRIMARY KEY (asof_ts, game_id)
So it can store only ONE quote row per game per timestamp.

To still capture both outcomes without breaking your DB, this script:
- Inserts the "outcome[0]" quote into public.polymarket_quotes (same behavior as before)
- ALSO writes a CSV containing BOTH outcomes (if --both-outcomes-csv is provided)
  so you can later decide whether to:
  (a) create a new table keyed by (asof_ts, game_id, token_id), OR
  (b) widen polymarket_quotes to have home/away columns.

If you want, tell me which option you prefer and I’ll adjust your Postgres schema + ETL accordingly.

Usage
-----
export DATABASE_URL='postgresql://user:pass@localhost:5432/nba_data'
python polymarket_etl.py --game-id 'YOUR_GAME_ID' --slug 'market-slug-here' \
  --both-outcomes-csv both_outcomes_quotes.csv

Batch:
python polymarket_etl.py --mapping-csv mappings.csv --both-outcomes-csv both_outcomes_quotes.csv
(where mappings.csv has columns: game_id,polymarket_slug)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


@dataclass
class Quote:
    asof_ts: datetime
    bid: Optional[float]
    ask: Optional[float]
    mid: Optional[float]
    liquidity_top_total: Optional[float]
    liquidity_depth_2c: Optional[float]


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def gamma_get_market_by_slug(slug: str, include_tag: bool = False, timeout_s: int = 30) -> Dict[str, Any]:
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    params = {"include_tag": "true"} if include_tag else None
    r = requests.get(url, params=params, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def clob_get_book(token_id: str, timeout_s: int = 30) -> Dict[str, Any]:
    url = f"{CLOB_BASE}/book"
    r = requests.get(url, params={"token_id": token_id}, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _parse_json_list_string(s: str) -> List[str]:
    s = (s or "").strip()
    if not (s.startswith("[") and s.endswith("]")):
        return []
    parts = [p.strip().strip('"').strip("'") for p in s[1:-1].split(",") if p.strip()]
    return parts


def _parse_clob_token_ids(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        return _parse_json_list_string(value)
    return []


def _parse_outcomes(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        return _parse_json_list_string(value)
    return []


def compute_quote_from_book(book: Dict[str, Any]) -> Quote:
    ts_ms = int(book["timestamp"])  # ms epoch
    asof_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    bids = book.get("bids") or []
    asks = book.get("asks") or []

    best_bid = _safe_float(bids[-1]["price"]) if bids else None
    best_ask = _safe_float(asks[0]["price"]) if asks else None

    mid = None
    if best_bid is not None and best_ask is not None:
        mid = 0.5 * (best_bid + best_ask)

    liquidity_top_total = None
    if bids and asks and best_bid is not None and best_ask is not None:
        bb_size = _safe_float(bids[-1].get("size")) or 0.0
        ba_size = _safe_float(asks[0].get("size")) or 0.0
        liquidity_top_total = best_bid * bb_size + best_ask * ba_size

    liquidity_depth_2c = None
    if mid is not None:
        lo = mid - 0.02
        hi = mid + 0.02
        depth = 0.0
        for lvl in bids:
            p = _safe_float(lvl.get("price"))
            s = _safe_float(lvl.get("size"))
            if p is None or s is None:
                continue
            if p >= lo:
                depth += p * s
        for lvl in asks:
            p = _safe_float(lvl.get("price"))
            s = _safe_float(lvl.get("size"))
            if p is None or s is None:
                continue
            if p <= hi:
                depth += p * s
        liquidity_depth_2c = depth

    return Quote(
        asof_ts=asof_ts,
        bid=best_bid,
        ask=best_ask,
        mid=mid,
        liquidity_top_total=liquidity_top_total,
        liquidity_depth_2c=liquidity_depth_2c,
    )


def upsert_polymarket_game_map(conn, game_id: str, polymarket_slug: str, gamma_market: Dict[str, Any], verified_ts: datetime):
    gamma_market_id = gamma_market.get("id")
    clob_market_id = gamma_market.get("conditionId")

    events = gamma_market.get("events") or []
    gamma_event_id = events[0].get("id") if events else None

    clob_token_ids = _parse_clob_token_ids(gamma_market.get("clobTokenIds"))
    token_yes_id = clob_token_ids[0] if len(clob_token_ids) >= 1 else None
    token_no_id = clob_token_ids[1] if len(clob_token_ids) >= 2 else None

    game_date = None
    for k in ("startDateIso", "endDateIso"):
        v = gamma_market.get(k)
        if v:
            try:
                game_date = datetime.fromisoformat(str(v).replace("Z", "+00:00")).date()
                break
            except Exception:
                pass

    home_abbr = None
    away_abbr = None
    if events and isinstance(events[0], dict):
        teams = events[0].get("teams") or []
        for t in teams:
            if not isinstance(t, dict):
                continue
            abbr = t.get("abbreviation") or t.get("abbr")
            if t.get("hostStatus") == "home":
                home_abbr = abbr
            elif t.get("hostStatus") == "away":
                away_abbr = abbr

    sql = """
    INSERT INTO public.polymarket_game_map (
        game_id, polymarket_slug, gamma_event_id, gamma_market_id, clob_market_id,
        token_yes_id, token_no_id, home_team_abbr, away_team_abbr,
        game_date, last_verified_ts, is_manual_override, notes
    )
    VALUES (
        %(game_id)s, %(polymarket_slug)s, %(gamma_event_id)s, %(gamma_market_id)s, %(clob_market_id)s,
        %(token_yes_id)s, %(token_no_id)s, %(home_team_abbr)s, %(away_team_abbr)s,
        %(game_date)s, %(last_verified_ts)s, false, null
    )
    ON CONFLICT (game_id)
    DO UPDATE SET
        polymarket_slug = EXCLUDED.polymarket_slug,
        gamma_event_id = EXCLUDED.gamma_event_id,
        gamma_market_id = EXCLUDED.gamma_market_id,
        clob_market_id = EXCLUDED.clob_market_id,
        token_yes_id = EXCLUDED.token_yes_id,
        token_no_id = EXCLUDED.token_no_id,
        home_team_abbr = COALESCE(EXCLUDED.home_team_abbr, public.polymarket_game_map.home_team_abbr),
        away_team_abbr = COALESCE(EXCLUDED.away_team_abbr, public.polymarket_game_map.away_team_abbr),
        game_date = COALESCE(EXCLUDED.game_date, public.polymarket_game_map.game_date),
        last_verified_ts = EXCLUDED.last_verified_ts
    WHERE public.polymarket_game_map.is_manual_override IS DISTINCT FROM true;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            dict(
                game_id=game_id,
                polymarket_slug=polymarket_slug,
                gamma_event_id=gamma_event_id,
                gamma_market_id=gamma_market_id,
                clob_market_id=clob_market_id,
                token_yes_id=token_yes_id,
                token_no_id=token_no_id,
                home_team_abbr=home_abbr,
                away_team_abbr=away_abbr,
                game_date=game_date,
                last_verified_ts=verified_ts,
            ),
        )


def insert_polymarket_quote(conn, game_id: str, polymarket_slug: str, clob_market_id: Optional[str], token_yes_id: str, q: Quote):
    sql = """
    INSERT INTO public.polymarket_quotes (
        asof_ts, game_id, polymarket_slug, clob_market_id, token_yes_id,
        bid, ask, mid, liquidity, liquidity_top_total, liquidity_depth_2c
    )
    VALUES (
        %(asof_ts)s, %(game_id)s, %(polymarket_slug)s, %(clob_market_id)s, %(token_yes_id)s,
        %(bid)s, %(ask)s, %(mid)s, %(liquidity)s, %(liquidity_top_total)s, %(liquidity_depth_2c)s
    )
    ON CONFLICT (asof_ts, game_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            dict(
                asof_ts=q.asof_ts,
                game_id=game_id,
                polymarket_slug=polymarket_slug,
                clob_market_id=clob_market_id,
                token_yes_id=token_yes_id,
                bid=q.bid,
                ask=q.ask,
                mid=q.mid,
                liquidity=None,
                liquidity_top_total=q.liquidity_top_total,
                liquidity_depth_2c=q.liquidity_depth_2c,
            ),
        )


def append_both_outcomes_csv(path: str, rows: List[Dict[str, Any]]):
    # create if not exists, append otherwise
    exists = os.path.exists(path)
    fieldnames = [
        "asof_ts",
        "game_id",
        "polymarket_slug",
        "gamma_market_id",
        "clob_market_id",
        "outcome_index",
        "outcome_name",
        "token_id",
        "bid",
        "ask",
        "mid",
        "liquidity_top_total",
        "liquidity_depth_2c",
    ]
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def get_conn(database_url: str):
    return psycopg2.connect(database_url)


def load_single(game_id: str, slug: str, database_url: str, both_outcomes_csv: Optional[str] = None):
    verified_ts = datetime.now(tz=timezone.utc)
    gamma_market = gamma_get_market_by_slug(slug)

    gamma_market_id = gamma_market.get("id")
    clob_market_id = gamma_market.get("conditionId")

    outcomes = _parse_outcomes(gamma_market.get("outcomes"))
    token_ids = _parse_clob_token_ids(gamma_market.get("clobTokenIds"))
    if len(token_ids) < 2:
        raise RuntimeError(f"Expected 2 clobTokenIds for binary market; got: {gamma_market.get('clobTokenIds')}")

    # Fetch both books
    books = [clob_get_book(token_ids[0]), clob_get_book(token_ids[1])]
    quotes = [compute_quote_from_book(books[0]), compute_quote_from_book(books[1])]

    # Ensure a consistent asof timestamp for both sides: use min(book timestamps)
    asof_ts = min(q.asof_ts for q in quotes)
    for q in quotes:
        q.asof_ts = asof_ts

    # Upsert map + insert the outcome[0] quote into polymarket_quotes
    with get_conn(database_url) as conn:
        upsert_polymarket_game_map(conn, game_id, slug, gamma_market, verified_ts)
        insert_polymarket_quote(conn, game_id, slug, clob_market_id, token_ids[0], quotes[0])
        conn.commit()

    # Optionally emit both outcomes to CSV
    if both_outcomes_csv:
        rows = []
        for i in (0, 1):
            rows.append(
                dict(
                    asof_ts=asof_ts.isoformat(),
                    game_id=game_id,
                    polymarket_slug=slug,
                    gamma_market_id=gamma_market_id,
                    clob_market_id=clob_market_id,
                    outcome_index=i,
                    outcome_name=(outcomes[i] if i < len(outcomes) else None),
                    token_id=token_ids[i],
                    bid=quotes[i].bid,
                    ask=quotes[i].ask,
                    mid=quotes[i].mid,
                    liquidity_top_total=quotes[i].liquidity_top_total,
                    liquidity_depth_2c=quotes[i].liquidity_depth_2c,
                )
            )
        append_both_outcomes_csv(both_outcomes_csv, rows)


def iter_mappings_from_csv(path: str) -> Iterable[Tuple[str, str]]:
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_id = (row.get("game_id") or "").strip()
            slug = (row.get("polymarket_slug") or row.get("slug") or "").strip()
            if game_id and slug:
                yield game_id, slug


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.getenv("DATABASE_URL"), help="postgresql://... (or set DATABASE_URL)")
    ap.add_argument("--game-id", help="Your internal game_id")
    ap.add_argument("--slug", help="Polymarket market slug")
    ap.add_argument("--mapping-csv", help="CSV with columns game_id,polymarket_slug")
    ap.add_argument("--both-outcomes-csv", help="Append both outcomes quotes to this CSV path")
    args = ap.parse_args(argv)

    if not args.database_url:
        print("ERROR: Provide --database-url or set DATABASE_URL", file=sys.stderr)
        return 2

    if args.mapping_csv:
        for game_id, slug in iter_mappings_from_csv(args.mapping_csv):
            load_single(game_id=game_id, slug=slug, database_url=args.database_url, both_outcomes_csv=args.both_outcomes_csv)
        return 0

    if not (args.game_id and args.slug):
        print("ERROR: Provide --game-id and --slug (or --mapping-csv)", file=sys.stderr)
        return 2

    load_single(game_id=args.game_id, slug=args.slug, database_url=args.database_url, both_outcomes_csv=args.both_outcomes_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

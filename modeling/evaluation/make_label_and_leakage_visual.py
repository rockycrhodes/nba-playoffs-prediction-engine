#!/usr/bin/env python3

DENSE_RANK_MAX_SEED_RANGE = "11–13"
ROW_NUMBER_MAX_SEED = "15"

TOP6_BEFORE, TOP6_AFTER = "~0.50", "0.400"
TOP10_BEFORE, TOP10_AFTER = "~0.81", "0.667"

CHECKPOINTS = [
    ("CP20", "team_game_number = 21"),
    ("CP40", "team_game_number = 41"),
    ("CP60", "team_game_number = 61"),
]

print("\n=== Label integrity (dense_rank vs row_number) ===\n")
print(f"Max seed (per conference):  DENSE_RANK -> {DENSE_RANK_MAX_SEED_RANGE}   |   ROW_NUMBER -> {ROW_NUMBER_MAX_SEED}")
print(f"Top-6 base rate:            DENSE_RANK -> {TOP6_BEFORE}   |   ROW_NUMBER -> {TOP6_AFTER}")
print(f"Top-10 base rate:           DENSE_RANK -> {TOP10_BEFORE}   |   ROW_NUMBER -> {TOP10_AFTER}")
print("\nWhy it broke:")
print("- DENSE_RANK assigns the same rank to ties, compressing seed values.")
print("- Compressed seeds inflate labels like seed <= 6 and seed <= 10.\n")
print("Fix:")
print("- Use ROW_NUMBER() (or RANK() + tie-breaks) to force seeds 1..15 per conference.\n")

print("=== Leakage-safe checkpoint alignment (pregame) ===\n")
print("Games 1..N completed (postgame stats exist)")
print("  ↓")
print("Pregame of game N+1 (features computed using only games 1..N)")
print("  ↓")
print("Checkpoint row uses team_game_number = N+1 (prevents postgame leakage)\n")
print("Concrete mapping:")
for cp, rule in CHECKPOINTS:
    print(f"- {cp}: {rule}")
print()

import pandas as pd
import glob

def transform_player_stats(file_list, out_file='player_stats_for_load_bulk.csv'):
    dfs = []
    for f in file_list:
        try:
            df_part = pd.read_csv(f)
            # Skip empty
            if df_part.shape[0] == 0:
                print(f"SKIPPING empty file: {f}")
                continue
            # Check for existence of essential columns
            if not (
                'gameId' in df_part.columns and 'personId' in df_part.columns
            ):
                print(f"SKIPPING {f} due to missing gameId or personId columns (columns: {df_part.columns.tolist()})")
                continue
            dfs.append(df_part)
        except pd.errors.EmptyDataError:
            print(f"SKIPPING truly empty file: {f}")
        except Exception as e:
            print(f"SKIPPING {f} due to error: {e}")
    if not dfs:
        print("No valid dataframes found. Exiting.")
        return
    df = pd.concat(dfs, ignore_index=True)

    # -- Column mapping for camelCase extraction --
    rename_cols = {
        'gameId': 'game_id',
        'teamId': 'team_id',
        'personId': 'player_id',
        'firstName': 'first_name',
        'familyName': 'last_name',
        'teamCity': 'team_city',
        'teamName': 'team_name',
        'teamTricode': 'team_abbreviation',
        'minutes': 'minutes',
        'position': 'start_position',
        'comment': 'comment',
        'fieldGoalsMade': 'fgm',
        'fieldGoalsAttempted': 'fga',
        'fieldGoalsPercentage': 'fg_pct',
        'threePointersMade': 'fg3m',
        'threePointersAttempted': 'fg3a',
        'threePointersPercentage': 'fg3_pct',
        'freeThrowsMade': 'ftm',
        'freeThrowsAttempted': 'fta',
        'freeThrowsPercentage': 'ft_pct',
        'reboundsOffensive': 'oreb',
        'reboundsDefensive': 'dreb',
        'reboundsTotal': 'reb',
        'assists': 'ast',
        'steals': 'stl',
        'blocks': 'blk',
        'turnovers': 'turnovers',
        'foulsPersonal': 'pf',
        'points': 'pts',
        'plusMinusPoints': 'plus_minus',
        'SEASON': 'season',   # extractions often add these columns at the end
        'SEASON_TYPE': 'season_type'
    }

    # Prepare the DataFrame with renamed columns, ignoring missing.
    df_clean = df.rename(columns=rename_cols)

    # Restrict to only those intended columns that exist in the data
    desired_cols = [
        'game_id', 'team_id', 'player_id', 'first_name', 'last_name', 'team_city',
        'team_name', 'team_abbreviation', 'minutes', 'start_position', 'comment',
        'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct', 'ftm', 'fta', 'ft_pct',
        'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'turnovers', 'pf', 'pts',
        'plus_minus', 'season', 'season_type'
    ]
    # Only include columns that are present in df_clean
    present_cols = [col for col in desired_cols if col in df_clean.columns]
    df_clean = df_clean[present_cols]

    # Drop duplicates if both columns are present
    drop_dupe_cols = [col for col in ['game_id', 'player_id'] if col in df_clean.columns]
    if len(drop_dupe_cols) > 0:
        df_clean = df_clean.drop_duplicates(subset=drop_dupe_cols)
    df_clean.to_csv(out_file, index=False)
    print(f"Transformed data shape: {df_clean.shape}")

if __name__ == '__main__':
    # Use glob to find all relevant files
    file_list = glob.glob('bulk_player_stats_202*.csv') + glob.glob('partial_player_stats_202*.csv')
    transform_player_stats(file_list, out_file='player_stats_for_load_bulk.csv')

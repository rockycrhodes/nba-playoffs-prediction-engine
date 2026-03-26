#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import smtplib
import subprocess
import traceback
import sys
import os
import pandas as pd
from datetime import date, timedelta
from email.mime.text import MIMEText

# --- CONFIGURATION ---
BASE_DIR = "/Users/rrhodes/Documents/NBA"
if not os.path.isdir(BASE_DIR):
    raise RuntimeError(f"BASE_DIR does not exist: {BASE_DIR}")
os.chdir(BASE_DIR)

LOGFILE = '/Users/rrhodes/Documents/NBA/nba_daily.log'
EMAIL_FROM = 'rockycrhodes@gmail.com'
EMAIL_TO = 'rockycrhodes@gmail.com'
EMAIL_SUBJECT = 'NBA ETL Daily Pipeline: FAILURE'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_USER = 'rockycrhodes@gmail.com'
SMTP_PASSWORD = 'qtlb kxfy wxwd zctq'  # ideally: os.environ["NBA_ETL_GMAIL_APP_PW"]


# Expected output files from your scripts (adjust if your filenames differ)
RAW_GAMES = "raw_games_today.csv"
RAW_TEAM_STATS = "raw_team_stats_today.csv"
RAW_PLAYER_STATS = "raw_player_stats_today.csv"
RAW_STANDINGS = "raw_standings.csv"  # or whatever your standings_extract writes

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler()]
)

def send_email(subject, body):
    msg = MIMEText(body)
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
        logging.info("Error notification email sent.")
    except Exception as e:
        logging.error(f"Failed to send error email: {e}")

def run_script(script_name: str):
    """Run a python script using the current interpreter."""
    logging.info(f"Running: {script_name}")
    subprocess.run([sys.executable, script_name], check=True)

def csv_has_rows(path: str) -> bool:
    """True if CSV exists and has at least 1 data row."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    try:
        df = pd.read_csv(path)
        return df.shape[0] > 0
    except Exception:
        return False

def main():
    yday = date.today() - timedelta(days=1)
    logging.info(f"Daily ETL starting for games date: {yday}")

    try:
        # 1) Games (drives whether there is anything to do)
        run_script("games_daily_extract.py")

        if not csv_has_rows(RAW_GAMES):
            logging.info(f"No games found in {RAW_GAMES}; skipping rest of pipeline.")
            return

        run_script("games_daily_transform.py")
        run_script("games_daily_load.py")

        # 2) Team stats
        run_script("team_stats_daily_extract.py")
        if csv_has_rows(RAW_TEAM_STATS):
            run_script("team_stats_daily_transform.py")
            run_script("team_stats_daily_load.py")
        else:
            logging.info(f"No team stats rows in {RAW_TEAM_STATS}; skipping team stats transform/load.")

        # 3) Player stats
        run_script("player_stats_daily_extract.py")
        if csv_has_rows(RAW_PLAYER_STATS):
            run_script("player_stats_daily_transform.py")
            run_script("player_stats_daily_load.py")
        else:
            logging.info(f"No player stats rows in {RAW_PLAYER_STATS}; skipping player stats transform/load.")

        # 4) Standings (standings can exist even on off-days; your call)
        run_script("standings_extract.py")
        if csv_has_rows(RAW_STANDINGS):
            run_script("standings_transform.py")
            run_script("standings_load.py")
        else:
            logging.info(f"No standings rows in {RAW_STANDINGS}; skipping standings transform/load.")

        logging.info("All NBA ETL scripts completed successfully!")

    except Exception as err:
        logging.error(f"Error in ETL: {err}")
        tb_str = traceback.format_exc()
        send_email(
            EMAIL_SUBJECT,
            f"NBA ETL pipeline failed.\n\nDate checked: {yday}\n\nError: {err}\n\nTraceback:\n{tb_str}"
        )
        raise  # optional: keep so cron shows non-zero exit

if __name__ == "__main__":
    main()

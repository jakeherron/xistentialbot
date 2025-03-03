import tweepy
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import pytz
import sys
import logging
import time
from dotenv import load_dotenv
import os

# Configure logging at module level to catch startup errors
try:
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler(sys.stdout))  # Ensure console output
    logger.debug("Logging initialized successfully")
except Exception as e:
    with open("/tmp/startup_error.log", "a") as f:
        f.write(f"Startup error: {e}\n{os.environ}\n")  # Log environment vars
    raise

# Ensure output is flushed immediately
sys.stdout.reconfigure(line_buffering=True)

load_dotenv()
BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN")
API_KEY = os.environ.get("X_API_KEY")
API_SECRET = os.environ.get("X_API_SECRET")
ACCESS_TOKEN = os.environ.get("X_ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET")

# Google Sheets
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    CREDS = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
    CLIENT = gspread.authorize(CREDS)
    SHEET = CLIENT.open_by_key("1nUu4RrZ32Eqnk5gdC2KBXCsJVLB1pninJ7GUxv5dQXk").sheet1
    logger.debug("Sheet connection established successfully")
except Exception as e:
    logger.error(f"Error connecting to sheet: {e}", exc_info=True)

# X API setup
try:
    client = tweepy.Client(
        consumer_key=API_KEY,
        consumer_secret=API_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
        wait_on_rate_limit=True
    )
    logger.debug("X API (v2) connection established successfully")
except Exception as e:
    logger.error(f"Error connecting to X API: {e}", exc_info=True)

def post_to_x(request):
    logger.debug("Entering post_to_x function")
    try:
        df = pd.DataFrame(SHEET.get_all_records())
        logger.debug(f"DataFrame contents: {df.to_string()}")
    except Exception as e:
        logger.error(f"Error loading DataFrame: {e}", exc_info=True)
        return f"Error loading sheet: {e}", 500

    now = datetime.now(pytz.timezone("America/Los_Angeles"))
    logger.debug(f"Current time (PST): {now}")
    WINDOW_SECONDS = 3600  # 1 hour

    for index, row in df.iterrows():
        post_id = row["Post ID"]
        post_text = row["Post"]
        try:
            scheduled_time = datetime.strptime(row["Scheduled Time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.FixedOffset(-480))
        except ValueError as e:
            logger.error(f"Invalid Scheduled Time format for post_id {post_id}: {row['Scheduled Time']}", exc_info=True)
            continue
        status = row["Status"]
        posted = row.get("Posted", "No")
        time_diff = (now - scheduled_time).total_seconds()
        logger.debug(f"Row {index}: post_id={post_id}, scheduled_time={scheduled_time}, status={status}, posted={posted}, time_diff={time_diff} seconds")

        if now >= scheduled_time and time_diff < WINDOW_SECONDS and status == "Draft" and posted == "No":
            logger.debug(f"Posting {post_id}")
            try:
                response = client.create_tweet(text=post_text)
                if response.data and 'id' in response.data:
                    logger.info(f"Posted {post_id}: {post_text} (Tweet ID: {response.data['id']})")
                    SHEET.update_cell(index + 2, 4, "Posted")
                    SHEET.update_cell(index + 2, 5, "Yes")
                else:
                    logger.warning(f"Posting failed, no tweet ID for {post_id}")
                    SHEET.update_cell(index + 2, 4, "Failed")
            except Exception as e:
                logger.error(f"Error posting {post_id}: {e}", exc_info=True)
                SHEET.update_cell(index + 2, 4, "Error")
                SHEET.update_cell(index + 2, 5, str(e))

    return "Function executed successfully", 200

def initialize_sheet():
    """Add 'Posted' column if missing with batch updates"""
    headers = SHEET.row_values(1)
    if "Posted" not in headers:
        SHEET.update_cell(1, 5, "Posted")
        updates = []
        for i in range(2, len(SHEET.get_all_values()) + 1):
            updates.append({"range": f"A{i}:E{i}", "values": [["" for _ in range(4)] + ["No"]]})
            if len(updates) >= 50:
                SHEET.batch_update(updates)
                updates = []
                time.sleep(1)
        if updates:
            SHEET.batch_update(updates)

if __name__ == "__main__":
    post_to_x()

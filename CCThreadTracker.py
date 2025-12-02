import os
import json
import gspread
from google.oauth2.service_account import Credentials
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout, RequestException
from urllib3.util.retry import Retry
import re
from datetime import datetime, timedelta
import concurrent.futures
import time
from gspread.exceptions import APIError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SERVICE_ACCOUNT_INFO = json.loads(SERVICE_ACCOUNT_JSON)

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Jeff's Thread Tracker v2")

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]

# NEW: log bot env vars
LOG_SLACK_BOT_TOKEN = os.environ.get("LOG_SLACK_BOT_TOKEN")
LOG_SLACK_CHANNEL_ID = os.environ.get("LOG_SLACK_CHANNEL_ID")

LIVE_FORUMS = os.environ.get(
    "LIVE_FORUMS", "Hot Deals,Marketplace Deals"
).split(",")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))
READ_TIMEOUT = int(os.environ.get("READ_TIMEOUT", "20"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

cookies = {
    "__ssid": os.environ.get("__SSID_COOKIE", "e93036082fb59fa25d8ac2077578d17")
}
default_headers = {
    "User-Agent": os.environ.get(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36",
    )
}

session = requests.Session()
retry_cfg = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(
    max_retries=retry_cfg,
    pool_connections=MAX_WORKERS * 2,
    pool_maxsize=MAX_WORKERS * 2,
)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update(default_headers)

creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def is_quota_error(e: Exception) -> bool:
    s = str(e)
    return "429" in s or "Quota exceeded" in s or "Rate Limit Exceeded" in s


def safe_sheet_get_range(sheet_obj, range_a1, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            return sheet_obj.get(range_a1)
        except APIError as e:
            if is_quota_error(e) and attempt < max_attempts:
                wait = 5 * attempt
                print(f"Sheets 429 on get({range_a1}), retry {attempt}/{max_attempts} in {wait}s...")
                time.sleep(wait)
                continue
            raise


def safe_batch_update(sheet_obj, updates, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            sheet_obj.batch_update(updates)
            return
        except APIError as e:
            if is_quota_error(e) and attempt < max_attempts:
                wait = 5 * attempt
                print(f"Sheets 429 on batch_update, retry {attempt}/{max_attempts} in {wait}s...")
                time.sleep(wait)
                continue
            raise


# NEW: log helper
def send_log_to_slack(level, text):
    """Send a log message to the log channel via the LOG_ Slack bot."""
    if not LOG_SLACK_BOT_TOKEN or not LOG_SLACK_CHANNEL_ID:
        # Logging bot not configured; fail silently
        return

    slack_url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LOG_SLACK_BOT_TOKEN}",
    }

    level_up = (level or "").upper()
    if level_up in ("CRITICAL", "ALERT"):
        prefix = f"<!here> *[{level_up}]* "
    else:
        prefix = f"*[{level_up or 'INFO'}]* "

    payload = {
        "channel": LOG_SLACK_CHANNEL_ID,
        "text": prefix + text,
    }

    try:
        resp = requests.post(slack_url, json=payload, headers=headers, timeout=5)
        if not resp.ok:
            print(f"Failed to send log to Slack ({level_up}): {resp.text}")
    except Exception as e:
        print(f"Error sending log to Slack ({level_up}): {e}")


def fetch_data(row_num, url):
    start_time = time.time()

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = session.get(url, cookies=cookies, timeout=READ_TIMEOUT)
            html = resp.text

            title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE)
            title = title_match.group(1).strip() if title_match else ""
            title = re.sub(r"amp;|&#x27;|&#x2F;|&quot;", "", title)
            title = re.sub(r"\s+-\s+\d{4}-\d{2}-\d{2}$", "", title)

            date_match = re.search(
                r'<meta class="swiftype" name="published_at"[^>]+content="([^"]+)"',
                html,
            )
            post_date = ""
            if date_match:
                dt = datetime.strptime(date_match.group(1).split("T")[0], "%Y-%m-%d")
                post_date = f"{dt.month}/{dt.day}/{dt.year}"

            votes_match = re.search(r'"votes":"(\d+)"', html)
            votes = votes_match.group(1) if votes_match else ""

            badge = (
                "Frontpage"
                if '"isFrontpageDeal":true' in html
                else "Popular"
                if '"isPopularDeal":true' in html
                else ""
            )

            thread_type = ""
            tf_match = re.search(r"ThreadForumView:([^:]+):", html)
            if tf_match:
                thread_type = tf_match.group(1).strip()
            else:
                f_match = re.search(r'"forum":"([^"]+)"', html)
                if f_match:
                    thread_type = f_match.group(1).strip()

            poster_match = re.search(r'"postedBy":"([^"]+)"', html)
            poster = poster_match.group(1).strip() if poster_match else ""

            price_match = re.search(r'"finalPrice":"([\d.]+)"', html)
            final_price = price_match.group(1) if price_match else ""

            elapsed = round(time.time() - start_time, 2)
            print(f"Row {row_num} scraped in {elapsed}s")

            return {
                "row": row_num,
                "post_date": post_date,
                "title": title,
                "votes": votes,
                "badge": badge,
                "thread_type": thread_type,
                "poster": poster,
                "final_price": final_price,
            }

        except Timeout as e:
            if attempt < MAX_ATTEMPTS:
                print(
                    f"Timeout scraping row {row_num} (attempt {attempt}/{MAX_ATTEMPTS}): {e}"
                )
                time.sleep(1.5 * attempt)
                continue
            else:
                print(
                    f"Error scraping row {row_num}: {e} "
                    f"(gave up after {MAX_ATTEMPTS} attempts)"
                )
                return None

        except RequestException as e:
            print(f"Request error scraping row {row_num}: {e}")
            return None

        except Exception as e:
            print(f"Error scraping row {row_num}: {e}")
            return None


def send_slack_expired_alert(row, checkbox, prev_status, new_status, thread_id, thread_link, sheet_title):
    if not (prev_status == "LIVE" and new_status == "EXPIRED"):
        return

    mention_texts = []

    poster = row["poster"].strip()
    if poster == "Meowssi | Staff":
        mention_texts.append("<@U0461S7R0L9>")
    elif poster == "Navy-Wife | Staff":
        mention_texts.append("<@U034HJT6F8W>")
    elif poster == "iconian | Staff":
        mention_texts.append("<@U0EBD4P2B>")

    if str(checkbox).lower() in ["true", "yes", "1", "checked", "☑"]:
        mention_texts.append("<@U03MWUXPALA>")

    mention_str = " ".join(mention_texts).strip()

    slack_text = f"*Thread Expired*\n\n*Title:* {sheet_title}\n*Link:* {thread_link}"
    if mention_str:
        slack_text += f"\n\n{mention_str}"

    slack_url = "https://slack.com/api/chat.postMessage"
    headers_slack = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
    }
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": slack_text},
            }
        ],
    }

    slack_resp = requests.post(slack_url, json=payload, headers=headers_slack)
    if slack_resp.ok:
        print(f"Slack alert sent for thread {thread_id}")
    else:
        print(
            f"Slack alert failed for thread {thread_id}. "
            f"Response: {slack_resp.text}"
        )


def main_loop():
    while True:
        run_start = datetime.now()

        try:
            # Log run start (no @here, level INFO)
            send_log_to_slack(
                "INFO",
                f"Run starting at {run_start.isoformat(timespec='seconds')}"
            )

            days_back = int(os.environ.get("DAYS_BACK", "120"))
            now = datetime.now()
            cutoff_date = now - timedelta(days=days_back)

            rows = safe_sheet_get_range(sheet, "A2:Q")
            if not rows:
                print("No data rows in sheet.")
                sleep_seconds = int(os.environ.get("SLEEP_SECONDS", "300"))
                # Run finished (nothing to do)
                run_end = datetime.now()
                duration = (run_end - run_start).total_seconds()
                send_log_to_slack(
                    "INFO",
                    f"Run completed at {run_end.isoformat(timespec='seconds')} "
                    f"(duration {duration:.1f}s, rows_to_process=0, rows_scraped=0, updates=0)"
                )
                time.sleep(sleep_seconds)
                continue

            thread_ids = []
            urls = []
            dates = []
            price_values = []
            prev_status_values = []
            checkbox_values = []

            for row_vals in rows:
                thread_ids.append(row_vals[0] if len(row_vals) > 0 else "")
                urls.append(row_vals[1] if len(row_vals) > 1 else "")
                dates.append(row_vals[2] if len(row_vals) > 2 else "")
            #     J (10th col, index 9), P (16th col, index 15), Q (17th col, index 16)
                price_values.append(row_vals[9] if len(row_vals) > 9 else "")
                prev_status_values.append(row_vals[15] if len(row_vals) > 15 else "")
                checkbox_values.append(row_vals[16] if len(row_vals) > 16 else "")

            rows_with_data = []
            max_len = max(len(thread_ids), len(urls), len(dates))

            for i in range(max_len):
                tid = thread_ids[i].strip() if i < len(thread_ids) else ""
                url = urls[i].strip() if i < len(urls) else ""
                date_str = dates[i].strip() if i < len(dates) else ""

                if not tid or not url:
                    continue

                if date_str:
                    try:
                        post_dt = datetime.strptime(date_str, "%m/%d/%Y")
                    except ValueError:
                        continue

                    if post_dt < cutoff_date:
                        continue

                row_num = i + 2
                rows_with_data.append((row_num, url))

            rows_to_process = rows_with_data[::-1]

            print(
                f"\nStarting scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                f"on {len(rows_to_process)} rows (last {days_back} days)..."
            )

            results = []
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_WORKERS
            ) as executor:
                for i in range(0, len(rows_to_process), MAX_WORKERS):
                    chunk = rows_to_process[i: i + MAX_WORKERS]
                    futures = [
                        executor.submit(fetch_data, row_num, url)
                        for (row_num, url) in chunk
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result:
                            results.append(result)

            results.sort(key=lambda r: r["row"])

            updates = []

            for row in results:
                row_idx = row["row"] - 2

                thread_type = row["thread_type"]
                new_status = "LIVE" if thread_type in LIVE_FORUMS else "EXPIRED"

                prev_status = (
                    prev_status_values[row_idx]
                    if row_idx < len(prev_status_values)
                    else ""
                )
                checkbox = (
                    checkbox_values[row_idx]
                    if row_idx < len(checkbox_values)
                    else ""
                )
                existing_price = (
                    price_values[row_idx]
                    if row_idx < len(price_values)
                    else ""
                )

                if not existing_price and row["final_price"]:
                    updates.append(
                        {
                            "range": f"J{row['row']}",
                            "values": [[row["final_price"]]],
                        }
                    )
                    print(
                        f"Wrote scraped price {row['final_price']} "
                        f"into Col J for row {row['row']}"
                    )

                thread_id = thread_ids[row_idx] if row_idx < len(thread_ids) else ""
                thread_link = urls[row_idx] if row_idx < len(urls) else ""
                sheet_title = row["title"]

                send_slack_expired_alert(
                    row,
                    checkbox,
                    prev_status,
                    new_status,
                    thread_id,
                    thread_link,
                    sheet_title,
                )

                updates.append(
                    {"range": f"P{row['row']}", "values": [[new_status]]}
                )

                updates.extend(
                    [
                        {
                            "range": f"C{row['row']}",
                            "values": [[row["post_date"]]],
                        },
                        {
                            "range": f"D{row['row"]}",
                            "values": [[row["title"]]],
                        },
                        {
                            "range": f"E{row['row"]}",
                            "values": [[row["votes"]]],
                        },
                        {
                            "range": f"F{row['row"]}",
                            "values": [[row["badge"]]],
                        },
                        {
                            "range": f"G{row['row"]}",
                            "values": [[row["thread_type"]]],
                        },
                        {
                            "range": f"M{row['row"]}",
                            "values": [[row["poster"]]],
                        },
                    ]
                )

            if updates:
                safe_batch_update(sheet, updates)
                print(f"Batch update complete. Rows written: {len(results)}")

            # Log run completion (no @here, level INFO)
            run_end = datetime.now()
            duration = (run_end - run_start).total_seconds()
            send_log_to_slack(
                "INFO",
                f"Run completed at {run_end.isoformat(timespec='seconds')} "
                f"(duration {duration:.1f}s, rows_to_process={len(rows_to_process)}, "
                f"rows_scraped={len(results)}, updates={len(updates)})"
            )

            sleep_seconds = int(os.environ.get("SLEEP_SECONDS", "300"))
            print(f"Sleeping {sleep_seconds} seconds...\n")
            time.sleep(sleep_seconds)

        except APIError as e:
            if is_quota_error(e):
                msg = f"Script hit Sheets quota (429). Backing off 120s. Error: {e}"
                print(msg)
                send_log_to_slack("ALERT", msg)
                time.sleep(120)
            else:
                msg = f"Google Sheets API error: {e}"
                print(msg)
                send_log_to_slack("ERROR", msg)
                time.sleep(60)

        except Exception as e:
            msg = f"Script error in main_loop: {e}"
            print(msg)
            send_log_to_slack("CRITICAL", msg)
            time.sleep(60)

if __name__ == "__main__":
    main_loop()

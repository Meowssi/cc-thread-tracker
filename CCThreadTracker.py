import os
import json
import gspread
from google.oauth2.service_account import Credentials
import requests
import re
from datetime import datetime, timedelta
import concurrent.futures
import time

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Load service account JSON from env var
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SERVICE_ACCOUNT_INFO = json.loads(SERVICE_ACCOUNT_JSON)

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Jeff's Thread Tracker v2")

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]

LIVE_FORUMS = os.environ.get("LIVE_FORUMS", "Hot Deals,Marketplace Deals").split(",")

cookies = {"__ssid": os.environ.get("__SSID_COOKIE", "e93036082fb59fa25d8ac2077578d17")}
headers = {"User-Agent": os.environ.get("USER_AGENT", "Mozilla/5.0")}

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))

creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def fetch_data(row_num, url):
    start_time = time.time()
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        html = response.text

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
            else "Popular" if '"isPopularDeal":true' in html else ""
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
    except Exception as e:
        print(f"Error scraping row {row_num}: {e}")
        return None


def send_slack_expired_alert(row, checkbox, prev_status, new_status):
    if not (prev_status == "LIVE" and new_status == "EXPIRED"):
        return

    row_num = row["row"]
    thread_id = sheet.cell(row_num, 1).value

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

    sheet_title = sheet.cell(row_num, 4).value  # Column D = Title
    thread_link = sheet.cell(row_num, 2).value  # Column B = Thread link

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
        print(f"Slack alert failed for thread {thread_id}. Response: {slack_resp.text}")


def main_loop():
    while True:
        try:
            # Only consider threads whose post date (Col C) is within the last N days
            days_back = int(os.environ.get("DAYS_BACK", "120"))
            now = datetime.now()
            cutoff_date = now - timedelta(days=days_back)

            # Read columns A (thread id), B (URL), C (post date)
            thread_ids = sheet.col_values(1)[1:]  # A
            urls = sheet.col_values(2)[1:]        # B
            dates = sheet.col_values(3)[1:]       # C

            rows_with_data = []

            for i, (tid, url, date_str) in enumerate(zip(thread_ids, urls, dates)):
                if not tid.strip() or not url.strip():
                    continue

                date_str = date_str.strip()

                if date_str:
                    # Date format on sheet: 11/26/2025 (mm/dd/yyyy)
                    try:
                        post_dt = datetime.strptime(date_str, "%m/%d/%Y")
                    except ValueError:
                        # Skip rows with bad date format
                        continue

                    # If we already have a date and it's older than cutoff, skip
                    if post_dt < cutoff_date:
                        continue
                # If date_str is blank, we still include this row so it can be scraped
                row_num = i + 2  # +2 because we skipped header row and lists are 0-based
                rows_with_data.append((row_num, url))


            # Most recent rows first (optional)
            rows_to_process = rows_with_data[::-1]

            print(
                f"\nStarting scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                f"on {len(rows_to_process)} rows (last {days_back} days)..."
            )

            # Scrape in small chunks to keep memory low
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for i in range(0, len(rows_to_process), MAX_WORKERS):
                    chunk = rows_to_process[i : i + MAX_WORKERS]
                    futures = [
                        executor.submit(fetch_data, row_num, url)
                        for row_num, url in chunk
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result:
                            results.append(result)

            results.sort(key=lambda r: r["row"])

            prev_status_values = sheet.col_values(16)[1:]  # P
            checkbox_values = sheet.col_values(17)[1:]     # Q
            price_values = sheet.col_values(10)[1:]        # J

            updates = []

            for row in results:
                row_idx = row["row"] - 2

                new_status = "LIVE" if row["thread_type"] in LIVE_FORUMS else "EXPIRED"

                prev_status = (
                    prev_status_values[row_idx] if row_idx < len(prev_status_values) else ""
                )
                checkbox = (
                    checkbox_values[row_idx] if row_idx < len(checkbox_values) else ""
                )

                existing_price = (
                    price_values[row_idx] if row_idx < len(price_values) else ""
                )

                if not existing_price and row["final_price"]:
                    updates.append(
                        {"range": f"J{row['row']}", "values": [[row["final_price"]]]}
                    )
                    print(
                        f"Wrote scraped price {row['final_price']} into Col J for row {row['row']}"
                    )

                send_slack_expired_alert(row, checkbox, prev_status, new_status)

                updates.append({"range": f"P{row['row']}", "values": [[new_status]]})

                updates.extend(
                    [
                        {"range": f"C{row['row']}", "values": [[row["post_date"]]]},
                        {"range": f"D{row['row']}", "values": [[row["title"]]]},
                        {"range": f"E{row['row']}", "values": [[row["votes"]]]},
                        {"range": f"F{row['row']}", "values": [[row["badge"]]]},
                        {"range": f"G{row['row']}", "values": [[row["thread_type"]]]},
                        {"range": f"M{row['row']}", "values": [[row["poster"]]]},
                    ]
                )

            if updates:
                sheet.batch_update(updates)
                print(f"Batch update complete. Rows written: {len(results)}")

            sleep_seconds = int(os.environ.get("SLEEP_SECONDS", "300"))
            print(f"Sleeping {sleep_seconds} seconds...\n")
            time.sleep(sleep_seconds)

        except Exception as e:
            print(f"Script error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main_loop()

import datetime as dt
import os
import re
import sys
import requests
from pymongo import MongoClient, ASCENDING, errors

NOAA_URL = "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt"

# Use Atlas/remote Mongo via env var on Render. Falls back to localhost for local testing.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27019/")
DB_NAME   = os.getenv("DB_NAME", "space_forecasts")
COLL_NAME = os.getenv("COLL_NAME", "noaa_3day_snapshots")

def fetch_forecast() -> str:
    r = requests.get(NOAA_URL, timeout=30)
    r.raise_for_status()
    return r.text

def parse_issuance(text: str):
    m = re.search(r"Issued:\s*(\d{4})\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})\s+UTC", text)
    if not m:
        return None
    year, mon, day, hhmm = m.groups()
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    month = months.index(mon) + 1
    hour, minute = int(hhmm[:2]), int(hhmm[2:])
    return dt.datetime(int(year), month, int(day), hour, minute, tzinfo=dt.timezone.utc)

def main():
    if not MONGO_URI:
        print("[ERROR] MONGO_URI not set", file=sys.stderr)
        sys.exit(1)

    now_utc = dt.datetime.now(dt.timezone.utc)
    snapshot_date = now_utc.date().isoformat()  # daily de-dupe key

    raw = fetch_forecast()
    issuance_utc = parse_issuance(raw)  # may be None

    doc = {
        "snapshot_saved_utc": now_utc,
        "snapshot_date": snapshot_date,
        "issuance_utc": issuance_utc,
        "forecast_raw": raw,
        "source": {"name": "NOAA SWPC 3-day geomagnetic forecast", "url": NOAA_URL},
        "runner": "render-cron"
    }

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        coll = client[DB_NAME][COLL_NAME]
        coll.create_index([("snapshot_date", ASCENDING)], unique=True)
        coll.insert_one(doc)
        print(f"[OK] Saved daily snapshot for {snapshot_date}")
        return 0
    except errors.DuplicateKeyError:
        print(f"[SKIP] Snapshot for {snapshot_date} already exists.")
        return 0
    except Exception as e:
        print(f"[ERROR] Mongo save failed: {e}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())

import datetime as dt
import requests
import re
from pymongo import MongoClient, ASCENDING, errors
import os

NOAA_URL = "https://services.swpc.noaa.gov/text/27-day-outlook.txt"

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME   = os.environ.get("DB_NAME", "space_forecasts")
COLL_NAME = os.environ.get("COLL_NAME", "noaa_27day_snapshots")

def fetch_forecast():
    r = requests.get(NOAA_URL, timeout=30)
    r.raise_for_status()
    return r.text

def main():
    now = dt.datetime.now(dt.timezone.utc)
    snapshot_date = now.date().isoformat()

    raw = fetch_forecast()

    doc = {
        "snapshot_saved_utc": now,
        "snapshot_date": snapshot_date,
        "forecast_raw": raw,
        "source": {"name": "NOAA SWPC 27-day outlook", "url": NOAA_URL},
    }

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        coll = client[DB_NAME][COLL_NAME]

        coll.create_index([("snapshot_date", ASCENDING)], unique=True)

        coll.insert_one(doc)
        print(f"[OK] Saved 27-day forecast snapshot for {snapshot_date}")

    except errors.DuplicateKeyError:
        print(f"[SKIP] Snapshot for {snapshot_date} already exists.")
    except Exception as e:
        print(f"[ERROR] Mongo save failed: {e}")

if __name__ == "__main__":
    main()

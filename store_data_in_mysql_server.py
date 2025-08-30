import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import sys

# --- Load .env file ---
load_dotenv()

# --- Read environment variables ---
DB_CONFIG = {
    "host": os.getenv("HOST"),
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "database": os.getenv("DATABASE"),
}

TABLE_NAME = os.getenv("TABLE_NAME")
CSV_FILE = os.getenv("CSV_FILE")

# --- Validate inputs ---
if not TABLE_NAME or not CSV_FILE:
    print("❌ Missing TABLE_NAME or CSV_FILE in .env")
    sys.exit(1)

if not os.path.exists(CSV_FILE):
    print(f"❌ CSV file not found: {CSV_FILE}")
    sys.exit(1)

# --- Read CSV ---
try:
    df = pd.read_csv(CSV_FILE)
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
    sys.exit(1)

# --- Fix columns dynamically ---
expected_cols = ["date", "open", "high", "low", "close", "volume"]

if df.shape[1] == 6:
    df.columns = expected_cols
elif df.shape[1] > 6:
    df = df.iloc[:, :6]  # take first 6 cols
    df.columns = expected_cols
else:
    print(f"❌ CSV has {df.shape[1]} columns, expected 6")
    sys.exit(1)

# --- Parse timestamp ---
try:
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if df["date"].isnull().any():
        print("⚠️ Warning: Some timestamps could not be parsed")
except Exception as e:
    print(f"❌ Error parsing dates: {e}")
    sys.exit(1)

# --- Connect to MySQL ---
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)

# --- Insert query ---
insert_query = f"""
INSERT INTO {TABLE_NAME} (timestamp, open, high, low, close, volume)
VALUES (%s, %s, %s, %s, %s, %s)
"""

# --- Prepare data ---
data = []
for row in df.itertuples(index=False):
    try:
        data.append((
            row.date.to_pydatetime(),
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            int(row.volume)
        ))
    except Exception as e:
        print(f"⚠️ Skipping row due to error: {e}")

# --- Insert into DB ---
try:
    cursor.executemany(insert_query, data)
    conn.commit()
    print(f"✅ Inserted {cursor.rowcount} rows into {TABLE_NAME}")
except Exception as e:
    print(f"❌ Insert failed: {e}")
finally:
    cursor.close()
    conn.close()

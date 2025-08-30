import mysql.connector
import os
import pandas as pd
from dotenv import load_dotenv

# Load MySQL Credentials from .env
load_dotenv()

HOST = os.getenv("HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
CSV_FILE = os.getenv("CSV_FILE")

# Connect to MySQL
try:
    conn = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    print("✅ Connected to MySQL")
except mysql.connector.Error as err:
    print(f"❌ Error: {err}")
    exit(1)

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS hdfc_ohlcv (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATETIME,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
)
"""
cursor.execute(create_table_query)
conn.commit()

# Read CSV in chunks
chunksize = 5000  # you can adjust based on memory
try:
    for chunk in pd.read_csv(CSV_FILE, chunksize=chunksize):
        data = [
            (
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            )
            for _, row in chunk.iterrows()
        ]
        
        insert_query = """
        INSERT INTO hdfc_ohlcv (timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"✅ Inserted {len(data)} rows")
except Exception as e:
    print(f"❌ Error loading CSV: {e}")
finally:
    cursor.close()
    conn.close()
    print("🔒 MySQL connection closed")

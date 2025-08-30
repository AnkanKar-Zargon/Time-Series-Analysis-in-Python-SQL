# 📈 Time-Series Analysis in Python + MySQL

This project demonstrates how to perform **time-series analysis** on HDFC Bank’s minute-level stock data by storing it in a MySQL database and analyzing it with Python.

---

## 🗄️ Database Setup

First, create the database and table in MySQL:

```sql
-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS hdfc_db;

-- Select the database
USE hdfc_db;

-- Create OHLCV table for HDFC Bank data
CREATE TABLE IF NOT EXISTS hdfc_ohlcv (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    open FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT NOT NULL,
    volume BIGINT NOT NULL
);
```

Then run the python script `store_data_in_mysql_server.py` to store data in the server. 

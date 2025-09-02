-- ==============================================
-- HDFC OHLCV Data Analysis Queries
-- Run with: ./run_data_analysis.sh with proper entries in .env
-- ==============================================

-- ==============================================
-- Section: current_database
SELECT DATABASE() AS current_database;

-- ==============================================
-- Section: overview
SELECT 
    COUNT(*) AS total_rows,
    MIN(timestamp) AS start_date,
    MAX(timestamp) AS end_date
FROM hdfc_ohlcv;

-- ==============================================
-- Section: daily_summary
SELECT 
    DATE(timestamp) AS trade_date,
    MIN(open) AS day_open,
    MAX(high) AS day_high,
    MIN(low) AS day_low,
    MAX(timestamp) AS close_time,
    SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close,
    SUM(volume) AS total_volume
FROM hdfc_ohlcv
GROUP BY DATE(timestamp)
ORDER BY trade_date;

-- ==============================================
-- Section: daily_returns
SELECT 
    trade_date,
    day_close,
    ROUND(LOG(day_close / LAG(day_close) OVER (ORDER BY trade_date)) * 100, 4) AS log_return_pct
FROM (
    SELECT 
        DATE(timestamp) AS trade_date,
        SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close
    FROM hdfc_ohlcv
    GROUP BY DATE(timestamp)
) t;

-- ==============================================
-- Section: moving_averages
SELECT 
    trade_date,
    day_close,
    ROUND(AVG(day_close) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS ma5,
    ROUND(AVG(day_close) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 2) AS ma20
FROM (
    SELECT 
        DATE(timestamp) AS trade_date,
        SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close
    FROM hdfc_ohlcv
    GROUP BY DATE(timestamp)
) t;

-- ==============================================
-- Section: volatility
SELECT 
    trade_date,
    log_return_pct,
    ROUND(STDDEV_SAMP(log_return_pct) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 4) AS rolling_volatility
FROM (
    SELECT 
        trade_date,
        ROUND(LOG(day_close / LAG(day_close) OVER (ORDER BY trade_date)) * 100, 4) AS log_return_pct
    FROM (
        SELECT 
            DATE(timestamp) AS trade_date,
            SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close
        FROM hdfc_ohlcv
        GROUP BY DATE(timestamp)
    ) t
) r;

-- ==============================================
-- Section: top_volume
SELECT 
    DATE(timestamp) AS trade_date,
    SUM(volume) AS total_volume
FROM hdfc_ohlcv
GROUP BY DATE(timestamp)
ORDER BY total_volume DESC
LIMIT 10;

-- ==============================================
-- Section: gaps
SELECT 
    d1.trade_date AS prev_date,
    d1.day_close AS prev_close,
    d2.trade_date AS curr_date,
    d2.day_open AS curr_open,
    ROUND((d2.day_open - d1.day_close) / d1.day_close * 100, 2) AS gap_pct
FROM (
    SELECT DATE(timestamp) AS trade_date,
           MIN(open) AS day_open,
           SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close
    FROM hdfc_ohlcv
    GROUP BY DATE(timestamp)
) d1
JOIN (
    SELECT DATE(timestamp) AS trade_date,
           MIN(open) AS day_open,
           SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY timestamp DESC), ',', 1) AS day_close
    FROM hdfc_ohlcv
    GROUP BY DATE(timestamp)
) d2
ON d2.trade_date = DATE_ADD(d1.trade_date, INTERVAL 1 DAY);


CREATE OR REPLACE TABLE account_features_daily AS
WITH daily AS (
    SELECT
        account_id,
        DATE(timestamp) AS tx_date,

        COUNT(*) AS tx_count_1d,
        SUM(ABS(amount)) AS total_amount_1d,
        COUNT(DISTINCT merchant_id) AS unique_merchants_1d,

        -- % of transactions under 10k (simple structuring signal)
        SUM(CASE WHEN ABS(amount) < 10000 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_under_10k_1d

    FROM fact_transactions
    GROUP BY account_id, DATE(timestamp)
),

rolling AS (
    SELECT
        account_id,
        tx_date,
        tx_count_1d,
        total_amount_1d,
        unique_merchants_1d,
        pct_under_10k_1d,

        SUM(tx_count_1d) OVER (
            PARTITION BY account_id
            ORDER BY tx_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tx_count_7d,

        SUM(total_amount_1d) OVER (
            PARTITION BY account_id
            ORDER BY tx_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS total_amount_7d,

        SUM(unique_merchants_1d) OVER (
            PARTITION BY account_id
            ORDER BY tx_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS merchant_days_7d

    FROM daily
)

SELECT * FROM rolling;

CREATE OR REPLACE TABLE aml_alerts AS
WITH enriched AS (
    SELECT
        f.account_id,
        DATE(f.timestamp) AS alert_date,

        -- daily aggregates directly from transactions for better structuring signal
        SUM(CASE WHEN f.direction = 'credit' AND ABS(f.amount) BETWEEN 9000 AND 10000 THEN 1 ELSE 0 END) AS near_threshold_credits_1d,
        SUM(CASE WHEN f.direction = 'credit' AND ABS(f.amount) BETWEEN 9000 AND 10000 THEN ABS(f.amount) ELSE 0 END) AS near_threshold_amount_1d

    FROM fact_transactions f
    GROUP BY f.account_id, DATE(f.timestamp)
),

scored AS (
    SELECT
        a.account_id,
        a.tx_date AS alert_date,

        a.tx_count_1d,
        a.tx_count_7d,
        a.total_amount_7d,

        COALESCE(e.near_threshold_credits_1d, 0) AS near_threshold_credits_1d,
        COALESCE(e.near_threshold_amount_1d, 0) AS near_threshold_amount_1d,

        -- percentile-tuned signals
        CASE WHEN a.tx_count_7d >= 33 THEN 40 ELSE 0 END AS velocity_score,
        CASE WHEN a.total_amount_7d >= 35000 THEN 30 ELSE 0 END AS volume_score,

        -- structuring-style signal (based on your injected pattern)
        CASE WHEN COALESCE(e.near_threshold_credits_1d, 0) >= 3 THEN 45 ELSE 0 END AS structuring_score,

        (
            CASE WHEN a.tx_count_7d >= 33 THEN 40 ELSE 0 END +
            CASE WHEN a.total_amount_7d >= 35000 THEN 30 ELSE 0 END +
            CASE WHEN COALESCE(e.near_threshold_credits_1d, 0) >= 3 THEN 45 ELSE 0 END
        ) AS risk_score

    FROM account_features_daily a
    LEFT JOIN enriched e
      ON a.account_id = e.account_id
     AND a.tx_date = e.alert_date
),

classified AS (
    SELECT
        *,
        CASE
            WHEN risk_score >= 70 THEN 'HIGH'
            WHEN risk_score >= 40 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level
    FROM scored
)

SELECT
    account_id,
    alert_date,
    risk_score,
    risk_level,

    TRIM(BOTH ',' FROM
        (CASE WHEN velocity_score > 0 THEN 'HIGH_VELOCITY,' ELSE '' END) ||
        (CASE WHEN structuring_score > 0 THEN 'NEAR_THRESHOLD_CREDITS,' ELSE '' END) ||
        (CASE WHEN volume_score > 0 THEN 'HIGH_VOLUME_7D,' ELSE '' END)
    ) AS reason_codes,

    struct_pack(
        tx_count_1d := tx_count_1d,
        tx_count_7d := tx_count_7d,
        total_amount_7d := total_amount_7d,
        near_threshold_credits_1d := near_threshold_credits_1d,
        near_threshold_amount_1d := near_threshold_amount_1d
    ) AS evidence

FROM classified
WHERE risk_score >= 40;

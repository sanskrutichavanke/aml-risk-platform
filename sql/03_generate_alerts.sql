CREATE OR REPLACE TABLE aml_alerts AS
WITH scored AS (
    SELECT
        account_id,
        tx_date AS alert_date,

        -- bring forward feature columns for evidence
        tx_count_1d,
        tx_count_7d,
        pct_under_10k_1d,
        total_amount_7d,

        -- individual risk signals
        CASE WHEN tx_count_7d >= 120 THEN 40 ELSE 0 END AS velocity_score,
        CASE WHEN pct_under_10k_1d >= 0.9 AND tx_count_1d >= 8 THEN 35 ELSE 0 END AS structuring_score,
        CASE WHEN total_amount_7d >= 250000 THEN 25 ELSE 0 END AS volume_score,

        -- total risk score
        (
            CASE WHEN tx_count_7d >= 120 THEN 40 ELSE 0 END +
            CASE WHEN pct_under_10k_1d >= 0.9 AND tx_count_1d >= 8 THEN 35 ELSE 0 END +
            CASE WHEN total_amount_7d >= 250000 THEN 25 ELSE 0 END
        ) AS risk_score

    FROM account_features_daily
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
        (CASE WHEN structuring_score > 0 THEN 'STRUCTURING_PATTERN,' ELSE '' END) ||
        (CASE WHEN volume_score > 0 THEN 'HIGH_VOLUME,' ELSE '' END)
    ) AS reason_codes,

    struct_pack(
        tx_count_1d := tx_count_1d,
        tx_count_7d := tx_count_7d,
        pct_under_10k_1d := pct_under_10k_1d,
        total_amount_7d := total_amount_7d
    ) AS evidence


FROM classified
WHERE risk_score >= 40;

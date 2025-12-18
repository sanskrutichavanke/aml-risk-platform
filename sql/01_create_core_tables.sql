-- DIMENSIONS

CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    customer_id,
    full_name,
    email,
    phone,
    address,
    created_at
FROM raw_customers;

CREATE OR REPLACE TABLE dim_account AS
SELECT DISTINCT
    account_id,
    customer_id,
    account_type,
    opened_at
FROM raw_accounts;

CREATE OR REPLACE TABLE dim_merchant AS
SELECT DISTINCT
    merchant_id,
    merchant_name,
    category,
    country
FROM raw_merchants;

-- FACT TABLE

CREATE OR REPLACE TABLE fact_transactions AS
SELECT
    transaction_id,
    timestamp,
    account_id,
    merchant_id,
    direction,
    amount,
    channel,
    description,
    is_suspicious_ground_truth,
    pattern
FROM raw_transactions;

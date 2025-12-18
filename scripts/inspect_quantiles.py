import duckdb

con = duckdb.connect("db/aml.duckdb")

df = con.execute("""
SELECT
  approx_quantile(tx_count_7d, 0.90) AS q90_tx7,
  approx_quantile(tx_count_7d, 0.95) AS q95_tx7,
  approx_quantile(tx_count_7d, 0.99) AS q99_tx7,
  approx_quantile(total_amount_7d, 0.90) AS q90_amt7,
  approx_quantile(total_amount_7d, 0.95) AS q95_amt7,
  approx_quantile(total_amount_7d, 0.99) AS q99_amt7,
  approx_quantile(pct_under_10k_1d, 0.90) AS q90_pct,
  approx_quantile(pct_under_10k_1d, 0.95) AS q95_pct
FROM account_features_daily;
""").fetchdf()

print(df)

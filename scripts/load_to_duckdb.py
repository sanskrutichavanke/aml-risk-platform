import duckdb
from pathlib import Path

DB_PATH = Path("db/aml.duckdb")

RAW_DIR = Path("data/raw")

con = duckdb.connect(DB_PATH)

# Drop tables if re-running
con.execute("""
DROP TABLE IF EXISTS raw_customers;
DROP TABLE IF EXISTS raw_accounts;
DROP TABLE IF EXISTS raw_merchants;
DROP TABLE IF EXISTS raw_transactions;
""")

# Load CSVs
con.execute("""
CREATE TABLE raw_customers AS
SELECT * FROM read_csv_auto('data/raw/customers.csv');
""")

con.execute("""
CREATE TABLE raw_accounts AS
SELECT * FROM read_csv_auto('data/raw/accounts.csv');
""")

con.execute("""
CREATE TABLE raw_merchants AS
SELECT * FROM read_csv_auto('data/raw/merchants.csv');
""")

con.execute("""
CREATE TABLE raw_transactions AS
SELECT * FROM read_csv_auto('data/raw/transactions.csv');
""")

print("Loaded raw tables:")
print(con.execute("SHOW TABLES").fetchall())

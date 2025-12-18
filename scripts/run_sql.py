import duckdb
from pathlib import Path

DB_PATH = Path("db/aml.duckdb")
SQL_PATH = Path("sql/01_create_core_tables.sql")

con = duckdb.connect(DB_PATH)

with open(SQL_PATH, "r") as f:
    sql = f.read()

con.execute(sql)

print("SQL executed successfully.")
print("Current tables:")
print(con.execute("SHOW TABLES").fetchall())

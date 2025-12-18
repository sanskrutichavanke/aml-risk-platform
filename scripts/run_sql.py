import sys
from pathlib import Path
import duckdb

DB_PATH = Path("db/aml.duckdb")

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_sql.py <path-to-sql-file>")
        sys.exit(1)

    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"SQL file not found: {sql_path}")
        sys.exit(1)

    con = duckdb.connect(DB_PATH)

    sql = sql_path.read_text(encoding="utf-8")
    con.execute(sql)

    print("SQL executed successfully.")
    print("Tables now:")
    print(con.execute("SHOW TABLES").fetchall())

if __name__ == "__main__":
    main()

import subprocess
import sys

def run(cmd: str) -> None:
    print(f"\n>>> {cmd}")
    res = subprocess.run(cmd, shell=True)
    if res.returncode != 0:
        sys.exit(res.returncode)

def main() -> None:
    run("python scripts/generate_data.py")
    run("python scripts/load_to_duckdb.py")
    run("python scripts/run_sql.py sql/01_create_core_tables.sql")
    run("python scripts/run_sql.py sql/02_account_features_daily.sql")
    run("python scripts/run_sql.py sql/03_generate_alerts.sql")
    print("\nPipeline complete.")

if __name__ == "__main__":
    main()

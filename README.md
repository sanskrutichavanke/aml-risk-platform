# AML Risk Intelligence Platform (5-Day Build)

End-to-end transaction risk analytics system that generates explainable AML-style alerts from raw transaction data.

## What this project does
1. Generates realistic synthetic finance data with injected suspicious patterns:
   - Near-threshold deposits (structuring-style behavior)
   - High-velocity bursts
   - High-volume activity
2. Loads raw CSVs into DuckDB
3. Builds a clean analytics model:
   - `dim_customer`, `dim_account`, `dim_merchant`
   - `fact_transactions`
4. Creates rolling behavior features (`account_features_daily`)
5. Produces an explainable alerts table (`aml_alerts`) with:
   - `risk_score`, `risk_level`
   - `reason_codes`
   - `evidence` (feature snapshot)

## Architecture
CSV (raw) → DuckDB (raw tables) → core model (dim/fact) → features → risk scoring → alerts

## Data model
- Dimensions:
  - `dim_customer`
  - `dim_account`
  - `dim_merchant`
- Facts:
  - `fact_transactions`
- Feature mart:
  - `account_features_daily` (daily + 7-day rolling)
- Alerts:
  - `aml_alerts` (explainable alerts with reason codes + evidence)

## How to run (Windows)
### 1) Create and activate venv (PowerShell in VS Code)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

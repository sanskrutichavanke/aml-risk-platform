from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()
RNG_SEED = 42

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class Config:
    n_customers: int = 800
    accounts_per_customer_min: int = 1
    accounts_per_customer_max: int = 2
    n_merchants: int = 250
    days: int = 60
    base_tx_per_day: int = 3500

    structuring_threshold: float = 10000.0
    structuring_band_low: float = 9000.0
    structuring_band_high: float = 9999.0

    n_structuring_entities: int = 25
    n_velocity_entities: int = 25
    n_roundtrip_rings: int = 8

def make_customers(cfg: Config) -> pd.DataFrame:
    rows = []
    for i in range(cfg.n_customers):
        customer_id = f"C{i:05d}"
        rows.append(
            {
                "customer_id": customer_id,
                "full_name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "address": fake.address().replace("\n", ", "),
                "created_at": fake.date_time_between(start_date="-2y", end_date="-60d"),
            }
        )
    return pd.DataFrame(rows)

def make_accounts(customers: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rows = []
    acc_idx = 0
    for _, c in customers.iterrows():
        k = random.randint(cfg.accounts_per_customer_min, cfg.accounts_per_customer_max)
        for _ in range(k):
            account_id = f"A{acc_idx:06d}"
            acc_idx += 1
            rows.append(
                {
                    "account_id": account_id,
                    "customer_id": c["customer_id"],
                    "account_type": random.choice(["checking", "savings", "credit"]),
                    "opened_at": fake.date_time_between(start_date="-2y", end_date="-60d"),
                }
            )
    return pd.DataFrame(rows)

def make_merchants(cfg: Config) -> pd.DataFrame:
    rows = []
    for i in range(cfg.n_merchants):
        merchant_id = f"M{i:05d}"
        rows.append(
            {
                "merchant_id": merchant_id,
                "merchant_name": fake.company(),
                "category": random.choice(
                    ["grocery", "gas", "restaurant", "retail", "online", "travel", "utilities", "pharmacy", "other"]
                ),
                "country": random.choice(["US", "US", "US", "CA", "MX", "GB", "IN"]),
            }
        )
    return pd.DataFrame(rows)

def base_transactions(
    accounts: pd.DataFrame, merchants: pd.DataFrame, cfg: Config, start_date: datetime
) -> pd.DataFrame:
    rows = []
    tx_id = 0

    account_ids = accounts["account_id"].tolist()
    merchant_ids = merchants["merchant_id"].tolist()

    for d in range(cfg.days):
        day = start_date + timedelta(days=d)
        n = int(np.random.poisson(cfg.base_tx_per_day))

        for _ in range(n):
            account_id = random.choice(account_ids)
            merchant_id = random.choice(merchant_ids)

            # realistic-ish distribution: many small, some medium, few large
            amount = float(np.round(np.random.lognormal(mean=3.0, sigma=1.0), 2))
            if random.random() < 0.02:
                amount *= 50  # rare large spend
            amount = float(np.round(amount, 2))

            direction = random.choices(["debit", "credit"], weights=[0.86, 0.14])[0]
            signed_amount = -amount if direction == "debit" else amount

            ts = day + timedelta(seconds=random.randint(0, 86399))

            rows.append(
                {
                    "transaction_id": f"T{tx_id:09d}",
                    "timestamp": ts,
                    "account_id": account_id,
                    "merchant_id": merchant_id,
                    "direction": direction,
                    "amount": signed_amount,
                    "channel": random.choice(["card", "ach", "wire", "cash"]),
                    "description": fake.sentence(nb_words=6),
                    "is_suspicious_ground_truth": 0,
                    "pattern": None,
                }
            )
            tx_id += 1

    return pd.DataFrame(rows)

def inject_structuring(tx: pd.DataFrame, accounts: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    suspicious_accounts = accounts["account_id"].sample(cfg.n_structuring_entities, random_state=RNG_SEED).tolist()

    # For each suspicious account: add many near-threshold credits across multiple days
    new_rows = []
    max_id = tx["transaction_id"].str[1:].astype(int).max()

    for i, acc in enumerate(suspicious_accounts):
        for k in range(random.randint(18, 35)):
            day = tx["timestamp"].min() + timedelta(days=random.randint(0, cfg.days - 1))
            ts = day + timedelta(seconds=random.randint(0, 86399))

            amt = float(np.round(random.uniform(cfg.structuring_band_low, cfg.structuring_band_high), 2))
            max_id += 1
            new_rows.append(
                {
                    "transaction_id": f"T{max_id:09d}",
                    "timestamp": ts,
                    "account_id": acc,
                    "merchant_id": random.choice(tx["merchant_id"].unique().tolist()),
                    "direction": "credit",
                    "amount": amt,
                    "channel": random.choice(["cash", "ach"]),
                    "description": "Deposit",
                    "is_suspicious_ground_truth": 1,
                    "pattern": "structuring",
                }
            )

    return pd.concat([tx, pd.DataFrame(new_rows)], ignore_index=True)

def inject_velocity(tx: pd.DataFrame, accounts: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    suspicious_accounts = accounts["account_id"].sample(cfg.n_velocity_entities, random_state=RNG_SEED + 7).tolist()

    new_rows = []
    max_id = tx["transaction_id"].str[1:].astype(int).max()

    for acc in suspicious_accounts:
        # Pick a random day and create a burst in a 20-minute window
        base_day = tx["timestamp"].min() + timedelta(days=random.randint(0, cfg.days - 1))
        start = base_day + timedelta(seconds=random.randint(0, 86399 - 1200))

        burst_n = random.randint(25, 60)
        for j in range(burst_n):
            ts = start + timedelta(seconds=random.randint(0, 1200))
            amt = float(np.round(np.random.lognormal(mean=2.3, sigma=0.7), 2))
            max_id += 1
            new_rows.append(
                {
                    "transaction_id": f"T{max_id:09d}",
                    "timestamp": ts,
                    "account_id": acc,
                    "merchant_id": random.choice(tx["merchant_id"].unique().tolist()),
                    "direction": "debit",
                    "amount": -amt,
                    "channel": random.choice(["card", "ach"]),
                    "description": "Rapid purchases",
                    "is_suspicious_ground_truth": 1,
                    "pattern": "velocity",
                }
            )

    return pd.concat([tx, pd.DataFrame(new_rows)], ignore_index=True)

def inject_roundtrip_rings(tx: pd.DataFrame, accounts: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    # Create rings of 4 accounts where money cycles: A->B->C->D->A
    account_ids = accounts["account_id"].sample(cfg.n_roundtrip_rings * 4, random_state=RNG_SEED + 99).tolist()
    rings = [account_ids[i : i + 4] for i in range(0, len(account_ids), 4)]

    new_rows = []
    max_id = tx["transaction_id"].str[1:].astype(int).max()

    for ring in rings:
        base_day = tx["timestamp"].min() + timedelta(days=random.randint(0, cfg.days - 1))
        start = base_day + timedelta(seconds=random.randint(0, 86399 - 3600))
        amount = float(np.round(random.uniform(1500, 8000), 2))

        # 8 cycles
        for cycle in range(8):
            for idx in range(4):
                src = ring[idx]
                ts = start + timedelta(seconds=cycle * 900 + idx * 120 + random.randint(0, 30))
                max_id += 1
                new_rows.append(
                    {
                        "transaction_id": f"T{max_id:09d}",
                        "timestamp": ts,
                        "account_id": src,
                        "merchant_id": random.choice(tx["merchant_id"].unique().tolist()),
                        "direction": "debit",
                        "amount": -amount,
                        "channel": "wire",
                        "description": f"Transfer to {ring[(idx + 1) % 4]}",
                        "is_suspicious_ground_truth": 1,
                        "pattern": "round_trip",
                    }
                )

            # Then credit back to each next account (simplified ledger, enough for analytics)
            for idx in range(4):
                dst = ring[(idx + 1) % 4]
                ts = start + timedelta(seconds=cycle * 900 + idx * 120 + 60 + random.randint(0, 30))
                max_id += 1
                new_rows.append(
                    {
                        "transaction_id": f"T{max_id:09d}",
                        "timestamp": ts,
                        "account_id": dst,
                        "merchant_id": random.choice(tx["merchant_id"].unique().tolist()),
                        "direction": "credit",
                        "amount": amount,
                        "channel": "wire",
                        "description": f"Transfer from {ring[idx]}",
                        "is_suspicious_ground_truth": 1,
                        "pattern": "round_trip",
                    }
                )

    return pd.concat([tx, pd.DataFrame(new_rows)], ignore_index=True)

def main() -> None:
    random.seed(RNG_SEED)
    np.random.seed(RNG_SEED)

    cfg = Config()
    start_date = datetime.now() - timedelta(days=cfg.days)

    customers = make_customers(cfg)
    accounts = make_accounts(customers, cfg)
    merchants = make_merchants(cfg)

    tx = base_transactions(accounts, merchants, cfg, start_date=start_date)
    tx = inject_structuring(tx, accounts, cfg)
    tx = inject_velocity(tx, accounts, cfg)
    tx = inject_roundtrip_rings(tx, accounts, cfg)

    # Final cleanup for consistent types
    tx["timestamp"] = pd.to_datetime(tx["timestamp"])
    tx = tx.sort_values("timestamp").reset_index(drop=True)

    customers.to_csv(OUT_DIR / "customers.csv", index=False)
    accounts.to_csv(OUT_DIR / "accounts.csv", index=False)
    merchants.to_csv(OUT_DIR / "merchants.csv", index=False)
    tx.to_csv(OUT_DIR / "transactions.csv", index=False)

    print("Wrote:")
    print(f"- {OUT_DIR / 'customers.csv'}: {len(customers):,} rows")
    print(f"- {OUT_DIR / 'accounts.csv'}: {len(accounts):,} rows")
    print(f"- {OUT_DIR / 'merchants.csv'}: {len(merchants):,} rows")
    print(f"- {OUT_DIR / 'transactions.csv'}: {len(tx):,} rows")
    print()
    print("Suspicious labels breakdown:")
    print(tx["pattern"].fillna("normal").value_counts().head(10))

if __name__ == "__main__":
    main()

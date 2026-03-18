"""
Telecom Mock Data Generator
============================
Generates 4 source files for the DE project:
  1. cdr.csv            - Call Detail Records (~50,000 rows)
  2. customers.csv      - Customer master data (~5,000 rows)
  3. billing.json       - Monthly billing records (~5,000 rows)
  4. network_kpi.csv    - Network tower KPIs (~10,000 rows)

Usage:
    pip install faker          # optional — only used if available
    python generate_mock_data.py

Output folder: ./raw_data/
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

OUTPUT_DIR = "./raw_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS   = 5_000
NUM_CDR         = 50_000
NUM_TOWERS      = 200
NUM_KPI_ROWS    = 10_000

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 12, 31)

# ── Helpers ───────────────────────────────────────────────────────────────────

UK_AREA_CODES = [
    "020", "0121", "0113", "0141", "0161",
    "0117", "0114", "0151", "0191", "01865"
]
REGIONS = [
    "London", "Birmingham", "Leeds", "Glasgow", "Manchester",
    "Bristol", "Sheffield", "Liverpool", "Newcastle", "Oxford"
]
PLANS = ["Basic", "Standard", "Premium", "Business"]
CONTRACT_TYPES = ["Monthly", "12-month", "24-month"]
CALL_TYPES = ["Voice", "International", "Roaming", "Premium"]
NETWORK_TYPES = ["4G", "5G", "3G"]
TOWER_STATUS = ["Active", "Active", "Active", "Active", "Degraded", "Maintenance"]

FIRST_NAMES = [
    "James", "Oliver", "Harry", "Jack", "George", "Noah", "Charlie",
    "Jacob", "Alfie", "Freddie", "Amelia", "Olivia", "Isla", "Emily",
    "Ava", "Lily", "Sophia", "Mia", "Isabella", "Grace"
]
LAST_NAMES = [
    "Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Evans",
    "Wilson", "Thomas", "Roberts", "Johnson", "Walker", "Wright", "Robinson",
    "Thompson", "White", "Hughes", "Edwards", "Green", "Hall"
]

def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rand_phone():
    area = random.choice(UK_AREA_CODES)
    suffix_len = 11 - len(area)
    suffix = "".join([str(random.randint(0, 9)) for _ in range(suffix_len)])
    return f"{area}{suffix}"

def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


# ── 1. customers.csv ──────────────────────────────────────────────────────────
print("Generating customers.csv ...")

customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, NUM_CUSTOMERS + 1)]
customers = []

for cid in customer_ids:
    region = random.choice(REGIONS)
    tenure_months = random.randint(1, 84)          # up to 7 years
    join_date = END_DATE - timedelta(days=tenure_months * 30)
    age = random.randint(18, 75)
    monthly_charge = round(random.uniform(10, 120), 2)
    # Churn label: higher probability for short tenure + high charge
    churn_score = max(0, min(1, (1 / (tenure_months + 1)) + random.gauss(0, 0.1)))
    churned = 1 if churn_score > 0.6 else 0

    customers.append({
        "customer_id":      cid,
        "full_name":        rand_name(),
        "phone_number":     rand_phone(),
        "email":            f"user{cid[4:]}@example.co.uk",
        "region":           region,
        "age":              age,
        "plan":             random.choice(PLANS),
        "contract_type":    random.choice(CONTRACT_TYPES),
        "tenure_months":    tenure_months,
        "join_date":        join_date.strftime("%Y-%m-%d"),
        "monthly_charge":   monthly_charge,
        "is_churned":       churned,
        "data_usage_gb":    round(random.uniform(0.5, 100), 2),
        "num_complaints":   random.randint(0, 8),
    })

with open(f"{OUTPUT_DIR}/customers.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)

print(f"  -> {NUM_CUSTOMERS} rows written")


# ── 2. cdr.csv ────────────────────────────────────────────────────────────────
print("Generating cdr.csv ...")

cdr_rows = []
for i in range(NUM_CDR):
    caller = random.choice(customer_ids)
    # Receiver: 70% another customer, 30% external number
    receiver = random.choice(customer_ids) if random.random() < 0.7 else rand_phone()
    call_date = rand_date()
    duration  = max(0, int(random.gauss(180, 120)))   # seconds, mean 3 min
    call_type = random.choice(CALL_TYPES)
    tower_id  = f"TOWER{str(random.randint(1, NUM_TOWERS)).zfill(4)}"
    network   = random.choice(NETWORK_TYPES)
    # Dropped calls: ~3% overall, higher for 3G
    drop_prob = 0.08 if network == "3G" else 0.02
    dropped   = 1 if (duration == 0 or random.random() < drop_prob) else 0

    cdr_rows.append({
        "cdr_id":           f"CDR{str(i+1).zfill(8)}",
        "caller_id":        caller,
        "receiver_number":  receiver,
        "call_start_time":  call_date.strftime("%Y-%m-%d %H:%M:%S"),
        "call_duration_sec": duration,
        "call_type":        call_type,
        "tower_id":         tower_id,
        "network_type":     network,
        "is_dropped":       dropped,
        "roaming_flag":     1 if call_type == "Roaming" else 0,
        "charge_per_min":   round(random.uniform(0.01, 0.5), 4),
    })

with open(f"{OUTPUT_DIR}/cdr.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=cdr_rows[0].keys())
    writer.writeheader()
    writer.writerows(cdr_rows)

print(f"  -> {NUM_CDR} rows written")


# ── 3. billing.json ───────────────────────────────────────────────────────────
print("Generating billing.json ...")

billing_records = []
for cid in customer_ids:
    cust = next(c for c in customers if c["customer_id"] == cid)
    base_charge = cust["monthly_charge"]
    tenure = cust["tenure_months"]
    months_to_generate = min(tenure, 12)

    for m in range(1, months_to_generate + 1):
        bill_date = END_DATE.replace(day=1) - timedelta(days=30 * (months_to_generate - m))
        # Add some variance to the monthly charge
        actual_charge = round(base_charge + random.gauss(0, 5), 2)
        actual_charge = max(5.0, actual_charge)
        late_payment  = 1 if random.random() < 0.12 else 0
        data_overage  = round(random.uniform(0, 15), 2) if random.random() < 0.2 else 0.0

        billing_records.append({
            "bill_id":          f"BILL{cid[4:]}{str(m).zfill(2)}",
            "customer_id":      cid,
            "bill_month":       bill_date.strftime("%Y-%m"),
            "base_charge":      round(base_charge, 2),
            "data_overage_charge": data_overage,
            "roaming_charge":   round(random.uniform(0, 20), 2) if random.random() < 0.1 else 0.0,
            "total_charge":     round(actual_charge + data_overage, 2),
            "is_late_payment":  late_payment,
            "payment_method":   random.choice(["Direct Debit", "Credit Card", "Bank Transfer"]),
            "invoice_date":     bill_date.strftime("%Y-%m-%d"),
        })

with open(f"{OUTPUT_DIR}/billing.json", "w") as f:
    json.dump(billing_records, f, indent=2)

print(f"  -> {len(billing_records)} records written")


# ── 4. network_kpi.csv ────────────────────────────────────────────────────────
print("Generating network_kpi.csv ...")

tower_ids = [f"TOWER{str(i).zfill(4)}" for i in range(1, NUM_TOWERS + 1)]
kpi_rows  = []

for _ in range(NUM_KPI_ROWS):
    tower  = random.choice(tower_ids)
    region = random.choice(REGIONS)
    ts     = rand_date()
    status = random.choice(TOWER_STATUS)
    # Degraded towers have worse signal / higher drop rates
    is_degraded = status in ("Degraded", "Maintenance")

    kpi_rows.append({
        "kpi_id":               f"KPI{str(_+1).zfill(8)}",
        "tower_id":             tower,
        "region":               region,
        "timestamp":            ts.strftime("%Y-%m-%d %H:%M:%S"),
        "network_type":         random.choice(NETWORK_TYPES),
        "signal_strength_dbm":  round(random.gauss(-75 if not is_degraded else -95, 10), 1),
        "download_speed_mbps":  round(random.gauss(50 if not is_degraded else 10, 15), 2),
        "upload_speed_mbps":    round(random.gauss(20 if not is_degraded else 5, 5), 2),
        "latency_ms":           round(random.gauss(20 if not is_degraded else 80, 10), 1),
        "active_connections":   random.randint(0, 500),
        "dropped_call_rate_pct": round(random.uniform(0.5, 3) if not is_degraded else random.uniform(5, 20), 2),
        "tower_status":         status,
        "utilisation_pct":      round(random.uniform(10, 95), 1),
    })

with open(f"{OUTPUT_DIR}/network_kpi.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=kpi_rows[0].keys())
    writer.writeheader()
    writer.writerows(kpi_rows)

print(f"  -> {NUM_KPI_ROWS} rows written")


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n✓ All files generated in ./raw_data/")
print(f"  customers.csv   — {NUM_CUSTOMERS:,} rows")
print(f"  cdr.csv         — {NUM_CDR:,} rows")
print(f"  billing.json    — {len(billing_records):,} records")
print(f"  network_kpi.csv — {NUM_KPI_ROWS:,} rows")
print("\nNext step: upload ./raw_data/ to Azure Blob Storage (raw container)")
"""
DataBridge — Presentation-Grade Data Generator
───────────────────────────────────────────────
Generates 8,000 accounts × 75,000 transactions with realistic
banking patterns that tell a compelling analytical story.

Story arcs:
  1. Digital transformation — UPI/Online payments growing 3× faster than cash
  2. Seasonal spikes — Oct-Dec holiday surge, Jan dip, summer plateau
  3. Regional hotspots — Metro branches (NYC, LA, Chicago) dominate
  4. Growth trajectory — 20% YoY volume increase
  5. High-value customers — Pareto distribution (top 5% = 40% of volume)

Run:  python generate_presentation_data.py
"""

import os
import random
import numpy as np
import pandas as pd
import duckdb
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/DataBridge_db.duckdb")

# ── Seed for reproducibility ────────────────────────
random.seed(42)
np.random.seed(42)

# ══════════════════════════════════════════════════════
#  Reference Data — realistic names & distributions
# ══════════════════════════════════════════════════════

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna",
    "Kenneth", "Michelle", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth",
    "Jose", "Julie", "Adam", "Olivia", "Nathan", "Joyce", "Henry", "Virginia",
    "Peter", "Victoria", "Zachary", "Kelly", "Douglas", "Lauren", "Harold", "Christina",
    "Arun", "Priya", "Raj", "Anita", "Vikram", "Deepa", "Sanjay", "Meera",
    "Wei", "Mei", "Chen", "Ling", "Hiroshi", "Yuki", "Ahmed", "Fatima",
    "Carlos", "Maria", "Miguel", "Sofia", "Antonio", "Isabella", "Omar", "Leila",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales",
    "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
    "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward",
    "Richardson", "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray",
    "Mendoza", "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel",
    "Myers", "Long", "Ross", "Foster", "Jimenez", "Powell", "Jenkins", "Perry",
    "Sullivan", "Russell", "Sharma", "Gupta", "Kumar", "Singh", "Chen", "Wang",
    "Liu", "Zhang", "Li", "Yang", "Nakamura", "Tanaka", "Ali", "Hassan",
]

US_STATES = [
    "California", "Texas", "Florida", "New York", "Pennsylvania",
    "Illinois", "Ohio", "Georgia", "North Carolina", "Michigan",
    "New Jersey", "Virginia", "Washington", "Arizona", "Massachusetts",
    "Tennessee", "Indiana", "Missouri", "Maryland", "Wisconsin",
    "Colorado", "Minnesota", "South Carolina", "Alabama", "Louisiana",
    "Kentucky", "Oregon", "Oklahoma", "Connecticut", "Nevada",
]

# State weights — bigger states = more accounts (realistic distribution)
_state_raw = [
    0.12, 0.10, 0.08, 0.07, 0.05,
    0.05, 0.04, 0.04, 0.04, 0.03,
    0.03, 0.03, 0.03, 0.03, 0.03,
    0.02, 0.02, 0.02, 0.02, 0.02,
    0.02, 0.02, 0.02, 0.015, 0.015,
    0.015, 0.015, 0.015, 0.01, 0.01,
]
STATE_WEIGHTS = [w / sum(_state_raw) for w in _state_raw]

# Metro-area branches — 50 branches with city-based names
BRANCHES = [
    # Major metros (higher traffic)
    "Manhattan Downtown", "Brooklyn Heights", "Queens Central",
    "Los Angeles Main", "Beverly Hills", "Santa Monica",
    "Chicago Loop", "Chicago Northside", "Lincoln Park",
    "Houston Galleria", "Houston Downtown", "Dallas Uptown",
    "San Francisco FiDi", "San Jose Tech Park", "Oakland Lakeside",
    "Miami Beach", "Miami Downtown", "Fort Lauderdale",
    "Phoenix Central", "Scottsdale",
    # Medium metros
    "Atlanta Midtown", "Atlanta Buckhead",
    "Seattle Downtown", "Bellevue",
    "Denver LoDo", "Denver Tech Center",
    "Boston Back Bay", "Cambridge",
    "Philadelphia Center City", "King of Prussia",
    "Detroit Renaissance", "Ann Arbor",
    "Minneapolis Uptown", "St. Paul",
    "Charlotte Uptown", "Raleigh Triangle",
    "Portland Pearl District", "Portland Downtown",
    "Las Vegas Strip", "Henderson",
    # Smaller / suburban
    "Nashville Broadway", "Austin Congress",
    "San Antonio Riverwalk", "Tampa Bayshore",
    "Columbus Short North", "Indianapolis Circle",
    "Milwaukee Third Ward", "Kansas City Plaza",
    "Salt Lake City", "Hartford Downtown",
]

# Branch weights — metros get more accounts
BRANCH_WEIGHTS = np.array([
    5, 4, 3,       # NYC
    5, 3, 3,       # LA
    4, 3, 2,       # Chicago
    3, 3, 3,       # Houston/Dallas
    3, 3, 2,       # SF Bay
    3, 2, 2,       # Miami
    2, 2,          # Phoenix
    3, 2,          # Atlanta
    2, 2,          # Seattle
    2, 2,          # Denver
    2, 2,          # Boston
    2, 1,          # Philly
    1, 1,          # Detroit
    1, 1,          # Minneapolis
    2, 2,          # Charlotte/Raleigh
    1, 1,          # Portland
    2, 1,          # Vegas
    2, 2,          # Nashville/Austin
    1, 1,          # San Antonio/Tampa
    1, 1,          # Columbus/Indy
    1, 1,          # Milwaukee/KC
    1, 1,          # SLC/Hartford
], dtype=float)
BRANCH_WEIGHTS /= BRANCH_WEIGHTS.sum()

ACCOUNT_TYPES = ["Checking", "Savings", "Business Checking", "Money Market"]
ACCOUNT_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

TRANSACTION_TYPES = [
    "UPI Payment", "Online Transfer", "ATM Withdrawal", "POS Payment",
    "Cash Deposit", "Bill Payment", "Loan EMI", "Interest Credit",
    "Cheque Deposit", "Refund", "Wire Transfer", "Mobile Wallet",
]

# Amount profiles per transaction type: (mean, std, min, max)
AMOUNT_PROFILES = {
    "ATM Withdrawal":   (250, 150, 20, 2000),
    "UPI Payment":      (180, 200, 5, 5000),
    "Online Transfer":  (1500, 2000, 50, 50000),
    "POS Payment":      (120, 150, 5, 3000),
    "Cash Deposit":     (2000, 3000, 100, 50000),
    "Bill Payment":     (350, 400, 20, 5000),
    "Loan EMI":         (800, 500, 100, 10000),
    "Interest Credit":  (50, 80, 1, 500),
    "Cheque Deposit":   (3000, 5000, 100, 100000),
    "Refund":           (150, 200, 5, 5000),
    "Wire Transfer":    (5000, 8000, 500, 100000),
    "Mobile Wallet":    (80, 100, 5, 2000),
}

# ══════════════════════════════════════════════════════
#  Monthly seasonality & growth multipliers
# ══════════════════════════════════════════════════════

# Month → relative transaction volume multiplier
MONTHLY_SEASONALITY = {
    1: 0.75,   # January — post-holiday slump
    2: 0.80,   # February — recovering
    3: 0.90,   # March — tax season activity
    4: 0.95,   # April — tax refunds flowing
    5: 0.90,   # May — steady
    6: 0.85,   # June — summer lull start
    7: 0.85,   # July — summer lull
    8: 0.90,   # August — back-to-school
    9: 0.95,   # September — Q3 push
    10: 1.10,  # October — holiday shopping begins
    11: 1.25,  # November — Black Friday / holiday peak
    12: 1.30,  # December — Holiday peak
}

# Digital payment types (growing faster over time)
DIGITAL_TYPES = {"UPI Payment", "Online Transfer", "Mobile Wallet", "POS Payment"}
TRADITIONAL_TYPES = {"ATM Withdrawal", "Cash Deposit", "Cheque Deposit"}

# ══════════════════════════════════════════════════════
#  Data Generation
# ══════════════════════════════════════════════════════

NUM_CUSTOMERS = 6_500
NUM_ACCOUNTS = 8_000
NUM_TRANSACTIONS = 75_000
DATE_START = date(2025, 1, 1)
DATE_END = date(2026, 4, 12)

print(f"Generating presentation data…")
print(f"  Customers:    {NUM_CUSTOMERS:,}")
print(f"  Accounts:     {NUM_ACCOUNTS:,}")
print(f"  Transactions: {NUM_TRANSACTIONS:,}")
print(f"  Date range:   {DATE_START} to {DATE_END}")
print()

# ── Step 1: Generate Customers ───────────────────────
print("Step 1/4: Generating customers…")
used_names = set()
customer_names = []
while len(customer_names) < NUM_CUSTOMERS:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    if name not in used_names:
        used_names.add(name)
        customer_names.append(name)

customer_states = np.random.choice(US_STATES, size=NUM_CUSTOMERS, p=STATE_WEIGHTS)

customers = pd.DataFrame({
    "customer_id": range(1, NUM_CUSTOMERS + 1),
    "customer_name": customer_names,
    "customer_state": customer_states,
})

# ── Step 2: Generate Accounts ────────────────────────
print("Step 2/4: Generating accounts…")

# Each customer gets 1-3 accounts (most get 1, some get 2-3)
account_customer_ids = []
for cid in range(1, NUM_CUSTOMERS + 1):
    n_accounts = np.random.choice([1, 2, 3], p=[0.70, 0.22, 0.08])
    account_customer_ids.extend([cid] * n_accounts)

# Trim or extend to hit target
if len(account_customer_ids) > NUM_ACCOUNTS:
    account_customer_ids = account_customer_ids[:NUM_ACCOUNTS]
while len(account_customer_ids) < NUM_ACCOUNTS:
    account_customer_ids.append(random.randint(1, NUM_CUSTOMERS))

random.shuffle(account_customer_ids)

account_branches = np.random.choice(BRANCHES, size=NUM_ACCOUNTS, p=BRANCH_WEIGHTS)
account_types = np.random.choice(ACCOUNT_TYPES, size=NUM_ACCOUNTS, p=ACCOUNT_TYPE_WEIGHTS)

# Active card: 65% of accounts have an active card
has_active_card = np.random.choice([True, False], size=NUM_ACCOUNTS, p=[0.65, 0.35])

dim_df = pd.DataFrame({
    "account_sk": range(1, NUM_ACCOUNTS + 1),
    "account_id": range(1, NUM_ACCOUNTS + 1),
    "customer_id": account_customer_ids,
    "customer_name": [customers.loc[customers["customer_id"] == cid, "customer_name"].iloc[0]
                      for cid in account_customer_ids],
    "customer_state": [customers.loc[customers["customer_id"] == cid, "customer_state"].iloc[0]
                       for cid in account_customer_ids],
    "branch_name": account_branches,
    "account_type": account_types,
    "has_active_card": has_active_card,
})

print(f"  → dim_account_customer: {len(dim_df):,} rows")

# ── Step 3: Generate Transactions (story-driven) ─────
print("Step 3/4: Generating transactions with story patterns…")

# Generate dates with seasonality & growth
total_days = (DATE_END - DATE_START).days + 1
all_dates = [DATE_START + timedelta(days=d) for d in range(total_days)]

# Compute daily weights: seasonality × growth trend
daily_weights = []
for d in all_dates:
    # Seasonality
    season = MONTHLY_SEASONALITY[d.month]
    # YoY growth: ~25% growth from start to end (linear ramp)
    days_elapsed = (d - DATE_START).days
    growth = 1.0 + 0.25 * (days_elapsed / total_days)
    # Weekend boost (more retail on weekends)
    weekend_boost = 1.15 if d.weekday() >= 5 else 1.0
    daily_weights.append(season * growth * weekend_boost)

daily_weights = np.array(daily_weights)
daily_weights /= daily_weights.sum()

# Sample transaction dates
txn_dates = np.random.choice(all_dates, size=NUM_TRANSACTIONS, p=daily_weights)

# Assign accounts with Pareto-like distribution (some accounts very active)
# Use a power law: lower-numbered accounts are "older" and more active
account_activity = np.random.pareto(1.5, NUM_ACCOUNTS) + 1
account_activity /= account_activity.sum()
txn_account_sks = np.random.choice(range(1, NUM_ACCOUNTS + 1), size=NUM_TRANSACTIONS, p=account_activity)

# Assign transaction types with time-dependent shift (digital growth story)
txn_types = []
for d in txn_dates:
    days_elapsed = (d - DATE_START).days
    progress = days_elapsed / total_days  # 0.0 → 1.0

    # Digital share grows from 45% to 70% over the period
    digital_share = 0.45 + 0.25 * progress
    traditional_share = 0.25 - 0.10 * progress  # shrinks
    other_share = 1.0 - digital_share - traditional_share

    # Within each category, equal probability
    digital_list = list(DIGITAL_TYPES)
    traditional_list = list(TRADITIONAL_TYPES)
    other_list = [t for t in TRANSACTION_TYPES if t not in DIGITAL_TYPES and t not in TRADITIONAL_TYPES]

    category = np.random.choice(
        ["digital", "traditional", "other"],
        p=[digital_share, traditional_share, other_share]
    )

    if category == "digital":
        txn_types.append(random.choice(digital_list))
    elif category == "traditional":
        txn_types.append(random.choice(traditional_list))
    else:
        txn_types.append(random.choice(other_list))

# Generate amounts based on transaction type
txn_amounts = []
for ttype in txn_types:
    mean, std, mn, mx = AMOUNT_PROFILES[ttype]
    amount = max(mn, min(mx, np.random.lognormal(
        np.log(mean) - 0.5 * (std / mean) ** 2,
        std / mean * 0.8
    )))
    txn_amounts.append(round(amount, 2))

fact_df = pd.DataFrame({
    "transaction_id": range(1, NUM_TRANSACTIONS + 1),
    "account_sk": txn_account_sks,
    "transaction_date": txn_dates,
    "transaction_type": txn_types,
    "amount": txn_amounts,
})

# Sort by date for clean time series
fact_df = fact_df.sort_values("transaction_date").reset_index(drop=True)
fact_df["transaction_id"] = range(1, NUM_TRANSACTIONS + 1)

print(f"  → fact_transactions: {len(fact_df):,} rows")

# ── Step 4: Load into DuckDB ─────────────────────────
print("Step 4/4: Loading into DuckDB…")

os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)
conn = duckdb.connect(DUCKDB_PATH)

conn.execute("DROP TABLE IF EXISTS dim_account_customer")
conn.execute("CREATE TABLE dim_account_customer AS SELECT * FROM dim_df")

conn.execute("DROP TABLE IF EXISTS fact_transactions")
conn.execute("CREATE TABLE fact_transactions AS SELECT * FROM fact_df")

# ── Verify ───────────────────────────────────────────
dim_count = conn.execute("SELECT COUNT(*) FROM dim_account_customer").fetchone()[0]
fact_count = conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
total_value = conn.execute("SELECT SUM(amount) FROM fact_transactions").fetchone()[0]
date_range = conn.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM fact_transactions").fetchall()[0]

print()
print("═" * 55)
print("  DATA GENERATION COMPLETE")
print("═" * 55)
print(f"  Dimension rows:     {dim_count:>10,}")
print(f"  Fact rows:          {fact_count:>10,}")
print(f"  Total txn value:    ${total_value:>14,.2f}")
print(f"  Date range:         {date_range[0]} → {date_range[1]}")
print()

# Story summary stats
print("  STORY HIGHLIGHTS:")
type_summary = conn.execute("""
    SELECT transaction_type, COUNT(*) as cnt,
           ROUND(SUM(amount), 2) as total,
           ROUND(AVG(amount), 2) as avg_amt
    FROM fact_transactions
    GROUP BY 1 ORDER BY 2 DESC
""").fetchdf()
print(type_summary.to_string(index=False))
print()

state_summary = conn.execute("""
    SELECT d.customer_state, COUNT(*) as txn_count,
           ROUND(SUM(f.amount), 2) as total_volume
    FROM fact_transactions f
    JOIN dim_account_customer d ON f.account_sk = d.account_sk
    GROUP BY 1 ORDER BY 3 DESC LIMIT 10
""").fetchdf()
print("  TOP 10 STATES BY VOLUME:")
print(state_summary.to_string(index=False))
print()

monthly = conn.execute("""
    SELECT EXTRACT(YEAR FROM transaction_date) as yr,
           EXTRACT(MONTH FROM transaction_date) as mo,
           COUNT(*) as txns,
           ROUND(SUM(amount), 2) as volume
    FROM fact_transactions
    GROUP BY 1, 2 ORDER BY 1, 2
""").fetchdf()
print("  MONTHLY TREND:")
print(monthly.to_string(index=False))

conn.close()
print()
print(f"✅ Data written to: {DUCKDB_PATH}")

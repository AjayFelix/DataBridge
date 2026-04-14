"""
DataBridge — PostgreSQL Source Data Generator (India / Tamil Nadu Focus)
───────────────────────────────────────────────────────────────────────
Populates the 6 OLTP source tables in PostgreSQL with 75K+ records
set in India — Tamil Nadu dominant, with authentic Indian names,
cities, branches, and INR amounts.

Source tables populated:
  1. transaction_types  (12 rows)
  2. branches           (50 rows)
  3. customers          (6,500 rows)
  4. accounts           (8,000 rows)
  5. cards              (10,000 rows)
  6. transactions       (75,000 rows)

Story arcs:
  - Digital India: UPI/Online payments exploding 3× vs cash
  - Festival spikes: Diwali/Pongal/Dussehra surge
  - Tamil Nadu leads: Chennai, Coimbatore, Madurai dominate
  - Growth trajectory: 25% YoY volume increase
  - Pareto: Top 5% customers = 40% volume

Run:  python generate_source_data.py
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "DB_SRC_OLTP")
PG_USER = os.getenv("PG_USER", "AJAYFELIX")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA = os.getenv("PG_SCHEMA", "DBANK_SRC")

url = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)
engine = create_engine(url, pool_pre_ping=True)

random.seed(42)
np.random.seed(42)

# ══════════════════════════════════════════════════════
#  Indian Names — Tamil-heavy mix
# ══════════════════════════════════════════════════════

TAMIL_FIRST_NAMES = [
    "Arun", "Bala", "Chandran", "Dinesh", "Ezhil", "Ganesh", "Hari",
    "Inba", "Jagan", "Karthik", "Lokesh", "Mani", "Naveen", "Oviya",
    "Prabhu", "Raghu", "Senthil", "Tamilselvan", "Udhay", "Vignesh",
    "Anand", "Bharathi", "Deepa", "Elango", "Gokul", "Harini",
    "Ilango", "Jayalakshmi", "Kavitha", "Lakshmi", "Meena", "Nithya",
    "Priya", "Revathi", "Saranya", "Thenmozhi", "Uma", "Vasuki",
    "Yuvarani", "Selvam", "Murugan", "Saravanan", "Kumaran", "Devi",
    "Arul", "Balamurugan", "Chitra", "Divya", "Eswari", "Geetha",
    "Hemavathi", "Indira", "Janani", "Kalai", "Lavanya", "Malar",
    "Nandhini", "Padma", "Radha", "Sangeetha", "Tharani", "Vasugi",
    "Anbu", "Boopathi", "Chelladurai", "Dhanush", "Elangovan",
    "Gowtham", "Haripriya", "Ilavarasi", "Jeeva", "Kannan",
    "Logesh", "Manikandan", "Nandha", "Parthiban", "Ranjith",
    "Surya", "Thangam", "Velmurugan", "Yamini", "Aravind",
    "Barathi", "Chandra", "Dhanalakshmi", "Ezhilarasi", "Guruvammal",
    "Hemalatha", "Iswarya", "Jeyanthi", "Karpagam", "Latha",
]

INDIAN_FIRST_NAMES = [
    "Rajesh", "Suresh", "Mahesh", "Ramesh", "Vikram", "Sanjay",
    "Amit", "Rahul", "Ajay", "Vijay", "Ravi", "Mohan", "Kumar",
    "Prakash", "Venkat", "Ashok", "Manoj", "Anil", "Sunil", "Vinod",
    "Priya", "Anita", "Sunita", "Rekha", "Pooja", "Neha", "Shreya",
    "Swati", "Rupa", "Meera", "Anjali", "Deepika", "Sneha", "Pallavi",
    "Rohit", "Sachin", "Nikhil", "Akash", "Arjun", "Varun",
    "Siddharth", "Aditya", "Rohan", "Pranav", "Ishaan", "Vivek",
    "Nisha", "Kavya", "Tanvi", "Aditi", "Isha", "Ritika", "Shruti",
    "Megha", "Bhavya", "Rashmi", "Sonam", "Komal", "Manisha", "Jyoti",
]

# Mix: ~60% Tamil names, ~40% pan-Indian
FIRST_NAMES = TAMIL_FIRST_NAMES + INDIAN_FIRST_NAMES

TAMIL_LAST_NAMES = [
    "Murugan", "Selvam", "Pandian", "Rajan", "Krishnan", "Subramanian",
    "Natarajan", "Govindaraj", "Shanmugam", "Palani", "Sundaram",
    "Ramasamy", "Velayutham", "Arumugam", "Balasubramanian",
    "Chidambaram", "Durai", "Elangovan", "Ganapathy", "Hariharan",
    "Iyappan", "Jayaram", "Kalyanasundaram", "Lakshmanan", "Manickam",
    "Nagarajan", "Perumal", "Rajagopal", "Srinivasan", "Thirunavukkarasu",
    "Venkatachalam", "Vaithiyanathan", "Annamalai", "Muthusamy",
    "Kandasamy", "Rangasamy", "Soundararajan", "Sekar", "Thangaraj",
    "Veerasamy", "Chellamuthu", "Devaraj", "Ganesan", "Kathiresan",
    "Mariappan", "Palanisamy", "Sakthivel", "Thiagarajan", "Velusamy",
    "Alagappan",
]

INDIAN_LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Singh", "Kumar", "Patel", "Shah",
    "Mehta", "Joshi", "Rao", "Reddy", "Nair", "Menon", "Pillai",
    "Iyer", "Iyengar", "Choudhary", "Mishra", "Pandey", "Tiwari",
    "Saxena", "Agarwal", "Banerjee", "Chatterjee", "Das", "Bose",
    "Sen", "Roy", "Ghosh", "Mukherjee", "Kapoor", "Malhotra",
    "Khanna", "Chopra", "Bhatia", "Ahuja", "Sethi", "Grover",
    "Bajaj", "Kohli",
]

LAST_NAMES = TAMIL_LAST_NAMES + INDIAN_LAST_NAMES

# ══════════════════════════════════════════════════════
#  Indian States — Tamil Nadu dominant
# ══════════════════════════════════════════════════════

STATES = [
    "Tamil Nadu", "Karnataka", "Maharashtra", "Kerala", "Andhra Pradesh",
    "Telangana", "Delhi", "Uttar Pradesh", "Gujarat", "Rajasthan",
    "West Bengal", "Madhya Pradesh", "Punjab", "Haryana", "Bihar",
    "Odisha", "Jharkhand", "Assam", "Chhattisgarh", "Uttarakhand",
    "Goa", "Himachal Pradesh", "Puducherry", "Chandigarh", "Jammu & Kashmir",
]

_state_raw = [
    0.25, 0.10, 0.10, 0.08, 0.06,   # TN, KA, MH, KL, AP
    0.05, 0.05, 0.04, 0.04, 0.03,   # TS, DL, UP, GJ, RJ
    0.03, 0.02, 0.02, 0.02, 0.02,   # WB, MP, PB, HR, BR
    0.015, 0.01, 0.01, 0.01, 0.01,  # OD, JH, AS, CG, UK
    0.01, 0.01, 0.01, 0.005, 0.005, # GA, HP, PY, CH, JK
]
STATE_WEIGHTS = [w / sum(_state_raw) for w in _state_raw]

# ══════════════════════════════════════════════════════
#  Branches — Tamil Nadu heavy, pan-India mix
# ══════════════════════════════════════════════════════

# (branch_name, city, state)
BRANCHES = [
    # ── Tamil Nadu (20 branches) ─────────────────────
    ("Chennai Anna Nagar", "Chennai", "Tamil Nadu"),
    ("Chennai T. Nagar", "Chennai", "Tamil Nadu"),
    ("Chennai Adyar", "Chennai", "Tamil Nadu"),
    ("Chennai Velachery", "Chennai", "Tamil Nadu"),
    ("Chennai Tambaram", "Chennai", "Tamil Nadu"),
    ("Chennai OMR Sholinganallur", "Chennai", "Tamil Nadu"),
    ("Chennai Porur", "Chennai", "Tamil Nadu"),
    ("Coimbatore RS Puram", "Coimbatore", "Tamil Nadu"),
    ("Coimbatore Gandhipuram", "Coimbatore", "Tamil Nadu"),
    ("Coimbatore Peelamedu", "Coimbatore", "Tamil Nadu"),
    ("Madurai Meenakshi", "Madurai", "Tamil Nadu"),
    ("Madurai Anna Nagar", "Madurai", "Tamil Nadu"),
    ("Trichy Cantonment", "Tiruchirappalli", "Tamil Nadu"),
    ("Trichy Thillai Nagar", "Tiruchirappalli", "Tamil Nadu"),
    ("Salem Junction", "Salem", "Tamil Nadu"),
    ("Tirunelveli Town", "Tirunelveli", "Tamil Nadu"),
    ("Erode Perundurai", "Erode", "Tamil Nadu"),
    ("Vellore Fort", "Vellore", "Tamil Nadu"),
    ("Thanjavur Palace", "Thanjavur", "Tamil Nadu"),
    ("Tiruppur Avinashi Road", "Tiruppur", "Tamil Nadu"),
    # ── Karnataka (5) ────────────────────────────────
    ("Bengaluru MG Road", "Bengaluru", "Karnataka"),
    ("Bengaluru Whitefield", "Bengaluru", "Karnataka"),
    ("Bengaluru Koramangala", "Bengaluru", "Karnataka"),
    ("Mysuru Sayyaji Rao Road", "Mysuru", "Karnataka"),
    ("Mangaluru Hampankatta", "Mangaluru", "Karnataka"),
    # ── Maharashtra (5) ──────────────────────────────
    ("Mumbai Fort", "Mumbai", "Maharashtra"),
    ("Mumbai Andheri", "Mumbai", "Maharashtra"),
    ("Mumbai Bandra", "Mumbai", "Maharashtra"),
    ("Pune FC Road", "Pune", "Maharashtra"),
    ("Pune Hinjewadi", "Pune", "Maharashtra"),
    # ── Kerala (3) ───────────────────────────────────
    ("Kochi MG Road", "Kochi", "Kerala"),
    ("Thiruvananthapuram Statue", "Thiruvananthapuram", "Kerala"),
    ("Kozhikode Mavoor Road", "Kozhikode", "Kerala"),
    # ── Andhra Pradesh / Telangana (4) ───────────────
    ("Hyderabad Ameerpet", "Hyderabad", "Telangana"),
    ("Hyderabad HITEC City", "Hyderabad", "Telangana"),
    ("Visakhapatnam Beach Road", "Visakhapatnam", "Andhra Pradesh"),
    ("Vijayawada MG Road", "Vijayawada", "Andhra Pradesh"),
    # ── North India (8) ──────────────────────────────
    ("Delhi Connaught Place", "New Delhi", "Delhi"),
    ("Delhi Dwarka", "New Delhi", "Delhi"),
    ("Gurugram Cyber City", "Gurugram", "Haryana"),
    ("Noida Sector 18", "Noida", "Uttar Pradesh"),
    ("Lucknow Hazratganj", "Lucknow", "Uttar Pradesh"),
    ("Jaipur MI Road", "Jaipur", "Rajasthan"),
    ("Ahmedabad CG Road", "Ahmedabad", "Gujarat"),
    ("Chandigarh Sector 17", "Chandigarh", "Chandigarh"),
    # ── East India (3) ───────────────────────────────
    ("Kolkata Park Street", "Kolkata", "West Bengal"),
    ("Bhubaneswar Janpath", "Bhubaneswar", "Odisha"),
    ("Patna Boring Road", "Patna", "Bihar"),
    # ── Others (2) ───────────────────────────────────
    ("Panaji Panjim", "Panaji", "Goa"),
    ("Puducherry MG Road", "Puducherry", "Puducherry"),
]

# Weights — Tamil Nadu branches get the lion's share
BRANCH_WEIGHTS = np.array([
    # Tamil Nadu (20 branches, heavy)
    8, 7, 6, 5, 4, 5, 4,         # Chennai (7)
    5, 4, 3,                       # Coimbatore (3)
    4, 3,                          # Madurai (2)
    3, 2,                          # Trichy (2)
    2, 2, 2, 2, 2, 3,             # Salem, Tirunelveli, Erode, Vellore, Thanjavur, Tiruppur
    # Karnataka (5)
    4, 3, 3, 2, 1,
    # Maharashtra (5)
    4, 3, 3, 2, 2,
    # Kerala (3)
    2, 2, 1,
    # AP/TS (4)
    3, 3, 2, 1,
    # North (8)
    3, 2, 2, 2, 1, 1, 2, 1,
    # East (3)
    2, 1, 1,
    # Others (2)
    1, 1,
], dtype=float)
BRANCH_WEIGHTS /= BRANCH_WEIGHTS.sum()

ACCOUNT_TYPES = ["Savings", "Current", "Salary Account", "Fixed Deposit"]
ACCOUNT_TYPE_WEIGHTS = [0.45, 0.25, 0.20, 0.10]

# ══════════════════════════════════════════════════════
#  Transaction Types — Indian banking context
# ══════════════════════════════════════════════════════

TRANSACTION_TYPES_DATA = [
    (1, "ATM Withdrawal", "Cash withdrawal from ATM"),
    (2, "NEFT Transfer", "National Electronic Funds Transfer"),
    (3, "POS Payment", "Point of Sale card swipe"),
    (4, "UPI Payment", "Unified Payments Interface (GPay/PhonePe/Paytm)"),
    (5, "Cash Deposit", "Over-the-counter cash deposit"),
    (6, "Cheque Deposit", "Cheque clearing deposit"),
    (7, "Bill Payment", "Utility and bill payments (EB/Water/Gas)"),
    (8, "Interest Credit", "Quarterly interest credit"),
    (9, "Loan EMI", "Equated monthly installment"),
    (10, "Refund", "Merchant or bank refund"),
    (11, "RTGS Transfer", "Real Time Gross Settlement (high value)"),
    (12, "IMPS Transfer", "Immediate Payment Service"),
]

# type_id → (mean, std, min, max) in INR
AMOUNT_PROFILES = {
    1:  (3000, 2000, 100, 25000),         # ATM Withdrawal
    2:  (25000, 30000, 500, 500000),       # NEFT Transfer
    3:  (1500, 2000, 50, 50000),           # POS Payment
    4:  (500, 800, 10, 25000),             # UPI Payment
    5:  (15000, 20000, 500, 500000),       # Cash Deposit
    6:  (50000, 80000, 1000, 1000000),     # Cheque Deposit
    7:  (2500, 3000, 100, 25000),          # Bill Payment
    8:  (800, 1500, 10, 15000),            # Interest Credit
    9:  (12000, 8000, 1000, 100000),       # Loan EMI
    10: (1200, 2000, 50, 25000),           # Refund
    11: (200000, 300000, 200000, 5000000), # RTGS Transfer (min ₹2L)
    12: (5000, 8000, 100, 200000),         # IMPS Transfer
}

# Indian banking seasonality — festivals matter
MONTHLY_SEASONALITY = {
    1:  1.10,  # January — Pongal (TN), New Year spending
    2:  0.85,  # February — post-festival lull
    3:  0.90,  # March — FY end, tax savings rush
    4:  0.80,  # April — new FY start, slower
    5:  0.85,  # May — summer, school admissions
    6:  0.85,  # June — monsoon start
    7:  0.80,  # July — monsoon lull
    8:  0.90,  # August — Independence Day, Raksha Bandhan
    9:  1.00,  # September — Ganesh Chaturthi, Onam
    10: 1.20,  # October — Navaratri, Dussehra
    11: 1.35,  # November — Diwali, Karthigai Deepam
    12: 1.05,  # December — Christmas, year-end
}

# Digital India story
DIGITAL_TYPE_IDS = {2, 3, 4, 12}       # NEFT, POS, UPI, IMPS
TRADITIONAL_TYPE_IDS = {1, 5, 6}        # ATM, Cash Deposit, Cheque
OTHER_TYPE_IDS = {7, 8, 9, 10, 11}      # Bill, Interest, EMI, Refund, RTGS

# ══════════════════════════════════════════════════════
#  Configuration
# ══════════════════════════════════════════════════════

NUM_CUSTOMERS = 6_500
NUM_ACCOUNTS = 8_000
NUM_CARDS = 10_000
NUM_TRANSACTIONS = 75_000
DATE_START = date(2025, 1, 1)
DATE_END = date(2026, 4, 12)

KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED"]
KYC_WEIGHTS = [0.75, 0.20, 0.05]

ACCOUNT_STATUSES = ["ACTIVE", "INACTIVE", "DORMANT", "CLOSED"]
ACCOUNT_STATUS_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

CARD_TYPES = ["Debit", "Credit", "RuPay"]
CARD_TYPE_WEIGHTS = [0.45, 0.30, 0.25]

TXN_STATUSES = ["SUCCESS", "FAILED", "PENDING"]
TXN_STATUS_WEIGHTS = [0.88, 0.07, 0.05]

print("=" * 60)
print("  DataBridge — India / Tamil Nadu Source Data Generator")
print("=" * 60)
print(f"  Target DB:      {PG_DATABASE} (schema: {PG_SCHEMA})")
print(f"  Customers:      {NUM_CUSTOMERS:,}")
print(f"  Accounts:       {NUM_ACCOUNTS:,}")
print(f"  Cards:          {NUM_CARDS:,}")
print(f"  Transactions:   {NUM_TRANSACTIONS:,}")
print(f"  Date range:     {DATE_START} → {DATE_END}")
print(f"  Currency:       INR (₹)")
print(f"  Focus region:   Tamil Nadu (25% weight)")
print("=" * 60)
print()

# ══════════════════════════════════════════════════════
#  Step 1: Transaction Types (12 rows)
# ══════════════════════════════════════════════════════
print("Step 1/6: Loading transaction_types…")

with engine.begin() as conn:
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."transactions"'))
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."cards"'))
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."accounts"'))
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."customers"'))
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."branches"'))
    conn.execute(text(f'DELETE FROM "{PG_SCHEMA}"."transaction_types"'))

now = datetime.now()

txn_types_rows = []
for type_id, type_name, description in TRANSACTION_TYPES_DATA:
    txn_types_rows.append({
        "type_id": type_id,
        "type_name": type_name,
        "description": description,
        "created_at": now,
    })

txn_types_df = pd.DataFrame(txn_types_rows)
txn_types_df.to_sql("transaction_types", engine, schema=PG_SCHEMA, if_exists="append", index=False)
print(f"  → transaction_types: {len(txn_types_df)} rows")

# ══════════════════════════════════════════════════════
#  Step 2: Branches (50 rows)
# ══════════════════════════════════════════════════════
print("Step 2/6: Loading branches…")

branch_rows = []
for bid, (bname, city, state) in enumerate(BRANCHES, start=1):
    branch_rows.append({
        "branch_id": bid,
        "branch_name": bname,
        "city": city,
        "state": state,
        "country": "India",
        "created_at": now,
    })

branches_df = pd.DataFrame(branch_rows)
branches_df.to_sql("branches", engine, schema=PG_SCHEMA, if_exists="append", index=False)
print(f"  → branches: {len(branches_df)} rows")

# ══════════════════════════════════════════════════════
#  Step 3: Customers (6,500 rows)
# ══════════════════════════════════════════════════════
print("Step 3/6: Generating customers…")

used_names = set()
customer_names = []
while len(customer_names) < NUM_CUSTOMERS:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    if name not in used_names:
        used_names.add(name)
        customer_names.append(name)

customer_states = np.random.choice(STATES, size=NUM_CUSTOMERS, p=STATE_WEIGHTS)

# Spread onboarding across timeline
onboarding_dates = []
total_days = (DATE_END - DATE_START).days
for _ in range(NUM_CUSTOMERS):
    days_offset = int(np.random.beta(2, 5) * total_days)
    onboarding_dates.append(DATE_START + timedelta(days=days_offset))

kyc_statuses = np.random.choice(KYC_STATUSES, size=NUM_CUSTOMERS, p=KYC_WEIGHTS)

customer_rows = []
for i in range(NUM_CUSTOMERS):
    cid = i + 1
    name = customer_names[i]
    email_name = name.lower().replace(" ", ".") + f"{cid}@mail.com"
    phone = f"{random.choice(['9', '8', '7', '6'])}{random.randint(100000000, 999999999)}"
    customer_rows.append({
        "customer_id": cid,
        "full_name": name,
        "email": email_name,
        "phone": phone,
        "state": customer_states[i],
        "onboarding_date": onboarding_dates[i],
        "kyc_status": kyc_statuses[i],
        "created_at": datetime.combine(onboarding_dates[i], datetime.min.time()) + timedelta(hours=random.randint(8, 18)),
    })

customers_df = pd.DataFrame(customer_rows)
customers_df.to_sql("customers", engine, schema=PG_SCHEMA, if_exists="append", index=False, method="multi", chunksize=1000)
print(f"  → customers: {len(customers_df)} rows")

# ══════════════════════════════════════════════════════
#  Step 4: Accounts (8,000 rows)
# ══════════════════════════════════════════════════════
print("Step 4/6: Generating accounts…")

account_customer_ids = []
for cid in range(1, NUM_CUSTOMERS + 1):
    n_accounts = np.random.choice([1, 2, 3], p=[0.70, 0.22, 0.08])
    account_customer_ids.extend([cid] * n_accounts)

if len(account_customer_ids) > NUM_ACCOUNTS:
    account_customer_ids = account_customer_ids[:NUM_ACCOUNTS]
while len(account_customer_ids) < NUM_ACCOUNTS:
    account_customer_ids.append(random.randint(1, NUM_CUSTOMERS))

random.shuffle(account_customer_ids)

account_branch_ids = np.random.choice(range(1, len(BRANCHES) + 1), size=NUM_ACCOUNTS, p=BRANCH_WEIGHTS)
account_types = np.random.choice(ACCOUNT_TYPES, size=NUM_ACCOUNTS, p=ACCOUNT_TYPE_WEIGHTS)
account_statuses = np.random.choice(ACCOUNT_STATUSES, size=NUM_ACCOUNTS, p=ACCOUNT_STATUS_WEIGHTS)

account_rows = []
for i in range(NUM_ACCOUNTS):
    aid = i + 1
    cid = account_customer_ids[i]
    cust_onboard = onboarding_dates[cid - 1]
    days_after = random.randint(0, 30)
    opened = min(cust_onboard + timedelta(days=days_after), DATE_END)
    # Balance in INR (₹500 to ₹25,00,000)
    balance = round(random.uniform(500, 2500000), 2)
    account_rows.append({
        "account_id": aid,
        "customer_id": cid,
        "branch_id": int(account_branch_ids[i]),
        "account_type": account_types[i],
        "balance": balance,
        "currency": "INR",
        "status": account_statuses[i],
        "opened_date": opened,
        "created_at": datetime.combine(opened, datetime.min.time()) + timedelta(hours=random.randint(8, 18)),
    })

accounts_df = pd.DataFrame(account_rows)
accounts_df.to_sql("accounts", engine, schema=PG_SCHEMA, if_exists="append", index=False, method="multi", chunksize=1000)
print(f"  → accounts: {len(accounts_df)} rows")

# ══════════════════════════════════════════════════════
#  Step 5: Cards (10,000 rows)
# ══════════════════════════════════════════════════════
print("Step 5/6: Generating cards…")

card_account_ids = []
for aid in range(1, NUM_ACCOUNTS + 1):
    n_cards = np.random.choice([0, 1, 2], p=[0.15, 0.55, 0.30])
    card_account_ids.extend([aid] * n_cards)

if len(card_account_ids) > NUM_CARDS:
    card_account_ids = card_account_ids[:NUM_CARDS]
while len(card_account_ids) < NUM_CARDS:
    card_account_ids.append(random.randint(1, NUM_ACCOUNTS))

random.shuffle(card_account_ids)

card_types = np.random.choice(CARD_TYPES, size=NUM_CARDS, p=CARD_TYPE_WEIGHTS)

card_rows = []
for i in range(NUM_CARDS):
    card_id = i + 1
    card_num = f"4{random.randint(100, 999)}-XXXX-XXXX-{card_id:04d}"
    expiry = DATE_END + timedelta(days=random.randint(30, 1460))
    is_active = random.random() < 0.65
    card_rows.append({
        "card_id": card_id,
        "account_id": card_account_ids[i],
        "card_number": card_num,
        "card_type": card_types[i],
        "expiry_date": expiry,
        "is_active": is_active,
        "created_at": now,
    })

cards_df = pd.DataFrame(card_rows)
cards_df.to_sql("cards", engine, schema=PG_SCHEMA, if_exists="append", index=False, method="multi", chunksize=1000)
print(f"  → cards: {len(cards_df)} rows")

# ══════════════════════════════════════════════════════
#  Step 6: Transactions (75,000 rows) — THE STORY
# ══════════════════════════════════════════════════════
print("Step 6/6: Generating 75K transactions (Digital India story)…")

total_days_range = (DATE_END - DATE_START).days + 1
all_dates = [DATE_START + timedelta(days=d) for d in range(total_days_range)]

# Daily weights: seasonality × growth × weekend
daily_weights = []
for d in all_dates:
    season = MONTHLY_SEASONALITY[d.month]
    days_elapsed = (d - DATE_START).days
    growth = 1.0 + 0.25 * (days_elapsed / total_days_range)
    weekend_boost = 1.15 if d.weekday() >= 5 else 1.0
    daily_weights.append(season * growth * weekend_boost)

daily_weights = np.array(daily_weights)
daily_weights /= daily_weights.sum()

txn_dates = np.random.choice(all_dates, size=NUM_TRANSACTIONS, p=daily_weights)

# Pareto account activity
account_activity = np.random.pareto(1.5, NUM_ACCOUNTS) + 1
account_activity /= account_activity.sum()
txn_account_ids = np.random.choice(range(1, NUM_ACCOUNTS + 1), size=NUM_TRANSACTIONS, p=account_activity)

# Transaction types — Digital India growth story
digital_list = list(DIGITAL_TYPE_IDS)
traditional_list = list(TRADITIONAL_TYPE_IDS)
other_list = list(OTHER_TYPE_IDS)

txn_type_ids = []
for d in txn_dates:
    days_elapsed = (d - DATE_START).days
    progress = days_elapsed / total_days_range

    # UPI/digital explodes from 40% to 72% (Digital India push)
    digital_share = 0.40 + 0.32 * progress
    traditional_share = 0.30 - 0.15 * progress
    other_share = 1.0 - digital_share - traditional_share

    category = np.random.choice(
        ["digital", "traditional", "other"],
        p=[digital_share, traditional_share, other_share]
    )
    if category == "digital":
        txn_type_ids.append(random.choice(digital_list))
    elif category == "traditional":
        txn_type_ids.append(random.choice(traditional_list))
    else:
        txn_type_ids.append(random.choice(other_list))

# Amounts in INR
txn_amounts = []
for tid in txn_type_ids:
    mean, std, mn, mx = AMOUNT_PROFILES[tid]
    amount = max(mn, min(mx, np.random.lognormal(
        np.log(mean) - 0.5 * (std / mean) ** 2,
        std / mean * 0.8
    )))
    txn_amounts.append(round(amount, 2))

txn_statuses = np.random.choice(TXN_STATUSES, size=NUM_TRANSACTIONS, p=TXN_STATUS_WEIGHTS)

print("  Building transaction records…")
txn_rows = []
for i in range(NUM_TRANSACTIONS):
    tid = i + 1
    txn_date = txn_dates[i]
    hour = random.randint(6, 23)  # Indian banking hours
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ts = datetime(txn_date.year, txn_date.month, txn_date.day, hour, minute, second)
    ref_id = f"TXN{tid:07d}_{random.randint(1000, 9999)}"

    txn_rows.append({
        "transaction_id": tid,
        "account_id": int(txn_account_ids[i]),
        "type_id": int(txn_type_ids[i]),
        "amount": txn_amounts[i],
        "txn_timestamp": ts,
        "status": txn_statuses[i],
        "reference_id": ref_id,
        "created_at": ts + timedelta(seconds=random.randint(1, 60)),
    })

txn_rows.sort(key=lambda r: r["txn_timestamp"])
for i, row in enumerate(txn_rows):
    row["transaction_id"] = i + 1

print("  Inserting into PostgreSQL (in chunks)…")
transactions_df = pd.DataFrame(txn_rows)
transactions_df.to_sql(
    "transactions", engine, schema=PG_SCHEMA,
    if_exists="append", index=False, method="multi", chunksize=2000
)
print(f"  → transactions: {len(transactions_df)} rows")

# ══════════════════════════════════════════════════════
#  Verification
# ══════════════════════════════════════════════════════
print()
print("═" * 65)
print("  VERIFICATION — PostgreSQL Source Data (India / Tamil Nadu)")
print("═" * 65)

with engine.connect() as conn:
    for tbl in ["transaction_types", "branches", "customers", "accounts", "cards", "transactions"]:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{PG_SCHEMA}"."{tbl}"')).fetchone()[0]
        print(f"  {tbl:25s} → {count:>10,} rows")

    print()

    # Transaction types
    result = conn.execute(text(f'''
        SELECT tt.type_name, COUNT(*) as cnt, ROUND(SUM(t.amount)::numeric, 2) as total
        FROM "{PG_SCHEMA}".transactions t
        JOIN "{PG_SCHEMA}".transaction_types tt ON t.type_id = tt.type_id
        GROUP BY tt.type_name ORDER BY cnt DESC
    ''')).fetchall()
    print(f"  {'Transaction Type':<20s} {'Count':>8s} {'Total (₹)':>18s}")
    print(f"  {'─'*20} {'─'*8} {'─'*18}")
    for row in result:
        print(f"  {row[0]:<20s} {row[1]:>8,} ₹{row[2]:>16,.2f}")

    # Top states
    result = conn.execute(text(f'''
        SELECT c.state, COUNT(*) as txn_count, ROUND(SUM(t.amount)::numeric, 2) as volume
        FROM "{PG_SCHEMA}".transactions t
        JOIN "{PG_SCHEMA}".accounts a ON t.account_id = a.account_id
        JOIN "{PG_SCHEMA}".customers c ON a.customer_id = c.customer_id
        GROUP BY c.state ORDER BY volume DESC LIMIT 8
    ''')).fetchall()
    print(f"\n  TOP STATES BY VOLUME:")
    print(f"  {'State':<20s} {'Txns':>8s} {'Volume (₹)':>18s}")
    print(f"  {'─'*20} {'─'*8} {'─'*18}")
    for row in result:
        print(f"  {row[0]:<20s} {row[1]:>8,} ₹{row[2]:>16,.2f}")

    # TN branch breakdown
    result = conn.execute(text(f'''
        SELECT b.branch_name, COUNT(*) as txn_count, ROUND(SUM(t.amount)::numeric, 2) as volume
        FROM "{PG_SCHEMA}".transactions t
        JOIN "{PG_SCHEMA}".accounts a ON t.account_id = a.account_id
        JOIN "{PG_SCHEMA}".branches b ON a.branch_id = b.branch_id
        WHERE b.state = 'Tamil Nadu'
        GROUP BY b.branch_name ORDER BY volume DESC LIMIT 10
    ''')).fetchall()
    print(f"\n  TOP TAMIL NADU BRANCHES:")
    print(f"  {'Branch':<30s} {'Txns':>8s} {'Volume (₹)':>18s}")
    print(f"  {'─'*30} {'─'*8} {'─'*18}")
    for row in result:
        print(f"  {row[0]:<30s} {row[1]:>8,} ₹{row[2]:>16,.2f}")

print()
print("═" * 65)
print("  ✅ Source data loaded! Run ETL pipeline next.")
print("═" * 65)

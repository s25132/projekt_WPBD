import os
from datetime import datetime, UTC
from random import randint, uniform
from faker import Faker
from sqlalchemy import create_engine, text

# --- konfiguracja połączenia ---
host = os.getenv("PGHOST", "db")
port = os.getenv("PGPORT", "5432")
db   = os.getenv("PGDATABASE", "appdb")
user = os.getenv("PGUSER", "app")
pw   = os.getenv("PGPASSWORD", "app")
rows = int(os.getenv("ROWS", "200"))  # liczba symulowanych nowych użytkowników

url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
engine = create_engine(url, pool_pre_ping=True)
fake = Faker()

# --- definicje tabel ---
ddl_users = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

ddl_orders = """
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  product_name TEXT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

# --- seeding danych ---
with engine.begin() as conn:
    # utworzenie tabel, jeśli nie istnieją
    conn.execute(text(ddl_users))
    conn.execute(text(ddl_orders))

    # generowanie użytkowników
    users_data = [{
        "full_name": fake.name(),
        "email": fake.unique.email(),
        "created_at": datetime.now(UTC)
    } for _ in range(rows)]

    # wstawianie (z unikaniem duplikatów)
    result = conn.execute(
        text("""
            INSERT INTO users (full_name, email, created_at)
            VALUES (:full_name, :email, :created_at)
            ON CONFLICT (email) DO NOTHING
            RETURNING id
        """),
        users_data
    )

    # pobieramy wszystkie id użytkowników (starych i nowych)
    all_users = conn.execute(text("SELECT id FROM users")).fetchall()
    user_ids = [r[0] for r in all_users]

    # generowanie zamówień
    orders_data = []
    for uid in user_ids:
        for _ in range(randint(1, 3)):  # 1–3 zamówienia per user
            orders_data.append({
                "user_id": uid,
                "product_name": fake.word().capitalize(),
                "amount": round(uniform(10, 500), 2),
                "created_at": datetime.now(UTC)
            })

    conn.execute(
        text("""
            INSERT INTO orders (user_id, product_name, amount, created_at)
            VALUES (:user_id, :product_name, :amount, :created_at)
        """),
        orders_data
    )

print(f"Seed done. Users total: {len(user_ids)}, Orders added: {len(orders_data)}")

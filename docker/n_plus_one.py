"""Triggers 1 parent SELECT + 50 child SELECTs. Run: pip install psycopg[binary]; python n_plus_one.py"""
import psycopg
with psycopg.connect("postgresql://postgres:postgres@127.0.0.1:5433/shop", application_name="n1-demo") as c:
    orders = c.execute("SELECT id, user_id FROM orders ORDER BY id LIMIT 50").fetchall()
    for _, uid in orders:
        c.execute("SELECT * FROM users WHERE id = %s", (uid,)).fetchone()

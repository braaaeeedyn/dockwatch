"""Operations simulator (M3): writes a bike-share operator's day into the ops Postgres database.

model.py is pure (alerts + a simulated clock -> operations), synthetic.py makes seeded stations and alerts,
db.py executes operations with psycopg and counts what Debezium should see, explain.py writes sql/ops/PLANS.md.
"""

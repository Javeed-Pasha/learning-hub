"""Analyst check: prove Starburst/Trino can read the gold tables Spark and dbt just wrote."""
import os
import sys

import trino

TABLES = ["fct_orders", "fct_order_items", "fct_events", "dim_customers", "dim_products"]


def main() -> int:
    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=8080,
                               user="airflow", catalog="lakehouse", schema="gold")
    cur = conn.cursor()
    for table in TABLES:
        cur.execute(f"select count(*) from {table}")
        rows = cur.fetchone()[0]
        print(f"trino lakehouse.gold.{table:<16} {rows:>10,} rows")
        if rows == 0:
            return 1
    cur.execute("""
        select c.region, count(*) as orders, round(sum(o.order_revenue), 2) as revenue
        from fct_orders o join dim_customers c on o.customer_id = c.customer_id
        group by 1 order by 3 desc
    """)
    for region, orders, revenue in cur.fetchall():
        print(f"{region:<14} {orders:>8,} orders  {revenue:>14,.2f} revenue")
    return 0


if __name__ == "__main__":
    sys.exit(main())

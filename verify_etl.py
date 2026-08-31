"""Verifica el resultado e idempotencia del pipeline ETL de pedidos."""

import json
import sqlite3
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "Data" / "orders.db"
WATERMARK_PATH = PROJECT_DIR / "Data" / "orders_watermark.json"


with sqlite3.connect(DB_PATH) as conn:
    curated = pd.read_sql_query(
        """
        SELECT order_id, shipping_status, delivery_delay_days, carrier_name
        FROM orders_curated
        ORDER BY order_id
        """,
        conn,
    )

    total_rows = int(
        conn.execute("SELECT COUNT(*) FROM orders_curated").fetchone()[0]
    )
    unique_orders = int(
        conn.execute(
            "SELECT COUNT(DISTINCT order_id) FROM orders_curated"
        ).fetchone()[0]
    )
    duplicated_orders = conn.execute(
        """
        SELECT order_id, COUNT(*) AS occurrences
        FROM orders_curated
        GROUP BY order_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    quarantine = pd.read_sql_query(
        """
        SELECT order_id, shipment_id, rejection_reason
        FROM orders_quarantine
        ORDER BY rowid
        """,
        conn,
    )
    audit = pd.read_sql_query(
        """
        SELECT run_id, status, source_orders, valid_shipments,
               quarantined_shipments, inserted_orders, updated_orders
        FROM etl_audit
        ORDER BY started_at
        """,
        conn,
    )

if total_rows != unique_orders or duplicated_orders:
    raise SystemExit(
        f"ERROR: orders_curated contiene pedidos duplicados: {duplicated_orders}"
    )

print("\n=== orders_curated ===")
print(curated.to_string(index=False))
print(f"\nFilas totales: {total_rows}")
print(f"Pedidos únicos: {unique_orders}")
print("Duplicados por order_id: 0")

print("\n=== orders_quarantine ===")
print(quarantine.to_string(index=False))

print("\n=== etl_audit ===")
print(audit.to_string(index=False))

print(f"\nWatermark actual: {json.loads(WATERMARK_PATH.read_text(encoding='utf-8'))}")
print("\nVERIFICACIÓN OK: orders_curated no contiene duplicados por order_id.")

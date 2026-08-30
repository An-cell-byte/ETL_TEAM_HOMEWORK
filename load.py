"""Carga incremental de la tabla orders_curated para la casuística A2."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd


DATABASE_PATH = Path(__file__).resolve().parent / "Data" / "orders.db"


def load(
    orders_curated: pd.DataFrame,
    quarantine: pd.DataFrame,
    quality_result: dict,
    batch_id: str,
    database_path: Path = DATABASE_PATH,
) -> dict:
    """Hace UPSERT de orders_curated y guarda los registros en cuarentena."""

    if quality_result.get("status") != "PASS":
        raise ValueError("LOAD detenido porque el Quality Gate no pasó")

    required_columns = {
        "order_id",
        "shipping_status",
        "delivery_delay_days",
        "carrier_name",
    }
    missing_columns = required_columns - set(orders_curated.columns)
    if missing_columns:
        raise ValueError(
            f"orders_curated no tiene estas columnas: {sorted(missing_columns)}"
        )

    if orders_curated["order_id"].isna().any():
        raise ValueError("orders_curated contiene order_id vacío")
    if orders_curated["order_id"].duplicated().any():
        raise ValueError("orders_curated debe tener una sola fila por order_id")

    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    loaded_at = pd.Timestamp.now("UTC").isoformat()
    inserted = 0
    updated = 0
    quarantined = 0

    with closing(sqlite3.connect(database_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_curated (
                order_id INTEGER PRIMARY KEY,
                shipping_status TEXT NOT NULL,
                delivery_delay_days INTEGER,
                carrier_name TEXT,
                batch_id TEXT NOT NULL,
                loaded_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders_quarantine (
                source_system TEXT,
                batch_id TEXT NOT NULL,
                ingested_at TEXT,
                order_id INTEGER,
                shipment_id TEXT,
                carrier_code TEXT,
                shipped_at TEXT,
                delivered_at TEXT,
                rejection_reason TEXT NOT NULL,
                UNIQUE (batch_id, shipment_id, rejection_reason)
            )
            """
        )

        with conn:
            for _, row in orders_curated.iterrows():
                order_id = int(row["order_id"])
                exists = conn.execute(
                    "SELECT 1 FROM orders_curated WHERE order_id = ?", (order_id,)
                ).fetchone()

                conn.execute(
                    """
                    INSERT INTO orders_curated (
                        order_id,
                        shipping_status,
                        delivery_delay_days,
                        carrier_name,
                        batch_id,
                        loaded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        shipping_status = excluded.shipping_status,
                        delivery_delay_days = excluded.delivery_delay_days,
                        carrier_name = excluded.carrier_name,
                        batch_id = excluded.batch_id,
                        loaded_at = excluded.loaded_at
                    """,
                    (
                        order_id,
                        row["shipping_status"],
                        None
                        if pd.isna(row["delivery_delay_days"])
                        else int(row["delivery_delay_days"]),
                        None if pd.isna(row["carrier_name"]) else row["carrier_name"],
                        batch_id,
                        loaded_at,
                    ),
                )

                if exists:
                    updated += 1
                else:
                    inserted += 1

            if not quarantine.empty:
                if "rejection_reason" not in quarantine.columns:
                    raise ValueError(
                        "quarantine debe incluir la columna 'rejection_reason'"
                    )

                for _, row in quarantine.iterrows():
                    row_batch_id = row.get("batch_id", batch_id)
                    if pd.isna(row_batch_id):
                        row_batch_id = batch_id

                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO orders_quarantine (
                            source_system,
                            batch_id,
                            ingested_at,
                            order_id,
                            shipment_id,
                            carrier_code,
                            shipped_at,
                            delivered_at,
                            rejection_reason
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row.get("source_system", "csv:shipments"),
                            str(row_batch_id),
                            None
                            if pd.isna(row.get("ingested_at"))
                            else str(row.get("ingested_at")),
                            None
                            if pd.isna(row.get("order_id"))
                            else int(row.get("order_id")),
                            None
                            if pd.isna(row.get("shipment_id"))
                            else str(row.get("shipment_id")),
                            None
                            if pd.isna(row.get("carrier_code"))
                            else str(row.get("carrier_code")),
                            None
                            if pd.isna(row.get("shipped_at"))
                            else str(row.get("shipped_at")),
                            None
                            if pd.isna(row.get("delivered_at"))
                            else str(row.get("delivered_at")),
                            str(row["rejection_reason"]),
                        ),
                    )
                    quarantined += cursor.rowcount

    if not orders_curated.empty and "updated_at" in orders_curated.columns:
        watermark_value = orders_curated["updated_at"].max()
        watermark_path = database_path.parent / "orders_watermark.json"
        watermark_path.write_text(
            json.dumps(
                {"last_processed_updated_at": str(watermark_value)},
                indent=2,
            ),
            encoding="utf-8",
        )

    result = {
        "inserted": inserted,
        "updated": updated,
        "quarantined": quarantined,
        "table": "orders_curated",
    }
    print("LOAD:", result)
    return result

"""Etapa AUDIT del ETL de pedidos."""

import logging
import sqlite3
from typing import Any


def audit(
    config: Any,
    run_id: str,
    started_at: str,
    finished_at: str,
    watermark_before: str | None,
    watermark_after: str | None,
    counts: dict[str, int],
    status: str,
) -> None:
    """Registra métricas y estado de una ejecución en la tabla de auditoría."""
    with sqlite3.connect(config.database_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.audit_table} (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                watermark_before TEXT,
                watermark_after TEXT,
                source_orders INTEGER,
                source_shipments INTEGER,
                valid_shipments INTEGER,
                quarantined_shipments INTEGER,
                inserted_orders INTEGER,
                updated_orders INTEGER,
                status TEXT
            )
            """
        )
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {config.audit_table} (
                run_id, started_at, finished_at, watermark_before, watermark_after,
                source_orders, source_shipments, valid_shipments, quarantined_shipments,
                inserted_orders, updated_orders, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                started_at,
                finished_at,
                watermark_before,
                watermark_after,
                counts.get("source_orders", 0),
                counts.get("source_shipments", 0),
                counts.get("valid_shipments", 0),
                counts.get("quarantined_shipments", 0),
                counts.get("inserted_orders", 0),
                counts.get("updated_orders", 0),
                status,
            ),
        )

    logging.info(
        "AUDIT: run_id=%s | status=%s registrado en la tabla %s",
        run_id,
        status,
        config.audit_table,
    )

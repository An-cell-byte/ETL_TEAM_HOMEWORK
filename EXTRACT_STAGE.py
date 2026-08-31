"""Ejecuta las etapas DEFINE, EXTRACT y STAGE del ETL de pedidos.

Las fuentes son ``orders.db`` (SQLite), ``shipments.csv`` y
``carriers.json``. El refresh definido para este ejercicio es incremental:
``updated_at`` y el watermark identifican pedidos nuevos o modificados.
"""

import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "Data"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


# ----------------------------------------------------------------------
# DEFINE: contrato del pipeline según DEFINE.md.
# Grain=pedido, business key=order_id y refresh incremental.
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ETLConfig:
    database_path: Path
    shipments_path: Path
    carriers_path: Path
    watermark_path: Path
    output_table: str
    audit_table: str
    grain: str
    business_key: str
    refresh_strategy: str
    quality_thresholds: dict[str, float]


def load_config() -> ETLConfig:
    """Define la configuración del ETL a partir de las fuentes del proyecto."""
    return ETLConfig(
        database_path=DATA_DIR / "orders.db",
        shipments_path=DATA_DIR / "shipments.csv",
        carriers_path=DATA_DIR / "carriers.json",
        watermark_path=DATA_DIR / "orders_watermark.json",
        output_table="orders_curated",
        audit_table="etl_audit",
        grain="un pedido",
        business_key="order_id",
        refresh_strategy="incremental",
        quality_thresholds={
            "unknown_carrier_rate_max": 0.25,
            "invalid_date_rate_max": 0.20,
        },
    )


def read_watermark(path: Path) -> str | None:
    """Lee la última fecha de actualización procesada."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as file:
        return json.load(file).get("last_processed_updated_at")


def write_watermark(path: Path, value: str) -> None:
    path.write_text(
        json.dumps({"last_processed_updated_at": value}, indent=2),
        encoding="utf-8",
    )


# ----------------------------------------------------------------------
# EXTRACT: se filtran pedidos mediante updated_at. Los envíos del lote son
# los que pertenecen a esos pedidos; el catálogo siempre se lee completo.
# ----------------------------------------------------------------------
def extract(
    config: ETLConfig, watermark: str | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(config.database_path) as conn:
        if watermark is None:
            orders = pd.read_sql_query("SELECT * FROM orders", conn)
        else:
            orders = pd.read_sql_query(
                "SELECT * FROM orders WHERE updated_at > :watermark",
                conn,
                params={"watermark": watermark},
            )

    shipments = pd.read_csv(config.shipments_path, dtype=str).replace(
        {"": pd.NA}
    )
    if watermark is not None:
        incremental_order_ids = set(orders["order_id"].astype(str))
        shipments = shipments[
            shipments["order_id"].isin(incremental_order_ids)
        ].copy()

    with config.carriers_path.open(encoding="utf-8") as file:
        carriers_raw = json.load(file)
    carriers = (
        pd.DataFrame.from_dict(carriers_raw, orient="index")
        .rename_axis("carrier_code")
        .reset_index()
    )

    logging.info(
        "EXTRACT: orders.db=%s pedidos | shipments.csv=%s paquetes | carriers.json=%s transportistas",
        len(orders),
        len(shipments),
        len(carriers),
    )
    return orders, shipments, carriers


# ----------------------------------------------------------------------
# STAGE: se conserva la trazabilidad de cada fuente antes de validar,
# transformar e integrar los datos.
# ----------------------------------------------------------------------
def stage(
    orders: pd.DataFrame,
    shipments: pd.DataFrame,
    carriers: pd.DataFrame,
    batch_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ingested_at = pd.Timestamp.now("UTC").isoformat()

    staged_orders = orders.copy()
    staged_orders["source_system"] = "sqlite:orders"
    staged_orders["batch_id"] = batch_id
    staged_orders["ingested_at"] = ingested_at

    staged_shipments = shipments.copy()
    staged_shipments["source_system"] = "csv:shipments"
    staged_shipments["batch_id"] = batch_id
    staged_shipments["ingested_at"] = ingested_at

    staged_carriers = carriers.copy()
    staged_carriers["source_system"] = "json:carriers"
    staged_carriers["batch_id"] = batch_id
    staged_carriers["ingested_at"] = ingested_at

    logging.info(
        "STAGE: batch_id=%s asignado a orders=%s, shipments=%s y carriers=%s",
        batch_id,
        len(staged_orders),
        len(staged_shipments),
        len(staged_carriers),
    )
    return staged_orders, staged_shipments, staged_carriers


def main() -> None:
    config = load_config()
    run_id = str(uuid.uuid4())
    batch_id = f"ETL_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    watermark_before = read_watermark(config.watermark_path)

    logging.info(
        "=== ETL DEFINE/EXTRACT/STAGE iniciado | run_id=%s batch_id=%s ===",
        run_id,
        batch_id,
    )
    logging.info(
        "DEFINE: grain=%s | business_key=%s | refresh=%s | output=%s | thresholds=%s",
        config.grain,
        config.business_key,
        config.refresh_strategy,
        config.output_table,
        config.quality_thresholds,
    )
    logging.info("Watermark anterior: %s", watermark_before or "sin watermark")

    orders, shipments, carriers = extract(config, watermark_before)
    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )

    if staged_orders.empty:
        logging.info("Sin pedidos nuevos o modificados")
    else:
        watermark_candidate = str(staged_orders["updated_at"].max())
        logging.info(
            "Watermark candidato=%s; se actualizará después de un LOAD exitoso",
            watermark_candidate,
        )

    print(
        f"STAGE listo: orders={len(staged_orders)}, "
        f"shipments={len(staged_shipments)}, carriers={len(staged_carriers)}"
    )


if __name__ == "__main__":
    main()

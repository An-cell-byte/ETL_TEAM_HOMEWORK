"""Ejecuta las etapas DEFINE, EXTRACT y STAGE del ETL de pedidos.

Las fuentes son ``orders.db`` (SQLite), ``shipments.csv`` y
``carriers.json``. El refresh definido para este ejercicio es full, por lo
que el watermark se conserva como metadata de la ejecución, pero no limita
la extracción.
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
# Grain=pedido, business key=order_id y refresh full.
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ETLConfig:
    database_path: Path
    shipments_path: Path
    carriers_path: Path
    watermark_path: Path
    output_table: str
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
        grain="un pedido",
        business_key="order_id",
        refresh_strategy="full",
        quality_thresholds={
            "unknown_carrier_rate_max": 0.25,
            "invalid_date_rate_max": 0.20,
        },
    )


def read_watermark(path: Path) -> str | None:
    """Lee el watermark anterior; en refresh full no se usa para filtrar."""
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
# EXTRACT: cada fuente se lee por separado. Se extraen todas las filas
# porque DEFINE.md establece una estrategia de refresh full.
# ----------------------------------------------------------------------
def extract(
    config: ETLConfig, watermark: str | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    del watermark  # El refresh full no aplica filtro incremental.

    with sqlite3.connect(config.database_path) as conn:
        orders = pd.read_sql_query("SELECT * FROM orders", conn)

    shipments = pd.read_csv(config.shipments_path, dtype=str).replace(
        {"": pd.NA}
    )

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

    watermark_after = str(staged_orders["updated_at"].max())
    write_watermark(config.watermark_path, watermark_after)
    logging.info("Watermark actualizado: %s", watermark_after)

    print(
        f"STAGE listo: orders={len(staged_orders)}, "
        f"shipments={len(staged_shipments)}, carriers={len(staged_carriers)}"
    )


if __name__ == "__main__":
    main()

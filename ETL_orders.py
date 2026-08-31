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
        "EXTRACT: orders.db= %s pedidos | shipments.csv= %s paquetes | carriers.json= `%s transportistas",
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
        "STAGE: batch_id= %s asignado a orders= %s, shipments= %s y carriers= %s",
        batch_id,
        len(staged_orders),
        len(staged_shipments),
        len(staged_carriers),
    )
    return staged_orders, staged_shipments, staged_carriers

# ------------------------------------------- Validate -------------------------------------------

def validate(
    staged_orders: pd.DataFrame,
    staged_shipments: pd.DataFrame,
    staged_carriers: pd.DataFrame,
    config: ETLConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    
    orders = staged_orders.copy()
    shipments = staged_shipments.copy()

    known_order_ids = set(pd.to_numeric(orders["order_id"], errors="coerce").dropna())
    known_carriers = set(staged_carriers["carrier_code"].dropna().astype(str))

    shipments["order_id"] = pd.to_numeric(shipments["order_id"], errors="coerce")
    shipped_dates = pd.to_datetime(shipments["shipped_at"], errors="coerce")
    delivered_dates = pd.to_datetime(shipments["delivered_at"], errors="coerce")
    delivered_missing = shipments["delivered_at"].isna()

    reasons: list[str] = []
    
    for index, row in shipments.iterrows():
        row_reasons: list[str] = []
        shipped_at = shipped_dates.loc[index]
        delivered_at = delivered_dates.loc[index]

        if pd.isna(row["order_id"]) or row["order_id"] not in known_order_ids:
            row_reasons.append("order_id_not_found")
        if pd.isna(row["shipment_id"]) or not str(row["shipment_id"]).strip():
            row_reasons.append("shipment_id_empty")
        if pd.isna(row["carrier_code"]) or not str(row["carrier_code"]).strip():
            row_reasons.append("carrier_code_empty")
        if pd.isna(shipped_at):
            row_reasons.append("invalid_shipped_at")
        if not delivered_missing.loc[index] and pd.isna(delivered_at):
            row_reasons.append("invalid_delivered_at")
        if pd.notna(shipped_at) and pd.notna(delivered_at) and delivered_at < shipped_at:
            row_reasons.append("delivered_before_shipped")

        reasons.append(";".join(row_reasons))

    shipments["rejection_reason"] = pd.Series(
        reasons,
        index=shipments.index,
        dtype="string",
    )
    valid_shipments = shipments[shipments["rejection_reason"] == ""].copy()
    quarantined_shipments = shipments[shipments["rejection_reason"] != ""].copy()

    # Un transportista desconocido no invalida el paquete: se reporta para
    # reconciliation y se resolverá durante la integración.
    
    unknown_carrier_count = int(
        (~valid_shipments["carrier_code"].isin(known_carriers)).sum()
    )
    invalid_date_count = int(
        shipments["rejection_reason"].str.contains(
            "invalid_|delivered_before_shipped", regex=True
        ).sum()
    )
    reconciliation = {
        "source_orders": len(orders),
        "source_shipments": len(shipments),
        "valid_shipments": len(valid_shipments),
        "quarantined_shipments": len(quarantined_shipments),
        "unknown_carrier_count": unknown_carrier_count,
        "unknown_carrier_rate": unknown_carrier_count / len(valid_shipments)
        if len(valid_shipments)
        else 0.0,
        "invalid_date_count": invalid_date_count,
        "invalid_date_rate": invalid_date_count / len(shipments) if len(shipments) else 0.0,
    }

    logging.info(
        "VALIDATE: paquetes válidos= %s | cuarentena= %s | reconciliation= %s",
        len(valid_shipments),
        len(quarantined_shipments),
        reconciliation,
    )
    return valid_shipments, quarantined_shipments, reconciliation


def save_quarantine(quarantined_shipments: pd.DataFrame, config: ETLConfig) -> None:
    """Agrega a cuarentena los rechazos del lote incremental actual."""
    quarantine_columns = [
        "order_id",
        "shipment_id",
        "carrier_code",
        "shipped_at",
        "delivered_at",
        "rejection_reason",
        "source_system",
        "batch_id",
        "ingested_at",
    ]
    if not quarantined_shipments.empty:
        with sqlite3.connect(config.database_path) as conn:
            quarantined_shipments[quarantine_columns].to_sql(
                "orders_quarantine", conn, if_exists="append", index=False
            )
    logging.info(
        "QUARANTINE: %s registros guardados en orders_quarantine",
        len(quarantined_shipments),
    )

# ------------------------------------------- Transform -------------------------------------------

def transform(
    valid_shipments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calcula el estado y selecciona el paquete relevante de cada pedido."""

    shipments = valid_shipments.copy()
    shipments["shipped_at"] = pd.to_datetime(
        shipments["shipped_at"], errors="coerce"
    )
    
    shipments["delivered_at"] = pd.to_datetime(
        shipments["delivered_at"], errors="coerce"
    )
    
    shipments["shipping_status"] = "delivered"
    shipments.loc[
        shipments["delivered_at"].isna(), "shipping_status"
    ] = "in_transit"

    shipments["delivery_delay_days"] = (
        shipments["delivered_at"] - shipments["shipped_at"]
    ).dt.days.astype("Int64")

    # Si algún paquete sigue en tránsito, el pedido también está en tránsito.
    # Si todos llegaron, se selecciona el paquete que tardó más días.
    
    shipments["selection_priority"] = (
        shipments["shipping_status"] == "in_transit"
    ).astype(int)

    selected_shipments = (
        shipments.sort_values(
            [
                "order_id",
                "selection_priority",
                "delivery_delay_days",
                "shipped_at",
                "shipment_id",
            ],
            na_position="first",
        )
        .groupby("order_id", as_index=False)
        .tail(1)
        .drop(columns="selection_priority")
        .copy()
    )

    shipments = shipments.drop(columns="selection_priority")
    logging.info(
        "TRANSFORM: paquetes válidos= %s | pedidos con paquete seleccionado= %s",
        len(shipments),
        len(selected_shipments),
    )
    return shipments, selected_shipments


def integrate(
    staged_orders: pd.DataFrame,
    transformed_shipments: pd.DataFrame,
    selected_shipments: pd.DataFrame,
    staged_carriers: pd.DataFrame,
    reconciliation: dict,
) -> tuple[pd.DataFrame, dict]:
    
    """Integra las fuentes y produce una sola fila por pedido."""

    if staged_orders["order_id"].duplicated().any():
        raise ValueError("orders contiene order_id duplicados")
    if selected_shipments["order_id"].duplicated().any():
        raise ValueError("Hay más de un paquete seleccionado por pedido")
    if staged_carriers["carrier_code"].duplicated().any():
        raise ValueError("carriers contiene carrier_code duplicados")

    selected_with_carrier = selected_shipments.merge(
        staged_carriers[["carrier_code", "carrier_name", "country"]],
        on="carrier_code",
        how="left",
        validate="many_to_one",
    )

    integrated = staged_orders.merge(
        selected_with_carrier,
        on="order_id",
        how="left",
        validate="one_to_one",
    )

    integrated["shipping_status"] = integrated["shipping_status"].fillna(
        "no_valid_shipment"
    )
    integrated["delivery_delay_days"] = integrated[
        "delivery_delay_days"
    ].astype("Int64")

    orders_curated = integrated[
        [
            "order_id",
            "shipping_status",
            "delivery_delay_days",
            "carrier_name",
            "updated_at",
        ]
    ].copy()

    reconciliation = {
        **reconciliation,
        "rows_before_integrate": len(staged_orders),
        "rows_after_integrate": len(orders_curated),
        "duplicated_order_id": int(
            orders_curated["order_id"].duplicated().sum()
        ),
    }

    if reconciliation["rows_before_integrate"] != reconciliation[
        "rows_after_integrate"
    ]:
        raise ValueError("La integración cambió la cantidad de pedidos")

    logging.info("INTEGRATE / RECONCILIATION: %s", reconciliation)
    return orders_curated, reconciliation

def quality_gate(reconciliation: dict, config: ETLConfig) -> dict:
    """Comprueba los umbrales sin rechazar transportistas desconocidos."""
    metrics = {
        "unknown_carrier_rate": round(reconciliation["unknown_carrier_rate"], 2),
        "invalid_date_rate": round(reconciliation["invalid_date_rate"], 2),
    }
    failures = []
    if metrics["unknown_carrier_rate"] > config.quality_thresholds["unknown_carrier_rate_max"]:
        failures.append("unknown_carrier_rate")
    if metrics["invalid_date_rate"] > config.quality_thresholds["invalid_date_rate_max"]:
        failures.append("invalid_date_rate")

    result = {"status": "FAIL" if failures else "PASS", "metrics": metrics, "failures": failures}
    logging.info("QUALITY GATE: %s", result)
    return result

# ------------------------------------------- LOAD -------------------------------------------

def load(
    config: ETLConfig,
    orders_curated: pd.DataFrame,
    quarantined_shipments: pd.DataFrame,
    batch_id: str,
) -> tuple[int, int]:

    """Cargamos los datos mediante un UPSERT idempotente"""
    
    if orders_curated.empty:
        logging.info("LOAD: No hay datos para procesar.")
        return 0, 0

    inserted = 0
    updated = 0

    with sqlite3.connect(config.database_path) as conn:
        
        # Definimos la estructura de la tabla curada:
        
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.output_table} (
                order_id INTEGER PRIMARY KEY,
                shipping_status TEXT,
                delivery_delay_days INTEGER,
                carrier_name TEXT,
                updated_at TEXT,
                batch_id TEXT
            )
            """
        )

        try:
            with conn:
                for _, row in orders_curated.iterrows():
                    cur = conn.execute(
                        f"SELECT 1 FROM {config.output_table} WHERE order_id = ?",
                        (int(row["order_id"]),),
                    )
                    exists = cur.fetchone() is not None

                    conn.execute(
                        f"""
                        INSERT INTO {config.output_table}
                        (order_id, shipping_status, delivery_delay_days, carrier_name, updated_at, batch_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(order_id) DO UPDATE SET
                            shipping_status = excluded.shipping_status,
                            delivery_delay_days = excluded.delivery_delay_days,
                            carrier_name = excluded.carrier_name,
                            updated_at = excluded.updated_at,
                            batch_id = excluded.batch_id
                        """,
                        (
                            int(row["order_id"]),
                            row["shipping_status"],
                            None
                            if pd.isna(row["delivery_delay_days"])
                            else int(row["delivery_delay_days"]),
                            row["carrier_name"]
                            if pd.notna(row["carrier_name"])
                            else None,
                            str(row["updated_at"]),
                            batch_id,
                        ),
                    )

                    if exists:
                        updated += 1
                    else:
                        inserted += 1

            if not quarantined_shipments.empty:
                save_quarantine(quarantined_shipments, config)

        except Exception as e:
            logging.error(
                "LOAD: Error durante la transacción SQL, ejecutando ROLLBACK. Detalle: %s",
                e,
            )
            raise

    logging.info(
        "LOAD: %s insertados, %s actualizados (UPSERT) en la tabla %s",
        inserted,
        updated,
        config.output_table,
    )

    watermark_candidate = str(orders_curated["updated_at"].max())
    write_watermark(config.watermark_path, watermark_candidate)

    return inserted, updated
 
 # ------------------------------------------- AUDIT -------------------------------------------
 
def audit(config: ETLConfig,
        run_id: str,
        started_at: str,
        finished_at: str,
        watermark_before: str | None,
        watermark_after: str | None,
        counts: dict[str, int],
        status: str) -> None:
    
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
        
    with conn:
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
        ))

    logging.info(
        "AUDIT: run_id=%s | status=%s registrado en la tabla %s",
        run_id,
        status,
        config.audit_table,
    )
 
 
# ------------------------------------------- Función main_ETL() -------------------------------------------

def main_ETL() -> None:
    config = load_config()
    run_id = str(uuid.uuid4())
    batch_id = f"ETL_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H:%M:%S')}"

    started_at = pd.Timestamp.now("UTC").isoformat()
    watermark_before = read_watermark(config.watermark_path)

    logging.info(
        "=== ETL iniciado | run_id= %s | batch_id= %s ===", run_id, batch_id
    )

    orders, shipments, carriers = extract(config, watermark_before)
    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )

    if staged_orders.empty:
        
        logging.info("Sin pedidos nuevos o modificados")
        watermark_after = watermark_before
        inserted, updated = 0, 0
        valid_shipments, quarantined_shipments = pd.DataFrame(), pd.DataFrame()
        
    else:
        
        valid_shipments, quarantined_shipments, reconciliation = validate(
            staged_orders, staged_shipments, staged_carriers, config
        )
        transformed, selected = transform(valid_shipments)
        orders_curated, reconciliation = integrate(
            staged_orders,
            transformed,
            selected,
            staged_carriers,
            reconciliation,
        )

        gate_result = quality_gate(reconciliation, config)
        if gate_result["status"] == "FAIL":
            raise SystemExit(
                "Pipeline detenido: los umbrales de calidad fueron excedidos"
            )

        inserted, updated = load(
            config, orders_curated, quarantined_shipments, batch_id
        )

        watermark_after = str(staged_orders["updated_at"].max())
        write_watermark(config.watermark_path, watermark_after)

    finished_at = pd.Timestamp.now("UTC").isoformat()
    counts = {
        "source_orders": len(staged_orders),
        "source_shipments": len(staged_shipments),
        "valid_shipments": len(valid_shipments),
        "quarantined_shipments": len(quarantined_shipments),
        "inserted_orders": inserted,
        "updated_orders": updated,
    }

    audit(
        config=config,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        watermark_before=watermark_before,
        watermark_after=watermark_after,
        counts=counts,
        status="SUCCESS",
    )

if __name__ == "__main__":
    main_ETL()
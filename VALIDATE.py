"""Ejecuta la fase VALIDATE del ETL de pedidos.

La validación se realiza a nivel de paquete, porque ``shipments.csv`` puede
tener varias filas para un mismo pedido. Los paquetes rechazados se guardan
en ``orders_quarantine`` y no pasan a las fases posteriores.
"""

import logging
import sqlite3

import pandas as pd

from EXTRACT_STAGE import ETLConfig, extract, load_config, read_watermark, stage


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


# ----------------------------------------------------------------------
# VALIDATE: aplica el contrato de DEFINE.md. La ausencia de delivered_at
# es válida porque representa un paquete que sigue en tránsito.
# ----------------------------------------------------------------------
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

    if quarantined_shipments.empty:
        logging.info("VALIDATE: no se enviaron registros a cuarentena")
    else:
        for _, rejected in quarantined_shipments.iterrows():
            logging.info(
                "VALIDATE: registro enviado a cuarentena | order_id=%s | shipment_id=%s | motivo=%s",
                rejected.get("order_id"),
                rejected.get("shipment_id"),
                rejected["rejection_reason"],
            )

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
        "VALIDATE: paquetes válidos=%s | cuarentena=%s | reconciliation=%s",
        len(valid_shipments),
        len(quarantined_shipments),
        reconciliation,
    )
    return valid_shipments, quarantined_shipments, reconciliation


def quality_gate(reconciliation: dict, config: ETLConfig) -> dict:
    """Comprueba los umbrales sin rechazar transportistas desconocidos."""
    metrics = {
        "unknown_carrier_rate": round(reconciliation["unknown_carrier_rate"], 4),
        "invalid_date_rate": round(reconciliation["invalid_date_rate"], 4),
    }
    failures = []
    if metrics["unknown_carrier_rate"] > config.quality_thresholds["unknown_carrier_rate_max"]:
        failures.append("unknown_carrier_rate")
    if metrics["invalid_date_rate"] > config.quality_thresholds["invalid_date_rate_max"]:
        failures.append("invalid_date_rate")

    result = {"status": "FAIL" if failures else "PASS", "metrics": metrics, "failures": failures}
    logging.info("QUALITY GATE: %s", result)
    return result


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


def main() -> None:
    config = load_config()
    watermark = read_watermark(config.watermark_path)
    orders, shipments, carriers = extract(config, watermark)
    batch_id = f"VALIDATE_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )

    valid_shipments, quarantined_shipments, reconciliation = validate(
        staged_orders, staged_shipments, staged_carriers, config
    )
    gate_result = quality_gate(reconciliation, config)
    save_quarantine(quarantined_shipments, config)

    if gate_result["status"] == "FAIL":
        raise SystemExit("Pipeline detenido: los umbrales de calidad fueron excedidos")

    print(
        f"VALIDATE listo: {len(valid_shipments)} filas válidas, "
        f"{len(quarantined_shipments)} en cuarentena"
    )


if __name__ == "__main__":
    main()

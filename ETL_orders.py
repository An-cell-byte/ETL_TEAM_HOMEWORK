"""Orquestador del pipeline ETL completo de pedidos.

Este archivo coordina las etapas implementadas en módulos separados:
EXTRACT_STAGE, VALIDATE, TRANSFORM_INTEGRATE, quality, load y AUDIT.
"""

import logging
import uuid

import pandas as pd

from AUDIT import audit
from EXTRACT_STAGE import extract, load_config, read_watermark, stage, write_watermark
from TRANSFORM_INTEGRATE import integrate, transform
from VALIDATE import validate
from LOAD import load
from QUALITY import quality_gate


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def main_ETL() -> None:
    """Ejecuta DEFINE, EXTRACT, STAGE, VALIDATE, TRANSFORM, INTEGRATE,
    QUALITY GATE, LOAD y AUDIT en una sola corrida.
    """
    config = load_config()
    run_id = str(uuid.uuid4())
    batch_id = f"ETL_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    started_at = pd.Timestamp.now("UTC").isoformat()
    watermark_before = read_watermark(config.watermark_path)

    logging.info(
        "=== ETL iniciado | run_id=%s | batch_id=%s | watermark=%s ===",
        run_id,
        batch_id,
        watermark_before or "sin watermark",
    )

    # EXTRACT -> STAGE
    orders, shipments, carriers = extract(config, watermark_before)
    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )

    # VALIDATE
    valid_shipments, quarantine, reconciliation = validate(
        staged_orders, staged_shipments, staged_carriers, config
    )

    # TRANSFORM -> INTEGRATE
    transformed_shipments, selected_shipments = transform(valid_shipments)
    orders_curated, reconciliation = integrate(
        staged_orders,
        transformed_shipments,
        selected_shipments,
        staged_carriers,
        reconciliation,
    )

    # QUALITY GATE
    quality_result = quality_gate(valid_shipments, quarantine, reconciliation)
    if quality_result["status"] != "PASS":
        finished_at = pd.Timestamp.now("UTC").isoformat()
        audit(
            config=config,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            counts={
                "source_orders": len(staged_orders),
                "source_shipments": len(staged_shipments),
                "valid_shipments": len(valid_shipments),
                "quarantined_shipments": len(quarantine),
                "inserted_orders": 0,
                "updated_orders": 0,
            },
            status="FAILED_QUALITY_GATE",
        )
        raise SystemExit("Pipeline detenido: los umbrales de calidad fueron excedidos")

    # LOAD: carga orders_curated y orders_quarantine.
    load_result = load(
        orders_curated,
        quarantine,
        quality_result,
        batch_id,
        config.database_path,
    )

    watermark_after = str(staged_orders["updated_at"].max())
    write_watermark(config.watermark_path, watermark_after)

    finished_at = pd.Timestamp.now("UTC").isoformat()
    audit(
        config=config,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        watermark_before=watermark_before,
        watermark_after=watermark_after,
        counts={
            "source_orders": len(staged_orders),
            "source_shipments": len(staged_shipments),
            "valid_shipments": len(valid_shipments),
            "quarantined_shipments": len(quarantine),
            "inserted_orders": load_result["inserted"],
            "updated_orders": load_result["updated"],
        },
        status="SUCCESS",
    )

    logging.info("=== ETL finalizado correctamente ===")


if __name__ == "__main__":
    main_ETL()

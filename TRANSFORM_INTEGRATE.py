"""Etapas TRANSFORM e INTEGRATE de la casuística A2."""

import logging

import pandas as pd

from EXTRACT_STAGE import extract, load_config, read_watermark, stage
from VALIDATE import validate


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


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
        "TRANSFORM: paquetes válidos=%s | pedidos con paquete seleccionado=%s",
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


def main() -> None:
    config = load_config()
    watermark = read_watermark(config.watermark_path)
    orders, shipments, carriers = extract(config, watermark)
    batch_id = f"TRANSFORM_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    orders, shipments, carriers = stage(
        orders, shipments, carriers, batch_id
    )
    valid_shipments, quarantine, reconciliation = validate(
        orders, shipments, carriers, config
    )
    transformed, selected = transform(valid_shipments)
    orders_curated, reconciliation = integrate(
        orders,
        transformed,
        selected,
        carriers,
        reconciliation,
    )

    print("\nORDERS_CURATED")
    print(orders_curated.to_string(index=False))
    print("\nRECONCILIATION")
    print(reconciliation)
    print(f"\nCUARENTENA: {len(quarantine)} registro(s)")


if __name__ == "__main__":
    main()

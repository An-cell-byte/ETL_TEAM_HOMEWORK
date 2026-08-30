"""Quality Gate para la casuística A2."""

import pandas as pd


UNKNOWN_CARRIER_RATE_MAX = 0.25
INVALID_DATE_RATE_MAX = 0.20


def quality_gate(
    valid_shipments: pd.DataFrame,
    quarantine: pd.DataFrame,
    reconciliation: dict,
) -> dict:
    """Compara las métricas reales de la corrida con los límites de A2."""

    if "unknown_carrier" in reconciliation:
        unknown_carrier = int(reconciliation["unknown_carrier"])
    elif "unknown_carrier_count" in reconciliation:
        unknown_carrier = int(reconciliation["unknown_carrier_count"])
    else:
        raise ValueError(
            "reconciliation debe incluir 'unknown_carrier' o "
            "'unknown_carrier_count'"
        )

    if not quarantine.empty and "rejection_reason" not in quarantine.columns:
        raise ValueError("quarantine debe incluir la columna 'rejection_reason'")

    valid_count = len(valid_shipments)
    total_count = valid_count + len(quarantine)
    if quarantine.empty:
        invalid_dates = 0
    else:
        date_rejection_reasons = (
            "shipped_at_invalid",
            "delivered_at_invalid",
            "invalid_shipped_at",
            "invalid_delivered_at",
            "delivered_before_shipped",
        )
        invalid_dates = int(
            quarantine["rejection_reason"]
            .fillna("")
            .apply(
                lambda reason: any(
                    date_reason in reason
                    for date_reason in date_rejection_reasons
                )
            )
            .sum()
        )

    unknown_carrier_rate = unknown_carrier / valid_count if valid_count else 0.0
    invalid_date_rate = invalid_dates / total_count if total_count else 0.0

    failures = []
    if unknown_carrier_rate > UNKNOWN_CARRIER_RATE_MAX:
        failures.append("unknown_carrier_rate")
    if invalid_date_rate > INVALID_DATE_RATE_MAX:
        failures.append("invalid_date_rate")

    result = {
        "status": "FAIL" if failures else "PASS",
        "metrics": {
            "unknown_carrier_rate": round(unknown_carrier_rate, 4),
            "invalid_date_rate": round(invalid_date_rate, 4),
        },
        "thresholds": {
            "unknown_carrier_rate_max": UNKNOWN_CARRIER_RATE_MAX,
            "invalid_date_rate_max": INVALID_DATE_RATE_MAX,
        },
        "counts": {
            "valid_shipments": valid_count,
            "total_shipments": total_count,
            "unknown_carrier": unknown_carrier,
            "invalid_dates": invalid_dates,
        },
        "failures": failures,
    }

    print("QUALITY GATE:", result)
    return result

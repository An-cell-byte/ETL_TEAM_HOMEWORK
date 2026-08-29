import pandas as pd

datos = {
    "order_id": [100, 100, 101, 102, 103, 104],
    "shipment_id": ["S1", "S2", "S3", "S4", "S5", "S6"],
    "carrier_code": ["DHL", "DHL", "FDX", "FDX", "XYZ", "FDX"],
    "shipped_at": [
        "2026-08-10",
        "2026-08-11",
        "2026-08-12",
        "2026-08-15",
        "2026-08-16",
        "2026-08-20",
    ],
    "delivered_at": [
        "2026-08-13",
        "2026-08-16",
        "2026-08-14",
        "",
        "2026-08-18",
        "2026-08-18",
    ],
}

df = pd.DataFrame(datos)
df.to_csv("Data/shipments.csv", index=False)

print(df)


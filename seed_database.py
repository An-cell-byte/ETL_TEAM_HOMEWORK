import pandas as pd
import json
import sqlite3
from pathlib import Path

# Creamos el archivo .csv

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

#print(df)

# Creamos el Archivo .json:

STORAGE_ADDRESS = Path(__file__).resolve().parent / "Data"
STORAGE_ADDRESS.mkdir(parents=True, exist_ok=True)  # Crea la carpeta Data si no existe

json_path = STORAGE_ADDRESS / "carriers.json"

with open(json_path, "r", encoding="utf-8") as file:
    carriers_data = json.load(file)

conn = sqlite3.connect("app.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS carriers (
    code TEXT PRIMARY KEY,
    carrier_name TEXT NOT NULL,
    country TEXT NOT NULL
)
""")

for code, details in carriers_data.items():
    cursor.execute("""
    INSERT OR REPLACE INTO carriers (code, carrier_name, country)
    VALUES (?, ?, ?)
    """, (code, details["carrier_name"], details["country"]))

conn.commit()
conn.close()
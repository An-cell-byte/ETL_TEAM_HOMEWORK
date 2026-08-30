import sqlite3
from pathlib import Path
import pandas as pd


DB_PATH = Path(__file__).resolve().parent / "data" / "orders.db"

ORDERS = [
    # order_id, customer_id, order_date, status, updated_at
    (100, 1, "2026-08-20", "pending", "2026-08-20T09:00:00"),
    (101, 2, "2026-08-21", "confirmed", "2026-08-21T10:30:00"),
    (102, 3, "2026-08-22", "shipped", "2026-08-22T08:15:00"),
    (103, 4, "2026-08-23", "delivered", "2026-08-23T14:45:00"),
    (104, 5, "2026-08-24", "cancelled", "2026-08-24T16:00:00"),
]


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date DATE NOT NULL,
                status TEXT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO orders
                (order_id, customer_id, order_date, status, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ORDERS,
        )
        conn.commit()

    print(f"Sembrados {len(ORDERS)} pedidos en {DB_PATH}")
    
# Creamos el Archivo .json:

STORAGE_ADDRESS = Path(__file__).resolve().parent / "Data"
STORAGE_ADDRESS.mkdir(parents=True, exist_ok=True)  # Crea la carpeta Data si no existe

json_path = STORAGE_ADDRESS / "carriers.json"

datos_carriers = {
    "DHL": {"carrier_name": "DHL Express", "country": "Germany"},
    "FDX": {"carrier_name": "FedEx", "country": "USA"}
}

dfjson = pd.DataFrame.from_dict(datos_carriers, orient='index')

carpeta_destino = Path("Data") # Cambia "Data" por la ruta de la carpeta que prefieras
carpeta_destino.mkdir(parents=True, exist_ok=True)

ruta_archivo = carpeta_destino / "carriers.json"
dfjson.to_json(ruta_archivo, orient='index', indent=4, force_ascii=False)


def crear_csv():
    datos_envios = {
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

    df_csv = pd.DataFrame(datos_envios)
    df_csv.to_csv(STORAGE_ADDRESS / "shipments.csv", index=False)
    print(df_csv)


if __name__ == "__main__":
    main()
    crear_csv()

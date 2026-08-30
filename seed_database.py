import sqlite3
from pathlib import Path
import pandas as pd


DB_PATH = Path(__file__).resolve().parent / "Data" / "orders.db"
WATERMARK_PATH = Path(__file__).resolve().parent / "Data" / "orders_watermark.json"

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
    if WATERMARK_PATH.exists():
        WATERMARK_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL
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


if __name__ == "__main__":
    main()

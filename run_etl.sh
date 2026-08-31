#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

reset_watermark() {
    python -c "import json; from pathlib import Path; p=Path('Data/orders_watermark.json'); p.write_text(json.dumps({'last_processed_updated_at': '1900-01-01T00:00:00'}, indent=2), encoding='utf-8')"
}

echo ">>> 0) Instalando dependencias"
python -m pip install -q -r requirements.txt

echo ""
echo ">>> 1) Sembrando las fuentes de la casuística A2 una sola vez"
python seed_database.py
reset_watermark

echo ""
echo ">>> 2) Primera corrida con los datos iniciales"
python ETL_orders.py
echo ">>> SELECT COUNT(*) después de la primera corrida"
python verify_etl.py

echo ""
echo ">>> 3) Segunda corrida con los mismos datos"
echo "    Se reinicia solo el watermark; no se vuelve a sembrar la base."
reset_watermark
python ETL_orders.py
echo ">>> SELECT COUNT(*) después de la segunda corrida"
python verify_etl.py

echo ""
echo ">>> 4) Tercera corrida sin datos nuevos"
python ETL_orders.py
echo "    El pipeline debe mostrar 0 pedidos procesados por el watermark."

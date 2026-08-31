# ETL Team Homework

## Propósito

Este proyecto implementa un proceso ETL para la casuística A2: integrar pedidos, paquetes y transportistas en una tabla curada con una fila por pedido.

El pipeline utiliza una carga incremental basada en `updated_at` de `orders.db` y en un archivo de watermark. Los pedidos nuevos o modificados se cargan mediante `UPSERT`, evitando duplicados en `orders_curated`.

## Fuentes de datos

Las fuentes se encuentran en `Data`:

- `orders.db`: base SQLite con la tabla `orders`.
- `shipments.csv`: paquetes enviados; un pedido puede tener varios paquetes.
- `carriers.json`: catálogo de transportistas y sus nombres.

La semilla contiene los pedidos `100` a `104`. El paquete `S6` se envía a cuarentena porque `delivered_at` es anterior a `shipped_at`. Un `delivered_at` vacío representa un envío legítimamente en tránsito.

## Estructura del proyecto

```text
.
├── AUDIT.py                 # Registra cada corrida
├── DEFINE.md                # Contrato y estrategia ETL
├── ETL_orders.py            # Orquestador del pipeline completo
├── EXTRACT_STAGE.py         # DEFINE, EXTRACT y STAGE
├── LOAD.py                  # UPSERT de curated y cuarentena
├── QUALITY.py               # Quality Gate y umbrales
├── TRANSFORM_INTEGRATE.py   # TRANSFORM e INTEGRATE
├── VALIDATE.py              # Validación y registros rechazados
├── seed_database.py         # Genera las fuentes de prueba
├── verify_etl.py            # Verifica conteos y duplicados
├── run_etl.sh               # Ejecuta la demostración completa
├── requirements.txt         # Dependencias Python
└── Data/
    ├── orders.db
    ├── shipments.csv
    ├── carriers.json
    └── orders_watermark.json
```

## Flujo ETL

```text
DEFINE → EXTRACT → STAGE → VALIDATE → TRANSFORM
→ INTEGRATE → QUALITY GATE → LOAD → AUDIT
```

### EXTRACT y STAGE

Cada fuente se lee por separado. Cada registro recibe los metadatos:

- `source_system`: origen del registro.
- `batch_id`: ejecución a la que pertenece.
- `ingested_at`: momento de recepción.

En modo incremental, `orders.db` se filtra con `updated_at > watermark`; los paquetes se filtran por los `order_id` seleccionados y el catálogo de transportistas se lee completo.

### VALIDATE

Los paquetes inválidos se guardan en `orders_quarantine` junto con el motivo exacto. Para los datos de prueba:

- 5 paquetes válidos.
- 1 paquete en cuarentena: `S6`.
- Motivo: `delivered_before_shipped`.
- `XYZ` se reporta como transportista desconocido, pero no se rechaza.

### TRANSFORM e INTEGRATE

Se calculan `shipping_status` y `delivery_delay_days`. Cuando un pedido tiene varios paquetes, se selecciona el paquete más lento; para el pedido `100` se utiliza `S2` y el retraso es de 5 días.

La integración produce `orders_curated`, cuyo grain es un pedido y cuyas columnas mínimas son:

```text
order_id
shipping_status
delivery_delay_days
carrier_name
```

También se verifican las cardinalidades `many_to_one` y `one_to_one`, y se genera un diccionario de reconciliation.

### QUALITY GATE

Los umbrales configurados son:

```text
unknown_carrier_rate_max = 0.25
invalid_date_rate_max    = 0.20
```

Con la semilla actual:

- Transportistas desconocidos: `1/5 = 20%`.
- Fechas inválidas: `1/6 = 16.67%`.
- Resultado: `PASS`.

### LOAD y AUDIT

`LOAD.py` realiza un `UPSERT` en `orders_curated` usando `order_id` como clave primaria y guarda los rechazos en `orders_quarantine`.

`AUDIT.py` registra una fila por corrida en `etl_audit`, incluyendo conteos, watermarks y estado.

## Instalación

Se requiere Python 3.12 o superior:

```bash
python -m pip install -r requirements.txt
```

La dependencia directa es `pandas==3.0.5`.

## Ejecución completa

### Git Bash

```bash
cd "/c/Users/usario/.../ETL_TEAM_HOMEWORK/"
source ../venv/Scripts/activate
bash run_etl.sh
```

El script siembra las fuentes una sola vez, ejecuta dos corridas con los mismos datos, verifica que `orders_curated` no tenga duplicados y realiza una tercera corrida sin datos nuevos, que debe procesar `0` pedidos.

### PowerShell

PowerShell no ejecuta `.sh` directamente. Si Git Bash está instalado, puede invocarse así:

```powershell
cd "C:\Users\sborb\Desktop\Universidad\7mo semestre\Materias\ETL_TEAM_HOMEWORK\ETL_TEAM_HOMEWORK"
bash .\run_etl.sh
```

También se pueden ejecutar los archivos Python directamente:

```powershell
..\venv\Scripts\python.exe seed_database.py
..\venv\Scripts\python.exe ETL_orders.py
..\venv\Scripts\python.exe verify_etl.py
```

## Verificación de duplicados

`verify_etl.py` ejecuta consultas como:

```sql
SELECT COUNT(*) FROM orders_curated;

SELECT COUNT(DISTINCT order_id) FROM orders_curated;
```

La verificación es correcta cuando el número de filas totales coincide con el número de pedidos únicos y no existen resultados en la consulta agrupada de duplicados.

## Demostración del funcionamiento

Video de drive: https://drive.google.com/file/d/1kq9CJ3gMvEF1pYOhuRE7uDRS_4UwkYbK/view?usp=sharing

## Reflexiones finales

Se tomó como referencia la estructura de las funciones y clases de los ejemplos vistos en clase relativos al ETL, lo cual nos sirvió para fundar una base inicial que posteriormente se adaptaría a la casuística A2. Para el desarrollo de este trabajo, se usaron conceptos como el watermark (fechas de actualización), las fases de ETL (distribuidas en diferentes archivos de python y orquestadas por un archivo central ETL_orders.py), y diversas fuentes de información (archivos .csv, .db, .json). 

Lo que hicimos diferente de los ejemplos vistos en clase fue básicamente adaptar el watermark al tipo de datos de las órdenes de la casuística, utilizando updated_at y almacenando el último valor procesado en orders_watermark.json; invalidar los paquetes con fechas de entrega anteriores a las fechas de envío mediante pandas.to_datetime durante VALIDATE, guardándolos en orders_quarantine con su motivo; crear una estructura general y sólida de los datos obtenidos de diversas fuentes durante TRANSFORM, usando operaciones de limpieza, agrupación y selección del paquete más lento; ajustar en QUALITY GATE los umbrales máximos a 20% para fechas erróneas y 25% para proveedores desconocidos; y dividir el proyecto en diferentes módulos Python, importados y orquestados por el archivo central ETL_orders.py.

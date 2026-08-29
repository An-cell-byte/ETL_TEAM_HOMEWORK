# Elegimos la Casuística A2

## Fuentes

- `orders.db`, tabla `orders`: contiene la información principal de cada pedido y su estado.
- `shipments.csv`: contiene los paquetes enviados. Un pedido puede aparecer varias veces porque puede tener más de un paquete.
- `carriers.json`: sirve como catálogo para obtener el nombre y el país de cada transportista a partir de `carrier_code`.

## Grain de `orders_curated`

Cada fila representa un pedido completo. Si el pedido tiene varios paquetes, se consolidan en una sola fila y se toma en cuenta el paquete que tardó más para calcular el retraso de entrega.

## Business key

La business key es `order_id`. Ya existe en `orders` y en `shipments.csv`, por lo que no es necesario crear una nueva.

## Refresh strategy

Se usará un refresh **full**. En cada ejecución se vuelven a leer todas las fuentes y se reconstruye `orders_curated`. Esto tiene sentido porque el volumen de datos de este ejercicio es pequeño y `shipments.csv` no tiene un campo que permita saber qué filas cambiaron.

## Contrato de datos

| Campo | Tipo | Requerido | Regla de validez |
|---|---|---|---|
| `order_id` | entero | Sí | Debe existir en la tabla `orders`. |
| `shipment_id` | texto | Sí | No puede estar vacío y debe identificar un paquete. |
| `carrier_code` | texto | Sí | No puede estar vacío. Si no existe en `carriers.json`, se marca como transportista desconocido, pero no se rechaza. |
| `shipped_at` | fecha | Sí | Debe ser una fecha válida. |
| `delivered_at` | fecha | No | Puede estar vacío si el paquete sigue en tránsito. Si tiene valor, no puede ser anterior a `shipped_at`; si lo es, se rechaza con el motivo `delivered_before_shipped`. |

El Quality Gate permite como máximo `unknown_carrier_rate = 0.25` e `invalid_date_rate = 0.20`. Con los datos semilla, las tasas son `1/5 = 20%` y `1/6 = 16.6%`, por lo que ambas pasan.

## Registros que no cumplen

Un registro inválido no entra a `orders_curated`. Se guarda en cuarentena junto con el motivo del rechazo para poder revisarlo. En este caso, el paquete `S6` queda en cuarentena con el motivo `delivered_before_shipped`. Un transportista desconocido como `XYZ` se conserva, se cuenta en la reconciliación y no se manda a cuarentena mientras no se supere el umbral permitido.

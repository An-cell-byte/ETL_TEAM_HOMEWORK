# Elegimos la Casuística A2

## Qué representa cada fuente?

- La de sqlite `orders.db` esta tiene toda la información principal de cada pedido y su estado.
- La de csv `shipments.csv`es la que contiene los paquetes enviados. Un pedido puede aparecer varias veces porque puede tener más de un paquete.
- La de json `carriers.json`: sirve como catálogo para obtener el nombre y el país de cada transportista a partir de `carrier_code`.

## Grain de `orders_curated`

Cada fila representa un pedido completo. Si el pedido tiene varios paquetes, se juntan en una sola fila y se toma en cuenta el paquete que tardó más para calcular el retraso de entrega.

## Business key

La business key es `order_id`. Ya existe en `orders` y en `shipments.csv`, por lo que no es necesario crear una nueva.

## Refresh strategy

Elegimos usar un refresh full para que en cada ejecución se vuelven a leer todas las fuentes reconstruyendo `orders_curated`. Esto tiene sentido porque el volumen de datos de este ejercicio es pequeño y `shipments.csv` no tiene un campo que permita saber si se modificaron filas

## Contrato de datos

| Campo | Tipo | Requerido | Regla de validez |
|---|---|---|---|
| `order_id` | entero | Sí | Debe existir en la tabla `orders`. |
| `shipment_id` | texto | Sí | No puede estar vacío y debe identificar un paquete. |
| `carrier_code` | texto | Sí | No puede estar vacío. Si no existe en `carriers.json`, se marca como transportista desconocido, pero no se rechaza. |
| `shipped_at` | fecha | Sí | Debe ser una fecha válida. |
| `delivered_at` | fecha | No | Puede estar vacío si el paquete sigue en tránsito. Si tiene valor, no puede ser anterior a `shipped_at`; si lo es, se rechaza con el motivo `delivered_before_shipped`. |

Antes de crear la tabla final, revisamos que no haya demasiados datos incorrectos. En nuestros datos hay un transportista desconocido y un envío con fechas incorrectas. Como estas cantidades están dentro de los límites permitidos, el proceso puede continuar.

## Registros que no cumplen

Un registro inválido no entra a `orders_curated`. Se guarda en cuarentena junto con el motivo del rechazo. En este ejemplo, tenemos el paquete `S6` que queda en cuarentena con el motivo `delivered_before_shipped`. 

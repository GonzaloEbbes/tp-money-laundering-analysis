# Handoff: deduplicacion estandar de mensajes

## Contexto

Se necesita implementar un mecanismo reutilizable para que todos los nodos del
pipeline descarten resultados o mensajes duplicados de forma estandar. El
gateway tambien debe considerarse un nodo dentro de este mecanismo.

La idea descartada fue tener un nodo dedicado de deduplicacion de resultados
(`result_deduplicator`). La deduplicacion debe ser responsabilidad comun de cada
consumidor.

## Objetivo

Agregar deduplicacion por nodo consumidor usando IDs incrementales y estado
compactado en rangos, con persistencia para sobrevivir reinicios de
contenedores.

Ejemplo de estado:

```text
Mensajes procesados: 20, 21, 22, 23, 26
Estado persistido: 20-23;26
```

Si luego llega `22`, se descarta como duplicado. Si llega `24`, el estado pasa a
`20-24;26`. Si luego llega `25`, se compacta a `20-26`.

## Decisiones tomadas

- Agregar un campo nuevo al protocolo interno, por ejemplo `message_id`.
- No reutilizar ni cambiar el significado de `data_id`.
- `data_id` debe seguir representando el dato original o identificador de
  negocio actual.
- `message_id` debe ser incremental y servir para deduplicacion.
- La deduplicacion debe aplicar a todos los nodos, gateway incluido.
- El estado debe persistirse, no vivir solo en memoria.
- No implementar un `result_deduplicator` separado.

## Estado actual observado

En `src/common/message_protocol/internal.py` existe `InternalMessage` con:

- `type`
- `source_client_uuid`
- `data_id`
- `data`

El gateway ya genera `data_id` incremental en
`src/gateway/message_handler/message_handler.py`.

Muchos nodos propagan ese `data_id`, pero algunos generan UUIDs nuevos como
`data_id`, especialmente en partes de scatter/gather y filtros agregadores. Por
eso conviene agregar `message_id` separado.

## Clave de deduplicacion

No alcanza con deduplicar solo por `message_id`, porque podria haber varios
clientes, tipos de mensaje o entradas con secuencias independientes.

Clave recomendada:

```text
node_id
input_queue
source_client_uuid
message_type
```

El valor asociado a esa clave es el conjunto compactado de rangos de
`message_id` ya procesados.

Conceptualmente:

```text
dedup_key = (node_id, input_queue, source_client_uuid, message_type)
processed_ranges = ranges(message_id)
```

## Persistencia

Como el mecanismo debe sobrevivir reinicios de contenedores, el estado de rangos
debe persistirse en un volumen.

Opciones:

- SQLite local por nodo, montado sobre volumen persistente.
- Archivo JSON por nodo, tambien en volumen.
- Store externo como Redis.

Para este proyecto, SQLite local parece la opcion mas robusta y simple: ofrece
actualizaciones atomicas y reduce riesgo de corrupcion ante caidas del proceso.

Tabla posible:

```sql
CREATE TABLE processed_ranges (
  node_id TEXT NOT NULL,
  input_queue TEXT NOT NULL,
  source_client_uuid TEXT NOT NULL,
  message_type INTEGER NOT NULL,
  ranges TEXT NOT NULL,
  PRIMARY KEY (node_id, input_queue, source_client_uuid, message_type)
);
```

`ranges` puede guardar una representacion compacta como:

```text
20-23;26;30-35
```

## Flujo esperado de consumo

Para cada mensaje recibido:

1. Deserializar el mensaje interno.
2. Leer `message_id`.
3. Si no hay `message_id`, tratarlo como mensaje legacy o no deduplicable.
4. Buscar rangos persistidos para la clave del nodo.
5. Si `message_id` ya esta dentro de un rango:
   - no ejecutar logica de negocio;
   - hacer `ack`;
   - loguear que se descarto un duplicado.
6. Si no esta procesado:
   - ejecutar la logica normal del nodo;
   - publicar mensajes downstream si corresponde;
   - persistir el nuevo `message_id` en los rangos;
   - hacer `ack`.

Punto importante: marcar el `message_id` como procesado despues de completar la
logica del nodo. Si se persiste antes y el proceso cae antes de publicar o
terminar, se podria perder el mensaje.

## Ubicacion propuesta

La logica debe vivir en `src/common`, no repetida en cada handler.

Estructura posible:

```text
src/common/dedup/__init__.py
src/common/dedup/ranges.py
src/common/dedup/store.py
src/common/dedup/deduplicator.py
```

Responsabilidades:

- `ranges.py`: estructura para agregar IDs, consultar duplicados y mergear
  intervalos.
- `store.py`: persistencia SQLite o backend equivalente.
- `deduplicator.py`: API de alto nivel para decidir si un mensaje se procesa o
  se descarta.

Integracion:

- Para nodos que usan `PipelineEntity`, integrar cerca de
  `src/common/entity.py`.
- Para nodos custom que consumen directo del middleware, usar un wrapper comun
  del callback.
- Evaluar si parte de la integracion puede vivir en
  `src/common/middleware`, pero evitar que el middleware sepa demasiado del
  protocolo interno si eso mezcla responsabilidades.

## Generacion de `message_id`

El gateway debe asignar `message_id` incremental a los primeros mensajes
internos.

Para nodos intermedios:

- Si el mensaje representa la misma unidad original, puede propagarse el mismo
  `message_id`.
- Si el nodo crea una nueva unidad logica, agregacion, join o resultado, debe
  generar un nuevo `message_id` incremental.

Para evitar volver a UUIDs, conviene crear un `MessageIdGenerator` comun y
persistente por emisor.

Clave sugerida para el generador:

```text
emitter_id
source_client_uuid
output_stream
```

Donde `output_stream` puede ser la cola o tipo de mensaje de salida.

## Riesgos y puntos abiertos

- Definir bien que mensajes deben propagar `message_id` y cuales deben generar
  uno nuevo.
- Definir nombres estables para `node_id` en docker compose/env vars.
- Definir ubicacion del volumen persistente para el estado de deduplicacion.
- Revisar todos los nodos que hoy generan `uuid` como `data_id`.
- Asegurar que EOF/control messages tengan politica clara:
  - pueden tener `message_id` y deduplicarse;
  - o pueden tratarse como mensajes de control con una clave/ruta separada.
- Cuidar el orden de operaciones: procesar/publicar primero, persistir rango
  despues, y recien entonces `ack`.

## Siguiente paso recomendado

Antes de implementar, actualizar la rama con los cambios nuevos y luego retomar
con este orden:

1. Agregar `message_id` backward-compatible a `InternalMessage`.
2. Implementar y testear `ProcessedRanges`.
3. Implementar store persistente SQLite.
4. Implementar wrapper comun de deduplicacion.
5. Integrar primero en uno o dos nodos representativos.
6. Extender al resto de nodos y gateway.
7. Agregar pruebas de reinicio/redelivery y duplicados fuera de orden.

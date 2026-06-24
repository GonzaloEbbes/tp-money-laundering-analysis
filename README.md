# Money Laundering Analysis

Trabajo practico con una arquitectura distribuida en Python, Docker Compose y RabbitMQ.

El sistema levanta un cliente, un gateway TCP y una cadena de entidades que consumen y publican mensajes en colas de RabbitMQ:

```text
client
  -> gateway
  -> transfer_data_controller
  -> currency_filter
  -> amount_filter
  -> data_per_bank_redirector
  -> map_max_amount_per_bank
  -> join_max_amount_per_bank
  -> filter_date_window
  -> dynamic_amount_filter
  -> map_average
  -> join_average
  -> map_scatter_gather
  -> aggregation_scatter_gather
  -> join_scatter_gather
  -> pay_format_filter
  -> currency_converter
  -> transfer_counter
  -> gateway
  -> client
```

Cada entidad del pipeline consume desde una cola, imprime:

```text
Soy {entity_type} y recibi un mensaje
```

y reenvia el mensaje a la cola siguiente.

## Requisitos

- Docker
- Docker Compose v2
- `make`

No hace falta crear un entorno virtual local para ejecutar el sistema con Docker.

## Como ejecutarlo

Desde la raiz del repositorio:

```bash
make up
```

Ese comando:

- crea la carpeta local `output/` si no existe;
- construye las imagenes;
- levanta RabbitMQ, gateway, entidades y cliente;
- deja los logs corriendo en primer plano.

El cliente envia el mensaje configurado en `docker-compose.yaml`:

```yaml
MESSAGE=mensaje de prueba
```

La respuesta final se imprime en los logs del contenedor `client`. Cada entidad tambien imprime un mensaje al recibir el payload.

## Comandos utiles

Ver logs:

```bash
make logs
```

Bajar contenedores:

```bash
make down
```

Ejecutar con Docker Compose directamente:

```bash
docker compose -f docker-compose.yaml up --build --remove-orphans
```

## RabbitMQ

RabbitMQ queda expuesto en:

- AMQP: `localhost:5672`
- Management: `http://localhost:15672`

## Entidades

Cada entidad define explicitamente su consumo, publicacion, manejo de EOF,
cierre de recursos y configuracion de logging. Las entidades concretas viven
dentro de `src/entities/`, separadas por categoria:

- `mappers/`: entidades `Map*`.
- `joiners/`: entidades `Join*`.
- `workers/`: filtros, conversores, contadores y agregadores simples.
- `general/`: entidades que no encajan en las categorias anteriores.

Docker levanta cada entidad con su entrypoint y Dockerfile correspondiente.

## Archivos locales

El repositorio ignora archivos generados o locales como:

- `.venv/`
- `__pycache__/`
- `*.pyc`
- `output/`
- `info/`
- `.env`

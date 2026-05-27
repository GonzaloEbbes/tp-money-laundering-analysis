
SCALE_CONFIG = {
    "currency_filter": 4,
    "amount_filter": 2,
    "bank_deduplicator": 4,         
    "data_per_bank_redirector": 1,
    "map_max_amount_per_bank": 3,
    "join_max_amount_per_bank": 1,
    "currency_converter": 4,
    "date_filter": 2,
}

def generate_compose():
    yaml_lines = [
        "services:",
        "  rabbitmq:",
        "    build:",
        "      context: ./src/rabbitmq",
        "      dockerfile: Dockerfile",
        "    container_name: rabbitmq",
        "    environment:",
        "      - RABBITMQ_LOG_LEVELS=error",
        "    healthcheck:",
        "      interval: 5s",
        "      retries: 10",
        "      start_period: 10s",
        "      test: rabbitmq-diagnostics check_port_connectivity",
        "      timeout: 3s",
        "    ports:",
        "      - 5672:5672",
        "      - 15672:15672",
        "",
        "  gateway:",
        "    build:",
        "      context: ./src",
        "      dockerfile: gateway/Dockerfile",
        "    container_name: gateway",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        "      - SERVER_HOST=gateway",
        "      - SERVER_PORT=5678",
        "      - MOM_HOST=rabbitmq",
        "      - OUTPUT_QUEUE=transfer_data_controller_queue",
        "      - INPUT_QUEUE=gateway_results_queue",
        ""
    ]

    all_workers_created = []


    def add_worker(
        name,
        entity_class,
        input_queue,
        output_queue=None,
        extra_env=None,
        queue_index=None,
        scale=1,
        shard_input=False,
        volumes=None,
    ):
        """
        Agrega uno o varios contenedores para un worker.
        Si scale > 1, se añaden múltiples instancias con el mismo nombre base.
        output_queue puede ser string o None.
        """
        for i in range(scale):
            container_name = f"{name}_{i}" if scale > 1 else name
            if shard_input:
                in_queue = f"{input_queue}_{i}"
            else:
                in_queue = f"{input_queue}_{queue_index}" if queue_index is not None else input_queue
            all_workers_created.append(container_name)

            yaml_lines.extend([
                f"  {container_name}:",
                "    build:",
                "      context: ./src",
                "      dockerfile: entities/Dockerfile",
                f"    container_name: {container_name}",
                "    depends_on:",
                "      rabbitmq:",
                "        condition: service_healthy",
                "    environment:",
                "      - PYTHONUNBUFFERED=1",
                "      - PROCESSING_DELAY_SECONDS=0",
                f"      - ENTITY_CLASS={entity_class}",
                "      - MOM_HOST=rabbitmq",
                f"      - INPUT_QUEUE={in_queue}",
            ])
            if output_queue:
                yaml_lines.append(f"      - OUTPUT_QUEUE={output_queue}")
            if extra_env:
                env_vars = extra_env(i) if callable(extra_env) else extra_env
                if isinstance(env_vars, dict):
                    env_vars = [f"{key}={value}" for key, value in env_vars.items()]
                for env_var in env_vars:
                    yaml_lines.append(f"      - {env_var}")
            if volumes:
                yaml_lines.append("    volumes:")
                for volume in volumes:
                    yaml_lines.append(f"      - {volume}")
            yaml_lines.append("")

    add_worker(
        "currency_filter",
        "CurrencyFilter",
        "currency_filter_queue",
        output_queue=None,
        scale=SCALE_CONFIG["currency_filter"]
    )
    
    add_worker(
        "amount_filter", 
        "AmountFilter", 
        "amount_filter_queue", 
        output_queue="gateway_results_queue", 
        scale=SCALE_CONFIG["amount_filter"]
    )

    add_worker(
        "data_per_bank_redirector",
        "DataPerBankRedirector",
        "data_per_bank_redirector_queue",
        output_queue="map_max_amount_per_bank_queue",
        scale=SCALE_CONFIG["data_per_bank_redirector"]
    )

    add_worker(
        "map_max_amount_per_bank",
        "MapMaxAmountPerBank",
        "map_max_amount_per_bank_queue",
        output_queue="join_max_amount_per_bank_queue",
        scale=SCALE_CONFIG["map_max_amount_per_bank"]
    )

    add_worker(
        "join_max_amount_per_bank",
        "JoinMaxAmountPerBank",
        "join_max_amount_per_bank_queue",
        output_queue="gateway_results_queue",
        scale=SCALE_CONFIG["join_max_amount_per_bank"]
    )

    add_worker(
        "bank_deduplicator",
        "BankDeduplicator",
        "bank_deduplicator_queue",
        output_queue="join_max_amount_per_bank_queue",
        scale=SCALE_CONFIG["bank_deduplicator"]
    )

    date_filter_extra = {
        "DATE_FILTER_PREFIX": "date_filter",
        "DATE_FILTER_AMOUNT": SCALE_CONFIG["date_filter"],
        "EOF_CONTROL_EXCHANGE": "date_filter_eof_exchange"
    }
    add_worker("date_filter", "DateFilter", "date_filter_queue", output_queue=None, extra_env=date_filter_extra, scale=SCALE_CONFIG["date_filter"])

    add_worker(
        "currency_converter",
        "CurrencyConverter",
        "currency_converter_queue",
        "pay_format_filter_to_amount_filter_q5_queue",
        scale=SCALE_CONFIG["currency_converter"],
        shard_input=True,
        extra_env=lambda i: [
            "CONVERSION_INPUT_EXCHANGE=pay_format_filter_to_usd_currency_converter_exchange",
            "CONVERSION_QUEUE_PREFIX=currency_converter_queue",
            f"CONVERSION_ROUTING_KEY=conversion.{i}",
            "CONVERSION_PROVIDER=frankfurter",
            "STATIC_CONVERSION_RATES_PATH=/data/static_conversion_rates.json",
            "CONVERSION_AMOUNT_FIELD=amount_paid",
            "CONVERSION_CURRENCY_FIELD=payment_currency",
            "CONVERSION_DATE_FIELD=timestamp",
            "CONVERSION_OUTPUT_AMOUNT_FIELD=amount_paid",
            "FRANKFURTER_MAX_RETRIES=2",
            "FRANKFURTER_RETRY_DELAY_SECONDS=1",
            "FRANKFURTER_MAX_RETRY_DELAY_SECONDS=60",
        ],
        volumes=["./data:/data"],
    )

    yaml_lines.extend([
        "  client:",
        "    build:",
        "      context: ./src",
        "      dockerfile: client/Dockerfile",
        "    container_name: client",
        "    depends_on:",
        "      - gateway"
    ])

    for worker in all_workers_created:
        yaml_lines.append(f"      - {worker}")

    yaml_lines.extend([
        "    volumes:",
        "      - ./data:/data",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        "      - SERVER_HOST=gateway",
        "      - SERVER_PORT=5678",
        "      - DATA_PATH=/data/dataset.csv",
        "      - DATA_PATH_ACCOUNTS=/data/accounts.csv",
        "      - MAX_TRANSACTION_RECORDS=1000",
        ""
    ])

    with open("docker-compose.yaml", "w", encoding="utf-8") as f:
        f.write("\n".join(yaml_lines))

if __name__ == "__main__":
    generate_compose()

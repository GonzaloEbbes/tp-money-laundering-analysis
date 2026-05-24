
SCALE_CONFIG = {
    "transfer_data_controller": 1,
    "currency_filter": 4,
    "amount_filter": 2,
    "bank_deduplicator": 4,         
    "data_per_bank_redirector": 1,
    "map_max_amount_per_bank": 3,
    "join_max_amount_per_bank": 1,
    "conversion_shard_router": 1,
    "currency_converter": 4,
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


    def add_worker(name, entity_class, input_queue, output_queue, is_stateful=False, index=None, extra_env=None, volumes=None):
        container_name = f"{name}_{index}" if is_stateful and index is not None else name
        in_queue = f"{input_queue}_{index}" if is_stateful and index is not None else input_queue

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
            f"      - OUTPUT_QUEUE={output_queue}"
        ])
        
        if entity_class in ["TransferDataController", "JoinMaxAmountPerBank", "DataPerBankRedirector"]:
            yaml_lines.append(f"      - TOTAL_DEDUPLICATORS={SCALE_CONFIG['bank_deduplicator']}")
            yaml_lines.append(f"      - TOTAL_REDUCERS={SCALE_CONFIG['map_max_amount_per_bank']}")
        if extra_env:
            for env_var in extra_env:
                yaml_lines.append(f"      - {env_var}")
        if volumes:
            yaml_lines.append("    volumes:")
            for volume in volumes:
                yaml_lines.append(f"      - {volume}")
        yaml_lines.append("")

    for i in range(SCALE_CONFIG["transfer_data_controller"]):
        add_worker(f"transfer_data_controller_{i}",
                   "TransferDataController",
                   "transfer_data_controller_queue", 
                   "currency_filter_queue")
    
    for i in range(SCALE_CONFIG["currency_filter"]):
        add_worker(f"currency_filter_{i}",
                   "CurrencyFilter",
                   "currency_filter_queue", 
                   "amount_filter_queue")
        
    for i in range(SCALE_CONFIG["amount_filter"]):
        add_worker(f"amount_filter_{i}",
                   "AmountFilter",
                   "amount_filter_queue", 
                   "gateway_results_queue")

    for i in range(SCALE_CONFIG["bank_deduplicator"]):
        add_worker(f"bank_deduplicator_{i}",
                   "BankDeduplicator",
                   f"bank_deduplicator_queue_{i}", 
                   "join_max_amount_per_bank_queue")

    add_worker("data_per_bank_redirector",
               "DataPerBankRedirector", "data_per_bank_redirector_queue", 
               "map_max_amount_per_bank_queue")
    
    for i in range(SCALE_CONFIG["map_max_amount_per_bank"]):
        add_worker("map_max_amount_per_bank",
                   "MapMaxAmountPerBank", "map_max_amount_per_bank_queue", 
                   "join_max_amount_per_bank_queue", is_stateful=True, index=i)

    add_worker("join_max_amount_per_bank", "JoinMaxAmountPerBank", 
               "join_max_amount_per_bank_queue", "gateway_results_queue")

    for i in range(SCALE_CONFIG["conversion_shard_router"]):
        add_worker(f"conversion_shard_router_{i}",
                   "ConversionShardRouter",
                   "conversion_shard_router_queue",
                   "currency_converter_queue",
                   extra_env=[
                       f"TOTAL_CONVERSION_WORKERS={SCALE_CONFIG['currency_converter']}",
                       "CONVERSION_CONVERTER_QUEUE_PREFIX=currency_converter_queue",
                   ])

    for i in range(SCALE_CONFIG["currency_converter"]):
        add_worker("currency_converter",
                   "CurrencyConverter",
                   "currency_converter_queue",
                   "gateway_results_queue",
                   is_stateful=True,
                   index=i,
                   extra_env=[
                       "CONVERSION_PROVIDER=frankfurter",
                       "STATIC_CONVERSION_RATES_PATH=/data/static_conversion_rates.json",
                       "CONVERSION_AMOUNT_FIELD=AmountPaid",
                       "CONVERSION_CURRENCY_FIELD=PaymentCurrency",
                       "CONVERSION_DATE_FIELD=Timestamp",
                       "CONVERSION_OUTPUT_AMOUNT_FIELD=AmountPaidUSD",
                   ],
                   volumes=["./data:/data"])

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
        "      - MESSAGE=mensaje de prueba",
        "      - DATA_PATH=/data/dataset.csv",
        ""
    ])

    with open("docker-compose.yaml", "w", encoding="utf-8") as f:
        f.write("\n".join(yaml_lines))


if __name__ == "__main__":
    generate_compose()

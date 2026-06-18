
configurations = {
    1: {
        "date_filter": 12,
        "usd_filter_q1q2": 15,
        "amount_filter_q1": 3,
        "usd_filter_q3": 3,
        "usd_filter_q4": 12,
        "pay_format_filter": 12,
        "amount_filter_q3": 1,
        "amount_filter_q5": 3,
        "scather_gather_mapper": 3,
        "scather_gather_aggregator": 3,
        "scather_gather_pair_joiner": 3,
        "scather_gather_joiner": 3,
        "currency_converter": 4,
        "average_per_pay_format_mapper": 2,
        "average_per_pay_format_joiner": 1,
        "data_per_bank_redirector": 8,
        "bank_filter": 6,
        "map_max_amount_per_bank": 12,
        "join_max_amount_per_bank": 4,
    },
    2: {
        "date_filter": 13,
        "usd_filter_q1q2": 16,
        "amount_filter_q1": 4,
        "usd_filter_q3": 6,
        "usd_filter_q4": 13,
        "pay_format_filter": 13,
        "amount_filter_q3": 5,
        "amount_filter_q5": 4,
        "scather_gather_mapper": 4,
        "scather_gather_aggregator": 4,
        "scather_gather_pair_joiner": 4,
        "scather_gather_joiner": 4,
        "currency_converter": 5,
        "average_per_pay_format_mapper": 4,
        "average_per_pay_format_joiner": 1,
        "data_per_bank_redirector": 9,
        "bank_filter": 7,
        "map_max_amount_per_bank": 11,
        "join_max_amount_per_bank": 3,
    }
}

def with_middleware_impl_env(lines):
    result = []
    for line in lines:
        result.append(line)
        if line == "      - PYTHONUNBUFFERED=1":
            result.append("      - MIDDLEWARE_IMPL=${MIDDLEWARE_IMPL:-rabbitmq}")
    return result

# pone aquellas lineas que son iguales siempre
def set_rabbitmq():
    return [
        "services:",
        "  rabbitmq:",
        "    build:",
        "      context: ./src/rabbitmq",
        "      dockerfile: Dockerfile",
        "    container_name: rabbitmq",
        "    environment:",
        "      - RABBITMQ_MAX_UNACKED_MESSAGES=1",
        "      - RABBITMQ_HEARTBEAT=0",
        "      - RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS=300",
        "      - RABBITMQ_BATCH_MAX_MESSAGES=100000",
        "      - RABBITMQ_BATCH_MAX_SECONDS=2",
        "      - RABBITMQ_BATCH_HEADER=x-middleware-batch",
        "      - RABBITMQ_BATCH_HEADER_VALUE=v1",
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
    ]

def set_gateway_config(bank_filters_amount, log_level):
    return [
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
        f"      - LOG_LEVEL={log_level}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=gateway_results_queue",
        "      - BANK_DEDUPLICATOR_QUEUE=bank_deduplicator_queue",
        "      - CURRENCY_FILTER_QUEUE=currency_filter_queue",
        "      - DATE_FILTER_QUEUE=date_filter_queue",
        f"      - BANK_FILTERS_AMOUNT={bank_filters_amount}",
        "      - BANK_EXCHANGE=bank_exchange",
        "      - BANK_ROUTING_KEY_PREFIX=bank_partition",
        "",
    ]

def set_date_filter_config(id,total, log_level):
    return [
        f"  date_filter_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/date_filter/Dockerfile",
        f"    container_name: date_filter_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=date_filter_queue",
        "      - DATE_FILTER_PREFIX=date_filter",
        f"      - DATE_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=date_filter_eof_control_exchange",
        "      - USD_FILTER_Q3_QUEUE=date_filter_to_usd_filter_q3_queue",
        "      - USD_FILTER_Q4_QUEUE=date_filter_to_usd_filter_q4_queue",
        "      - PAY_FORMAT_FILTER_QUEUE=date_filter_to_pay_format_filter_queue",
        "",
    ]

def set_usd_filter_q1q2_config(id,total, log_level):
    return [ 
        f"  usd_filter_q1q2_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/usd_filter_q1q2/Dockerfile",
        f"    container_name: usd_filter_q1q2_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=currency_filter_queue",
        "      - USD_FILTER_PREFIX=usd_filter_q1q2",
        f"      - USD_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=usd_filter_q1q2_eof_control_exchange",
        "      - AMOUNT_FILTER_Q1_QUEUE=usd_filter_q1q2_to_amount_filter_q1_queue",
        "      - DATA_PER_BANK_SHUFFLER_QUEUE=usd_filter_q1q2_to_data_per_bank_shuffler_queue",
        "",
    ]

def set_amount_filter_q1_config(id,total, log_level):
    return [
        f"  amount_filter_q1_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/amount_filter_q1/Dockerfile",
        f"    container_name: amount_filter_q1_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=usd_filter_q1q2_to_amount_filter_q1_queue",
        "      - AMOUNT_FILTER_PREFIX=amount_filter_q1",
        f"      - AMOUNT_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=amount_filter_q1_eof_control_exchange",
        "      - GATEWAY_FINAL_QUERY_QUEUE=gateway_results_queue",
        "",
    ]

def set_usd_filter_q3_config(id,total, log_level):
    return [
        f"  usd_filter_q3_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/usd_filter_q3/Dockerfile",
        f"    container_name: usd_filter_q3_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=date_filter_to_usd_filter_q3_queue",
        "      - USD_FILTER_PREFIX=usd_filter_q3",
        f"      - USD_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=usd_filter_q3_eof_control_exchange",
        "      - AMOUNT_FILTER_Q3_QUEUE=usd_filter_q3_to_amount_filter_q3_queue",
        "",
    ]

def set_usd_filter_q4_config(id,total, log_level):
    return [
        f"  usd_filter_q4_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/usd_filter_q4/Dockerfile",
        f"    container_name: usd_filter_q4_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=date_filter_to_usd_filter_q4_queue",
        "      - USD_FILTER_PREFIX=usd_filter_q4",
        f"      - USD_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=usd_filter_q4_eof_control_exchange",
        "      - AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE=usd_filter_q4_to_average_per_pay_format_mapper_queue",
        "      - SCATHER_GATHER_QUEUE=usd_filter_q4_to_scatter_gather_queue",
        "",
    ]

def set_pay_format_filter_config(id,total,total_usd_currency_converters, log_level):
    return [
        f"  pay_format_filter_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/pay_format_filter/Dockerfile",
        f"    container_name: pay_format_filter_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=date_filter_to_pay_format_filter_queue",
        "      - PAY_FORMAT_FILTER_PREFIX=pay_format_filter",
        f"      - PAY_FORMAT_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=pay_format_filter_eof_control_exchange",
        "      - CONVERSION_EXCHANGE=pay_format_filter_to_usd_currency_converter_exchange",
        "      - CONVERSION_QUEUE_PREFIX=currency_converter_queue",
        "      - CONVERSION_ROUTING_KEY_PREFIX=conversion",
        f"      - TOTAL_CONVERSION_WORKERS={total_usd_currency_converters}",
        "      - AMOUNT_FILTER_Q5_QUEUE=pay_format_filter_to_amount_filter_q5_queue",
        "",
    ]

def set_average_per_pay_format_mapper_config(id,total, log_level):
    return [
        f"  average_per_pay_format_mapper_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/average_per_pay_format/average_per_pay_format_mapper/Dockerfile",
        f"    container_name: average_per_pay_format_mapper_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - MOM_HOST=rabbitmq",
        f"      - ID={id}",
        "      - INPUT_QUEUE=usd_filter_q4_to_average_per_pay_format_mapper_queue",
        "      - OUTPUT_QUEUE=average_per_pay_format_mapper_to_average_per_pay_format_joiner_queue",
        f"      - MAPPER_FILTER_PREFIX=average_per_pay_format_mapper",
        f"      - MAPPER_FILTER_AMOUNT={total}",
        f"      - EOF_CONTROL_EXCHANGE=average_per_pay_format_mapper_eof_control_exchange",
        f"      - EXPECTED_INPUT_EOFS=1",
        "",
    ]

def set_average_pay_format_joiner_config(id, log_level):
    return [
        f"  average_per_pay_format_joiner_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/average_per_pay_format/average_per_pay_format_joiner/Dockerfile",
        f"    container_name: average_per_pay_format_joiner_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - MOM_HOST=rabbitmq",
        f"      - ID={id}",
        f"      - JOINER_PREFIX=average_per_pay_format_joiner",
        f"      - JOINER_AMOUNT=1",
        f"      - EOF_CONTROL_EXCHANGE=average_per_pay_format_joiner_eof_control_exchange",
        "      - INPUT_QUEUE=average_per_pay_format_mapper_to_average_per_pay_format_joiner_queue",
        f"      - EXPECTED_INPUT_EOFS=1",
        "      - AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE=average_per_pay_format_joiner_to_amount_filter_q3_exchange",
    ]

def set_amount_filter_q3_config(id,total, log_level):
    return [
        f"  amount_filter_q3_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/amount_filter_q3/Dockerfile",
        f"    container_name: amount_filter_q3_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=usd_filter_q3_to_amount_filter_q3_queue",
        "      - AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE=average_per_pay_format_joiner_to_amount_filter_q3_exchange",
        "      - AMOUNT_FILTER_PREFIX=amount_filter_q3",
        f"      - AMOUNT_FILTER_AMOUNT={total}",
        "      - EOF_CONTROL_EXCHANGE=amount_filter_q3_eof_control_exchange",
        "      - GATEWAY_FINAL_QUERY_QUEUE=gateway_results_queue",
        "",
    ]

def set_amount_filter_q5_config(id,total,total_usd_currency_converters, log_level):
    return [
        f"  amount_filter_q5_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/amount_filter_q5/Dockerfile",
        f"    container_name: amount_filter_q5_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=pay_format_filter_to_amount_filter_q5_queue",
        "      - AMOUNT_FILTER_PREFIX=amount_filter_q5",
        f"      - AMOUNT_FILTER_AMOUNT={total}",
        f"      - EXPECTED_INPUT_EOFS={total_usd_currency_converters+1}",
        "      - EOF_CONTROL_EXCHANGE=amount_filter_q5_eof_control_exchange",
        "      - GATEWAY_FINAL_QUERY_QUEUE=gateway_results_queue",
        "",
    ]

def set_scather_gather_mapper_config(id,total_mappers,total_aggregators, log_level):
    return [
        f"  scather_gather_mapper_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/scather_gather/scather_gather_mapper/Dockerfile",
        f"    container_name: scather_gather_mapper_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=usd_filter_q4_to_scatter_gather_queue",
        "      - SCATHER_GATHER_MAPPER_PREFIX=scather_gather_mapper",
        f"      - SCATHER_GATHER_MAPPER_AMOUNT={total_mappers}",
        "      - EOF_CONTROL_EXCHANGE=scather_gather_mapper_eof_control_exchange",
        f"      - SCATHER_GATHER_AGGREGATOR_AMOUNT={total_aggregators}",
        "      - SCATHER_GATHER_AGGREGATOR_PREFIX=scather_gather_aggregator",
        "",
    ]

def set_scather_gather_aggregator_config(id,total_mappers,total_pair_joiners, log_level):
    return [
        f"  scather_gather_aggregator_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/scather_gather/scather_gather_aggregator/Dockerfile",
        f"    container_name: scather_gather_aggregator_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        f"      - SCATHER_GATHER_MAPPER_AMOUNT={total_mappers}",
        "      - SCATHER_GATHER_AGG_PREFIX=scather_gather_aggregator",
        f"      - SCATHER_GATHER_PAIR_JOINER_AMOUNT={total_pair_joiners}",
        "      - SCATHER_GATHER_PAIR_JOINER_PREFIX=scather_gather_pair_joiner",
        "",
    ]

def set_scather_gather_pair_joiner_config(id,total_aggregators,total_joiners, log_level):
    return [
        f"  scather_gather_pair_joiner_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/scather_gather/scather_gather_pair_joiner/Dockerfile",
        f"    container_name: scather_gather_pair_joiner_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        f"      - SCATHER_GATHER_AGGREGATOR_AMOUNT={total_aggregators}",
        "      - SCATHER_GATHER_PAIR_JOINER_PREFIX=scather_gather_pair_joiner",
        f"      - SCATHER_GATHER_JOINER_AMOUNT={total_joiners}",
        "      - SCATHER_GATHER_JOINER_PREFIX=scather_gather_joiner",
        "",
    ]

def set_scather_gather_joiner_config(id,total_pair_joiners,total_joiners, log_level):
    return [
        f"  scather_gather_joiner_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/scather_gather/scather_gather_joiner/Dockerfile",
        f"    container_name: scather_gather_joiner_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        f"      - ID={id}",
        "      - MOM_HOST=rabbitmq",
        f"      - SCATHER_GATHER_PAIR_JOINER_AMOUNT={total_pair_joiners}",
        "      - SCATHER_GATHER_JOIN_PREFIX=scather_gather_joiner",
        "      - EOF_CONTROL_EXCHANGE=scather_gather_joiner_eof_control_exchange",
        f"      - SCATHER_GATHER_JOINER_AMOUNT={total_joiners}",
        "      - SCATHER_GATHER_JOINER_PREFIX=scather_gather_joiner",
        "      - GATEWAY_FINAL_QUERY_QUEUE=gateway_results_queue",
        "",
    ]

def set_currency_converter_config(id,total, log_level):
    return [
        f"  currency_converter_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/Dockerfile",
        f"    container_name: currency_converter_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - PROCESSING_DELAY_SECONDS=0",
        "      - ENTITY_CLASS=CurrencyConverter",
        "      - MOM_HOST=rabbitmq",
        f"      - INPUT_QUEUE=currency_converter_queue_{id}",
        "      - OUTPUT_QUEUE=pay_format_filter_to_amount_filter_q5_queue",
        "      - CONVERSION_INPUT_EXCHANGE=pay_format_filter_to_usd_currency_converter_exchange",
        f"      - CONVERSION_ROUTING_KEY=conversion.{id}",
        "      - CONVERSION_PROVIDER=${CONVERSION_PROVIDER:-frankfurter}",
        "      - STATIC_CONVERSION_RATES_PATH=/data/static_conversion_rates.json",
        "      - CONVERSION_AMOUNT_FIELD=amount_paid",
        "      - CONVERSION_CURRENCY_FIELD=payment_currency",
        "      - CONVERSION_DATE_FIELD=timestamp",
        "      - CONVERSION_OUTPUT_AMOUNT_FIELD=amount_paid",
        "      - FRANKFURTER_MAX_RETRIES=10",
        "      - FRANKFURTER_RETRY_DELAY_SECONDS=1",
        "      - FRANKFURTER_MAX_RETRY_DELAY_SECONDS=60",
        "    volumes:",
        "      - ./data:/data",
        "",
    ]

def set_client(config, log_level):
    cliente = []
    cliente += [
        "  client:",
        "    build:",
        "      context: ./src",
        "      dockerfile: client/Dockerfile",
        "    container_name: client",
        "    depends_on:",
        "      - gateway",
    ]
    for service in config.keys():
        for i in range(config[service]):
            cliente += [f"      - {service}_{i}"]
            
    cliente += [
        "    volumes:",
        "      - ./data:/data",
        "      - ./output:/output",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - SERVER_HOST=gateway",
        "      - SERVER_PORT=5678",
        "      - MESSAGE=mensaje de prueba",
        "      - DATA_PATH=${DATA_PATH:-/data/dataset.csv}",
        "      - DATA_PATH_ACCOUNTS=${DATA_PATH_ACCOUNTS:-/data/accounts.csv}",
        "      - EXPECTED_RESULT_EOFS=${EXPECTED_RESULT_EOFS:-5}",
        "      - RESULTS_DIR=${RESULTS_DIR:-/output/results}",
        "      - RESULTS_WAIT_LOG_INTERVAL=${RESULTS_WAIT_LOG_INTERVAL:-60}",
        "      - RESULTS_IDLE_TIMEOUT=${RESULTS_IDLE_TIMEOUT:-0}",
        "",
    ]
    return cliente

def set_data_per_bank_redirector_config(id,total_redirectors,total_mappers, log_level):
    return [
        f"  data_per_bank_redirector_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/general/data_per_bank_redirector/Dockerfile",
        f"    container_name: data_per_bank_redirector_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - PROCESSING_DELAY_SECONDS=0",
        f"      - ID={id}",
        f"      - DATA_PER_BANK_REDIRECTOR_AMOUNT={total_redirectors}",
        f"      - TOTAL_MAPPERS={total_mappers}",
        "      - MOM_HOST=rabbitmq",
        "      - INPUT_QUEUE=usd_filter_q1q2_to_data_per_bank_shuffler_queue",
        "      - EXCHANGE_NAME=map_max_exchange",
        "      - OUTPUT_ROUTING_KEY_PREFIX=map_partition",
        "      - EOF_CONTROL_EXCHANGE=dpb_control_exchange",
        "",
    ]

def set_bank_filter_config(id,total_bank_filters,total_join_max_amount_per_bank, log_level):
    return [
        f"  bank_filter_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/filters/bank_filter/Dockerfile",
        f"    container_name: bank_filter_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - PROCESSING_DELAY_SECONDS=0",
        f"      - ID={id}",
        f"      - BANK_FILTERS_AMOUNT={total_bank_filters}",
        "      - MOM_HOST=rabbitmq",
        "      - BANK_EXCHANGE=bank_exchange",
        "      - BANK_ROUTING_KEY_PREFIX=bank_partition",
        "      - JOIN_EXCHANGE=query2_join_exchange",
        f"      - JOIN_AMOUNT={total_join_max_amount_per_bank}",
        "      - JOIN_ROUTING_KEY_PREFIX=join_partition",
        "      - EOF_CONTROL_EXCHANGE=bank_filter_control_exchange",
        "",
    ]

def set_map_max_amount_per_bank_config(id,total_map_amount_filters,total_join_amount_per_bank, log_level):
    return [
        f"  map_max_amount_per_bank_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/mappers/map_max_amount_per_bank/Dockerfile",
        f"    container_name: map_max_amount_per_bank_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - PROCESSING_DELAY_SECONDS=0",
        f"      - ID={id}",
        f"      - MAP_AMOUNT={total_map_amount_filters}",
        "      - MOM_HOST=rabbitmq",
        "      - MAP_MAX_EXCHANGE=map_max_exchange",
        "      - MAP_MAX_ROUTING_KEY_PREFIX=map_max_partition",
        "      - JOIN_EXCHANGE=query2_join_exchange",
        f"      - JOIN_AMOUNT={total_join_amount_per_bank}",
        "      - JOIN_ROUTING_KEY_PREFIX=join_partition",
        "      - EOF_CONTROL_EXCHANGE=map_control_exchange",
        "",
    ]

def set_join_max_amount_per_bank_config(id,total_join_amount_filters,total_map_amount_filters, log_level):
    return [
        f"  join_max_amount_per_bank_{id}:",
        "    build:",
        "      context: ./src",
        "      dockerfile: entities/joiners/join_max_amount_per_bank/Dockerfile",
        f"    container_name: join_max_amount_per_bank_{id}",
        "    depends_on:",
        "      rabbitmq:",
        "        condition: service_healthy",
        "    environment:",
        "      - PYTHONUNBUFFERED=1",
        f"      - LOG_LEVEL={log_level}",
        "      - PROCESSING_DELAY_SECONDS=0",
        f"      - ID={id}",
        f"      - JOIN_AMOUNT={total_join_amount_filters}",
        f"      - MAP_AMOUNT={total_map_amount_filters}",
        "      - MOM_HOST=rabbitmq",
        "      - JOIN_EXCHANGE=query2_join_exchange",
        "      - JOIN_ROUTING_KEY_PREFIX=join_partition",
        "      - OUTPUT_QUEUE=gateway_results_queue",
        "      - EOF_CONTROL_EXCHANGE=join_control_exchange",
        "",
    ]

def generate_compose(config_id,log_level):
    if config_id not in configurations or log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ValueError("Configuración no encontrada")

    config = configurations[config_id]
    yaml_lines = []
    yaml_lines += set_rabbitmq()
    yaml_lines += set_gateway_config(config["bank_filter"], log_level)

    for i in range(config["date_filter"]):
        yaml_lines += set_date_filter_config(i, config["date_filter"], log_level)
    for i in range(config["usd_filter_q1q2"]):
        yaml_lines += set_usd_filter_q1q2_config(i, config["usd_filter_q1q2"], log_level)
    for i in range(config["amount_filter_q1"]):
        yaml_lines += set_amount_filter_q1_config(i, config["amount_filter_q1"], log_level)
    for i in range(config["usd_filter_q3"]):
        yaml_lines += set_usd_filter_q3_config(i, config["usd_filter_q3"], log_level)
    for i in range(config["usd_filter_q4"]):
        yaml_lines += set_usd_filter_q4_config(i, config["usd_filter_q4"], log_level)
    for i in range(config["average_per_pay_format_mapper"]):
        yaml_lines += set_average_per_pay_format_mapper_config(i, config["average_per_pay_format_mapper"], log_level)
    for i in range(config["average_per_pay_format_joiner"]):
        yaml_lines += set_average_pay_format_joiner_config(i, log_level)
    for i in range(config["amount_filter_q3"]):
        yaml_lines += set_amount_filter_q3_config(i, config["amount_filter_q3"], log_level)
    for i in range(config["pay_format_filter"]):
        yaml_lines += set_pay_format_filter_config(i, config["pay_format_filter"], config["currency_converter"], log_level)
    for i in range(config["amount_filter_q5"]):
        yaml_lines += set_amount_filter_q5_config(i, config["amount_filter_q5"], config["currency_converter"], log_level)
    for i in range(config["scather_gather_mapper"]):
        yaml_lines += set_scather_gather_mapper_config(i, config["scather_gather_mapper"], config["scather_gather_aggregator"], log_level)
    for i in range(config["scather_gather_aggregator"]):
        yaml_lines += set_scather_gather_aggregator_config(i, config["scather_gather_mapper"], config["scather_gather_pair_joiner"], log_level)
    for i in range(config["scather_gather_pair_joiner"]):
        yaml_lines += set_scather_gather_pair_joiner_config(i, config["scather_gather_aggregator"], config["scather_gather_joiner"], log_level)
    for i in range(config["scather_gather_joiner"]):
        yaml_lines += set_scather_gather_joiner_config(i, config["scather_gather_pair_joiner"], config["scather_gather_joiner"], log_level)
    for i in range(config["currency_converter"]):
        yaml_lines += set_currency_converter_config(i, config["currency_converter"], log_level)
    for i in range(config["data_per_bank_redirector"]):
        yaml_lines += set_data_per_bank_redirector_config(i, config["data_per_bank_redirector"], config["map_max_amount_per_bank"], log_level)
    for i in range(config["bank_filter"]):
        yaml_lines += set_bank_filter_config(i, config["bank_filter"], config["join_max_amount_per_bank"], log_level)
    for i in range(config["map_max_amount_per_bank"]):
        yaml_lines += set_map_max_amount_per_bank_config(i, config["map_max_amount_per_bank"],config["join_max_amount_per_bank"], log_level)
    for i in range(config["join_max_amount_per_bank"]):
        yaml_lines += set_join_max_amount_per_bank_config(i, config["join_max_amount_per_bank"], config["map_max_amount_per_bank"], log_level)
    yaml_lines += set_client(config, log_level)
    yaml_lines = with_middleware_impl_env(yaml_lines)

    with open("docker-compose.yaml", "w", encoding="utf-8") as f:
        f.write("\n".join(yaml_lines))

if __name__ == "__main__":
    import sys

    # Uso: python3 generar_compose.py [config_id] [log_level]
    if len(sys.argv) > 4:
        print("Usage: python3 generar_compose.py [config_id] [log_level]")
        sys.exit(1)

    if len(sys.argv) == 3:
        try:
            config_id = int(sys.argv[1])
            log_level = sys.argv[2].upper()
        except ValueError:
            print(f"Invalid config id: {sys.argv[1]} or log level: {sys.argv[2]}")
            print("Usage: python3 generar_compose.py [config_id] [log_level]")
            sys.exit(1)
    else:
        config_id = 1
        log_level = "INFO"

    generate_compose(config_id,log_level)

import logging
import os
import socket
import time
import csv
import json
from decimal import Decimal

from frankfurter_api import FrankfurterClient, FrankfurterApiError

SERVER_HOST = os.environ.get("SERVER_HOST")
SERVER_PORT = os.environ.get("SERVER_PORT")
MESSAGE = os.environ.get("MESSAGE", "mensaje de prueba")
DATA_PATH_TRANSACTIONS = os.environ.get("DATA_PATH", "/data/dataset.csv")
DATA_PATH_ACCOUNTS = os.environ.get("DATA_PATH_ACCOUNTS", "/data/accounts.csv")
CLIENT_MODE = os.environ.get("CLIENT_MODE", "pipeline")


def run_frankfurter_smoke():
    amount = os.environ.get("FRANKFURTER_AMOUNT", "10")
    base_currency = os.environ.get("FRANKFURTER_BASE", "EUR")
    quote_currency = os.environ.get("FRANKFURTER_QUOTE", "USD")
    date = os.environ.get("FRANKFURTER_DATE", "2022-09-01")
    base_url = os.environ.get("FRANKFURTER_API_URL", "https://api.frankfurter.app")
    api_version = os.environ.get("FRANKFURTER_API_VERSION", "v1")
    timeout_seconds = float(os.environ.get("FRANKFURTER_TIMEOUT_SECONDS", "5"))
    user_agent = os.environ.get(
        "FRANKFURTER_USER_AGENT",
        "tp-money-laundering-analysis/1.0",
    )

    client = FrankfurterClient(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        api_version=api_version,
        user_agent=user_agent,
    )
    rate = client.get_rate(base_currency, quote_currency, date)
    converted = Decimal(str(amount)) * rate

    result = {
        "source": "frankfurter",
        "api_url": base_url,
        "api_version": api_version,
        "date": date,
        "amount": str(amount),
        "base": base_currency,
        "quote": quote_currency,
        "rate": str(rate),
        "converted": str(converted),
    }
    print(json.dumps(result), flush=True)

def main():
    logging.basicConfig(level=logging.INFO)

    if CLIENT_MODE == "frankfurter_smoke":
        try:
            run_frankfurter_smoke()
        except FrankfurterApiError:
            logging.exception("Frankfurter smoke test failed")
            raise
        return

    if not SERVER_HOST or not SERVER_PORT:
        raise RuntimeError("SERVER_HOST and SERVER_PORT are required in pipeline mode")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        for _ in range(30):
            try:
                client_socket.connect((SERVER_HOST, int(SERVER_PORT)))
                break
            except OSError:
                time.sleep(1)
        else:
            logging.error("No se pudo conectar al Gateway")
            return

        logging.info("Iniciando streaming del CSV")

        with open(DATA_PATH_ACCOUNTS, mode='r', encoding='utf-8-sig') as f:
            clean_headers = ["BankName", "BankID", "AccountNumber", "EntityID", "EntityName"]
            next(f, None)
            reader = csv.DictReader(f, fieldnames=clean_headers)
            for indice, row in enumerate(reader):
                if (indice%100000) == 0:
                    print(f"Procesando fila {indice} de accounts.csv", flush=True)
                row["query_id"] = "query_2_accounts"
                client_socket.sendall((json.dumps(row) + "\n").encode("utf-8"))
        
        client_socket.sendall((json.dumps({"type": "EOF", "query_id": "query_2_accounts"}) + "\n").encode("utf-8"))
        logging.info("Streaming de accounts.csv finalizado. Iniciando streaming de transacciones...")

        with open(DATA_PATH_TRANSACTIONS, mode='r', encoding='utf-8-sig') as f:
            clean_headers = [
                "Timestamp", "FromBank", "AccountOrigin", "ToBank", 
                "AccountDestiny", "AmountReceived", "ReceivingCurrency", 
                "AmountPaid", "PaymentCurrency", "PaymentFormat"
            ]
            
            next(f, None)
            
            reader = csv.DictReader(f, fieldnames=clean_headers)
            for indice, row in enumerate(reader):
                if indice >= 1000:
                    break
                clean_row = {key.strip(): value for key, value in row.items() if key is not None}

                if clean_row.get("AmountReceived"):
                    clean_row["AmountReceived"] = float(clean_row["AmountReceived"])

                q1_row = clean_row.copy()
                q1_row["query_id"] = "transactions"
                client_socket.sendall((json.dumps(q1_row) + "\n").encode("utf-8"))


        client_socket.sendall((json.dumps({"type": "EOF", "query_id": "transactions"}) + "\n").encode("utf-8"))
        client_socket.shutdown(socket.SHUT_WR)
        logging.info("Streaming finalizado. Esperando respuestas...")

        buffer = ""
        while True:
            data = client_socket.recv(4096).decode("utf-8")
            if not data:
                break

            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    print(f"Resultado filtrado recibido: {line.strip()}", flush=True)

    logging.info("Conexión cerrada por el Gateway. Proceso terminado.")


if __name__ == "__main__":
    main()

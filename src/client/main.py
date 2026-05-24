import logging
import os
import socket
import time
import csv
import json

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MESSAGE = os.environ.get("MESSAGE", "mensaje de prueba")
DATA_PATH_TRANSACTIONS = os.environ.get("DATA_PATH", "/data/dataset.csv")
DATA_PATH_ACCOUNTS = os.environ.get("DATA_PATH_ACCOUNTS", "/data/accounts.csv")

def main():
    logging.basicConfig(level=logging.INFO)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        for _ in range(30):
            try:
                client_socket.connect((SERVER_HOST, SERVER_PORT))
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

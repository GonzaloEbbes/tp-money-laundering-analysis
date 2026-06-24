FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/max_amount_per_bank/data_per_bank_redirector .

ENTRYPOINT ["python3", "main.py"]

FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/workers/currency_converter .

ENTRYPOINT ["python3", "main.py"]

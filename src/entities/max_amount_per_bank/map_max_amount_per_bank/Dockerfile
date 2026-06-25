FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/max_amount_per_bank/map_max_amount_per_bank .

ENTRYPOINT ["python3", "main.py"]

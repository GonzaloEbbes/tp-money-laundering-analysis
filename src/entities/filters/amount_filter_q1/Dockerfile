FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/filters/amount_filter_q1 .

ENTRYPOINT ["python3", "main.py"]

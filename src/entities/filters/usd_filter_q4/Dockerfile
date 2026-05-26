FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/filters/usd_filter_q4 .

ENTRYPOINT ["python3", "main.py"]

FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/filters/pay_format_filter .

ENTRYPOINT ["python3", "main.py"]

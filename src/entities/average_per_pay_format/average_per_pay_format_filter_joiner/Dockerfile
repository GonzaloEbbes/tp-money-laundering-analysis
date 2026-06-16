FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/average_per_pay_format/average_per_pay_format_joiner.py .

ENTRYPOINT ["python3", "main.py"]

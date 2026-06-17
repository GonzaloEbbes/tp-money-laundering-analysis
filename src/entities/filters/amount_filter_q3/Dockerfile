FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/filters/amount_filter_q3 .

# Create data_pending directory for storing CSV files
RUN mkdir -p /data_pending && chmod 777 /data_pending

ENTRYPOINT ["python3", "main.py"]

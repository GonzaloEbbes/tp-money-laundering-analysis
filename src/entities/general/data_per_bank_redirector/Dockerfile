FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/general/data_per_bank_redirector .

ENTRYPOINT ["python3", "main.py"]

FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY entities/filters/scather_gather/scatter_gather_mapper .

ENTRYPOINT ["python3", "main.py"]

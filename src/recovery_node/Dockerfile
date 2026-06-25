FROM python:3-alpine

RUN apk add --no-cache docker-cli docker-cli-compose
RUN pip3 install pika

COPY common ./common
COPY recovery_node .

ENTRYPOINT ["python3", "main.py"]
FROM rabbitmq:3.9.16-management-alpine

RUN apk update && apk add --no-cache curl

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
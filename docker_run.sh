#!/bin/bash
echo "Starting rabbitmq management console on: http://localhost:15672"
docker run --rm -it -p 15672:15672 -p 5672:5672 rabbitmq_amqp1:latest

FROM rabbitmq:3-management
RUN echo '[rabbitmq_management,rabbitmq_amqp1_0].' > enabled_plugins \
 && rabbitmq-plugins enable rabbitmq_amqp1_0

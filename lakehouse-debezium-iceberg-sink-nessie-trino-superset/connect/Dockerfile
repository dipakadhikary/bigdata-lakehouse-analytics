FROM debezium/connect:2.7.3.Final

USER root

RUN mkdir -p /kafka/connect/iceberg

COPY plugins/ /kafka/connect/iceberg/

RUN chmod -R 755 /kafka/connect/iceberg

RUN chown -R kafka:kafka /kafka/connect/iceberg

USER kafka

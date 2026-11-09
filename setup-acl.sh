#!/bin/bash
# setup-acls.sh
# Настройка ACL для topic-1 и topic-2

BOOTSTRAP="localhost:9093"
CONFIG="certificates/brokers/kafka-1/client-ssl-local.properties"
PRINCIPAL="User:CN=kafka-1,L=Moscow,OU=Practice,O=Yandex,C=RU"

echo "Настройка ACL для topic-1..."
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONFIG \
  --add --allow-principal "$PRINCIPAL" --producer --topic topic-1

kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONFIG \
  --add --allow-principal "$PRINCIPAL" --consumer --topic topic-1 --group test-group

echo "Настройка ACL для topic-2 (только продюсер)..."
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONFIG \
  --add --allow-principal "$PRINCIPAL" --producer --topic topic-2

echo "Готово. Текущие ACL:"
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONFIG --list
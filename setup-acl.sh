#!/bin/bash
# setup-acl.sh
# Создание топиков topic-1/topic-2 и настройка ACL.
#
# Запуск с хоста (нужен только Docker):  ./setup-acl.sh
# Скрипт перезапускает сам себя в служебном контейнере kafka-cli (см. docker-compose.yaml),
# поэтому локально устанавливать Kafka CLI не требуется.
#
# Права:
#   producer - запись в topic-1 и topic-2
#   consumer - чтение topic-1 (группа test-group), к topic-2 доступа нет
#   kafka-admin - суперпользователь, от его имени выполняется этот скрипт

set -euo pipefail

if [ -z "${KAFKA_CLI_CONTAINER:-}" ]; then
  cd "$(dirname "$0")"
  # Git Bash на Windows иначе переписывает пути вида /opt/... в C:/Program Files/Git/opt/...
  export MSYS_NO_PATHCONV=1
  exec docker compose --profile tools run --rm -e KAFKA_CLI_CONTAINER=1 kafka-cli
fi

BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CONFIG="/etc/kafka/clients/kafka-admin/client-ssl.properties"
DN_SUFFIX="L=Moscow,OU=Practice,O=Yandex,C=RU"
PRODUCER="User:CN=producer,$DN_SUFFIX"
CONSUMER="User:CN=consumer,$DN_SUFFIX"
CONSUMER_GROUP="test-group"
BROKERS_EXPECTED=3

echo "Ожидание готовности $BROKERS_EXPECTED брокеров..."
for attempt in $(seq 1 60); do
  brokers=$(kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" 2>/dev/null \
    | grep -c '(id:' || true)
  if [ "$brokers" -ge "$BROKERS_EXPECTED" ]; then
    echo "Доступно брокеров: $brokers"
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Брокеры не поднялись (доступно: $brokers)" >&2
    exit 1
  fi
  sleep 2
done

echo "Создание топиков..."
for TOPIC in topic-1 topic-2; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" \
    --create --if-not-exists --topic "$TOPIC" \
    --partitions 3 --replication-factor 3 --config min.insync.replicas=2
done

echo "Настройка ACL для topic-1 (продюсер и консьюмер)..."
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" \
  --add --allow-principal "$PRODUCER" --producer --topic topic-1

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" \
  --add --allow-principal "$CONSUMER" --consumer --topic topic-1 --group "$CONSUMER_GROUP"

echo "Настройка ACL для topic-2 (только продюсер)..."
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" \
  --add --allow-principal "$PRODUCER" --producer --topic topic-2

echo "Готово. Топики:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" --describe --topic 'topic-.*'

echo "Текущие ACL:"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" --list

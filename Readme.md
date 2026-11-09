# Задание по модулю 5, настройка Kafka SSL

## Задание:
1. Создайте сертификаты для каждого брокера.
2. Создайте Truststore и Keystore для каждого брокера.
3. Настройте дополнительные брокеры в режиме SSL. В первом модуле курса вы уже работали с кластером Kafka, состоящим из трёх брокеров. Используйте имеющийся docker-compose кластера и настройте для него SSL.

4. Создать два топика:

    topic-1
     topic-2
   
5. Настроить права доступа:

    topic-1: Доступен как для продюсеров, так и для консьюмеров.
        
   topic-2:
               Продюсеры могут отправлять сообщения.
               Консьюмеры не имеют доступа к чтению данных.
## Описание проекта
В проекте реализованы сертификаторы для всех брокеров.
Для создания топиков требуется запустить скрипт setup-acl.sh или выполнить команду вручную:
``` bash
kafka-topics --bootstrap-server localhost:9093 \
  --command-config certificates/brokers/kafka-1/client-ssl-local.properties \
  --create --topic topic-1 --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9093 \
  --command-config certificates/brokers/kafka-1/client-ssl-local.properties \
  --create --topic topic-2 --partitions 1 --replication-factor 1
```

Проверка создания топиков:
``` bash
kafka-topics --bootstrap-server localhost:9093 \
--command-config certificates/brokers/kafka-1/client-ssl-local.properties --list
```

Проверка применения правил:
``` bash
kafka-acls --bootstrap-server localhost:9093 \
--command-config certificates/brokers/kafka-1/client-ssl-local.properties --list
```
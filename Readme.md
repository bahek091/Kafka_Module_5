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
        \
   topic-2:
               Продюсеры могут отправлять сообщения.
               Консьюмеры не имеют доступа к чтению данных.
## Описание проекта

### Состав
| Путь | Назначение |
|---|---|
| `docker-compose.yaml` | Zookeeper + 3 брокера Kafka (SSL, mTLS, ACL), служебный контейнер `kafka-cli` |
| `certificates/ca` | Корневой CA, которым подписаны все сертификаты |
| `certificates/brokers/kafka-N` | Ключ, сертификат (SAN: `kafka-N`, `kafka-N-external`, `localhost`), keystore/truststore брокера |
| `certificates/clients/kafka-admin` | Сертификат администратора (суперпользователь, создаёт топики и ACL) |
| `certificates/clients/producer` | Сертификат продюсера |
| `certificates/clients/consumer` | Сертификат консьюмера |
| `configs/openssl-*.cnf` | Шаблоны openssl для CA, брокеров и клиентов |
| `generate-client-certs.sh` | Перевыпуск клиентских сертификатов |
| `setup-acl.sh` | Создание топиков и ACL |
| `src/main/java/...` | `SslProducer`, `SslConsumer` |

### Безопасность
* Все листенеры — SSL, PLAINTEXT отключён:
  * `INTERNAL_SSL` (`kafka-N:9092`) — межброкерный трафик и клиенты внутри docker-сети;
  * `EXTERNAL_SSL` (`localhost:9093/9094/9095`) — клиенты с хоста.
* `ssl.client.auth=required` — без сертификата, подписанного CA, подключиться нельзя.
  Имя пользователя (principal) — DN сертификата, например `User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU`.
* `allow.everyone.if.no.acl.found=false`, `User:ANONYMOUS` суперпользователем не является.
* Суперпользователи — только брокеры `kafka-1..3` и `kafka-admin`. Продюсер и консьюмер работают со своими сертификатами и получают права только через ACL.

### Топики и права
Топики `topic-1` и `topic-2`: 3 партиции, `replication-factor=3`, `min.insync.replicas=2`.

| Principal | topic-1 | topic-2 |
|---|---|---|
| `CN=producer` | запись | запись |
| `CN=consumer` (группа `test-group`) | чтение | нет доступа |

## Запуск
Нужен только Docker (Kafka CLI локально ставить не требуется).

Bash (Linux/macOS, Git Bash):
```bash
docker compose up -d
./setup-acl.sh
```
PowerShell:
```powershell
docker compose up -d
docker compose --profile tools run --rm -e KAFKA_CLI_CONTAINER=1 kafka-cli
```
`setup-acl.sh` запускает себя в контейнере `kafka-cli` (профиль `tools`), ждёт готовности всех трёх брокеров,
создаёт `topic-1` и `topic-2` и выдаёт ACL. Скрипт можно запускать повторно.

Топики и ACL хранятся в данных ZooKeeper/Kafka. После `docker compose down -v` (или пересоздания
контейнеров) их нет — снова запустить `./setup-acl.sh`, иначе клиенты получат `TOPIC_AUTHORIZATION_FAILED`.

## Проверка
Команды приведены в двух вариантах — для **PowerShell** (перенос строки — обратная кавычка `` ` ``)
и для **Bash** (перенос — `\`). В Git Bash на Windows перед командами с путями `/etc/...` выполнить
`export MSYS_NO_PATHCONV=1`, иначе пути будут переписаны в `C:/Program Files/Git/etc/...`.

### Java-клиенты
Команды Maven одинаковы для PowerShell и Bash.

Продюсер (сертификат `producer`) — запись в оба топика проходит:
```bash
mvn compile exec:java "-Dexec.mainClass=praktikum.kafka.SslProducer" "-Dexec.args=topic-1"
mvn compile exec:java "-Dexec.mainClass=praktikum.kafka.SslProducer" "-Dexec.args=topic-2"
```

Консьюмер (сертификат `consumer`) — `topic-1` читается, на `topic-2` получаем `TopicAuthorizationException`:
```bash
mvn compile exec:java "-Dexec.mainClass=praktikum.kafka.SslConsumer" "-Dexec.args=topic-1 test-group"
mvn compile exec:java "-Dexec.mainClass=praktikum.kafka.SslConsumer" "-Dexec.args=topic-2 test-group"
```

### Консольные клиенты (внутри docker-сети)
1. Состояние кластера — топики, лидеры, ISR (администратор):
PowerShell:
```powershell
docker compose --profile tools run --rm --entrypoint kafka-topics kafka-cli `
  --bootstrap-server kafka-1:9092 --describe `
  --command-config /etc/kafka/clients/kafka-admin/client-ssl.properties
```
Bash:
```bash
docker compose --profile tools run --rm --entrypoint kafka-topics kafka-cli \
  --bootstrap-server kafka-1:9092 --describe \
  --command-config /etc/kafka/clients/kafka-admin/client-ssl.properties
```
Ожидается `ReplicationFactor: 3` и по три брокера в `Isr` для каждой партиции.

2. Продюсер пишет в `topic-1` и `topic-2` — успешно (код выхода 0, без ошибок):
PowerShell:
```powershell
"hello-1" | docker compose --profile tools run --rm -T --entrypoint kafka-console-producer kafka-cli `
  --bootstrap-server kafka-1:9092 --topic topic-1 `
  --producer.config /etc/kafka/clients/producer/client-ssl.properties

"hello-2" | docker compose --profile tools run --rm -T --entrypoint kafka-console-producer kafka-cli `
  --bootstrap-server kafka-1:9092 --topic topic-2 `
  --producer.config /etc/kafka/clients/producer/client-ssl.properties
```
Bash:
```bash
echo "hello-1" | docker compose --profile tools run --rm -T --entrypoint kafka-console-producer kafka-cli \
  --bootstrap-server kafka-1:9092 --topic topic-1 \
  --producer.config /etc/kafka/clients/producer/client-ssl.properties

echo "hello-2" | docker compose --profile tools run --rm -T --entrypoint kafka-console-producer kafka-cli \
  --bootstrap-server kafka-1:9092 --topic topic-2 \
  --producer.config /etc/kafka/clients/producer/client-ssl.properties
```

3. Консьюмер читает `topic-1` — выводит `hello-1`:
PowerShell:
```powershell
docker compose --profile tools run --rm --entrypoint kafka-console-consumer kafka-cli `
  --bootstrap-server kafka-1:9092 --topic topic-1 --group test-group --from-beginning --timeout-ms 10000 `
  --consumer.config /etc/kafka/clients/consumer/client-ssl.properties
```
Bash:
```bash
docker compose --profile tools run --rm --entrypoint kafka-console-consumer kafka-cli \
  --bootstrap-server kafka-1:9092 --topic topic-1 --group test-group --from-beginning --timeout-ms 10000 \
  --consumer.config /etc/kafka/clients/consumer/client-ssl.properties
```
`TimeoutException` в конце — это штатное завершение по `--timeout-ms`. `--from-beginning` действует только
для новой группы: если `test-group` уже вычитала топик, будут показаны только новые сообщения (шаг 2).

4. Консьюмер читает `topic-2` — `TopicAuthorizationException: Not authorized to access topics: [topic-2]`:
PowerShell:
```powershell
docker compose --profile tools run --rm --entrypoint kafka-console-consumer kafka-cli `
  --bootstrap-server kafka-1:9092 --topic topic-2 --group test-group --from-beginning --timeout-ms 10000 `
  --consumer.config /etc/kafka/clients/consumer/client-ssl.properties
```
Bash:
```bash
docker compose --profile tools run --rm --entrypoint kafka-console-consumer kafka-cli \
  --bootstrap-server kafka-1:9092 --topic topic-2 --group test-group --from-beginning --timeout-ms 10000 \
  --consumer.config /etc/kafka/clients/consumer/client-ssl.properties
```

5. Список ACL (администратор):
PowerShell:
```powershell
docker compose --profile tools run --rm --entrypoint kafka-acls kafka-cli `
  --bootstrap-server kafka-1:9092 --list `
  --command-config /etc/kafka/clients/kafka-admin/client-ssl.properties
```
Bash:
```bash
docker compose --profile tools run --rm --entrypoint kafka-acls kafka-cli \
  --bootstrap-server kafka-1:9092 --list \
  --command-config /etc/kafka/clients/kafka-admin/client-ssl.properties
```


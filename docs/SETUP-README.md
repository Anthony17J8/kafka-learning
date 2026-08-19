# Kafka Learning — практический курс по Apache Kafka 4.x

Поэтапное изучение Apache Kafka: от одиночного брокера до продакшн-подобной системы
с exactly-once обработкой, CDC, мониторингом и безопасностью.

**Стек:** Apache Kafka 4.0 (KRaft, без ZooKeeper) · Java 17 · Maven · Docker Compose
**Целевая ОС для инструкций:** Linux Mint 21.x / 22.x (подойдёт любая Ubuntu-совместимая)

---

## Оглавление

1. [Требования к машине](#1-требования-к-машине)
2. [Установка окружения на Linux Mint](#2-установка-окружения-на-linux-mint)
3. [Первый запуск](#3-первый-запуск)
4. [Структура репозитория](#4-структура-репозитория)
5. [Ежедневные команды](#5-ежедневные-команды)
6. [Этапы курса](#6-этапы-курса)
7. [Диагностика проблем](#7-диагностика-проблем)

---

## 1. Требования к машине

| Ресурс | Минимум | Комфортно |
|---|---|---|
| RAM | 8 ГБ | 16 ГБ |
| Свободный диск | 20 ГБ | 40 ГБ |
| CPU | 4 ядра | 8 ядер |

На 8 ГБ RAM полный стек (`--profile all`) поднимать не стоит — используйте отдельные профили.

---

## 2. Установка окружения на Linux Mint

> Все команды выполняются в терминале. Строки, начинающиеся с `#`, — комментарии.

### Шаг 2.1. Обновить систему и поставить базовые утилиты

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y curl wget git jq ca-certificates gnupg unzip net-tools
```

`jq` понадобится для работы с REST API Kafka Connect и Schema Registry.

### Шаг 2.2. Определить кодовое имя базовой Ubuntu

Linux Mint основан на Ubuntu, но `lsb_release -cs` вернёт мятное имя (`vera`, `virginia`, `wilma`),
которого нет в репозиториях Docker. Нужно кодовое имя **базовой Ubuntu**:

```bash
. /etc/os-release && echo "$UBUNTU_CODENAME"
```

Ожидаемый вывод: `jammy` (Mint 21.x) или `noble` (Mint 22.x). Запомните его.

### Шаг 2.3. Удалить старые/конфликтующие пакеты Docker

```bash
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
  sudo apt remove -y $pkg 2>/dev/null
done
```

Ничего страшного, если пакетов не было — команда просто ничего не сделает.

### Шаг 2.4. Подключить официальный репозиторий Docker

```bash
# GPG-ключ
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Репозиторий — обратите внимание на $UBUNTU_CODENAME, это ключевой момент для Mint
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo "$UBUNTU_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
```

### Шаг 2.5. Установить Docker Engine и плагин Compose

```bash
sudo apt install -y docker-ce docker-ce-cli containerd.io \
                    docker-buildx-plugin docker-compose-plugin
```

Проверка:

```bash
docker --version          # Docker version 27.x.x или новее
docker compose version    # Docker Compose version v2.3x.x или новее
```

> Обратите внимание: команда `docker compose` (через пробел), а не устаревшая `docker-compose`.

### Шаг 2.6. Запускать Docker без sudo

```bash
sudo usermod -aG docker $USER
```

**Обязательно перелогиньтесь** (выйти из сессии и зайти снова) либо выполните:

```bash
newgrp docker
```

Проверка — команда должна отработать без `sudo`:

```bash
docker run --rm hello-world
```

### Шаг 2.7. Включить автозапуск Docker

```bash
sudo systemctl enable --now docker
systemctl status docker --no-pager
```

### Шаг 2.8. Установить Java 17

```bash
sudo apt install -y openjdk-17-jdk
java -version    # openjdk version "17.0.x"
javac -version
```

Если в системе несколько JDK, выберите 17-ю по умолчанию:

```bash
sudo update-alternatives --config java
sudo update-alternatives --config javac
```

Пропишите `JAVA_HOME` (сохранится между сессиями):

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
echo $JAVA_HOME
```

### Шаг 2.9. Установить Maven

```bash
sudo apt install -y maven
mvn -version
```

Убедитесь, что в выводе `Java version: 17.x`. Если там другая версия — вернитесь к шагу 2.8.

### Шаг 2.10. Установить kcat (опционально, но очень удобно)

```bash
sudo apt install -y kafkacat    # в новых версиях пакет называется kcat
kcat -V 2>/dev/null || kafkacat -V
```

`kcat` — CLI-нож для Kafka: посмотреть сообщения, метаданные, отправить тестовую запись.

### Шаг 2.11. Увеличить лимиты системы

Kafka и Docker открывают много файловых дескрипторов и используют inotify:

```bash
# inotify (нужно Docker и IDE)
echo 'fs.inotify.max_user_watches=524288' | sudo tee -a /etc/sysctl.conf
echo 'fs.inotify.max_user_instances=512'  | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

# файловые дескрипторы
echo "$USER soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "$USER hard nofile 65536" | sudo tee -a /etc/security/limits.conf
```

Лимиты `nofile` применятся после перелогина.

### Шаг 2.12. Проверить, что нужные порты свободны

```bash
for p in 8080 8081 8083 8088 9090 3000 9092 19092 29092 39092 5432; do
  ss -ltn "sport = :$p" | grep -q LISTEN && echo "ЗАНЯТ: $p" || echo "свободен: $p"
done
```

Чаще всего конфликтует **5432** (локально установленный PostgreSQL) и **3000**.
Либо остановите свой сервис, либо поменяйте маппинг портов в `docker/full-stack.yml`.

### Шаг 2.13. Клонировать проект и сделать скрипты исполняемыми

```bash
git clone <URL-вашего-репозитория> kafka-learning
cd kafka-learning
chmod +x scripts/*.sh 09-operations-monitoring/loadtest/*.sh
```

### Шаг 2.14. Проверить сборку Java

```bash
mvn -q clean compile
```

Первый прогон скачает зависимости (несколько минут). Успех = отсутствие ошибок и появление папок `target/`.

---

## 3. Первый запуск

### 3.1. Поднять одиночный брокер

```bash
./scripts/up.sh single
```

Дождитесь состояния `healthy`:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### 3.2. Создать учебные топики

```bash
./scripts/create-topics.sh single
```

### 3.3. Проверить кластер

```bash
./scripts/kafka.sh kafka-topics.sh --list
./scripts/kafka.sh kafka-topics.sh --describe --topic orders

# Метаданные KRaft-кворума — то, что раньше жило в ZooKeeper
./scripts/kafka.sh kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

Откройте **http://localhost:8080** — веб-интерфейс Kafka UI.

### 3.4. Прогнать первый Java-код

Терминал 1 — консьюмер:

```bash
mvn -q -pl 03-consumers -am compile
mvn -q -pl 03-consumers exec:java \
  -Dexec.mainClass=dev.kafkalearn.consumers.ManualCommitConsumer
```

Терминал 2 — продюсер:

```bash
mvn -q -pl 02-producers -am compile
mvn -q -pl 02-producers exec:java \
  -Dexec.mainClass=dev.kafkalearn.producers.SimpleProducer
```

В консьюмере должны побежать сообщения с указанием партиции и оффсета.

### 3.5. Перейти на кластер из трёх брокеров

```bash
./scripts/down.sh
./scripts/up.sh cluster
./scripts/create-topics.sh cluster
```

Java-код против кластера запускается с переменной окружения:

```bash
export BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
mvn -q -pl 02-producers exec:java -Dexec.mainClass=dev.kafkalearn.producers.SimpleProducer
```

### 3.6. Финальная проверка — эксперимент с отказом

```bash
# Смотрим, кто лидер партиций
KAFKA_CONTAINER=kafka-1 BOOTSTRAP=kafka-1:19093 \
  ./scripts/kafka.sh kafka-topics.sh --describe --topic orders

# Убиваем один брокер
docker stop kafka-2

# Смотрим снова: лидеры переехали, ISR сократился
KAFKA_CONTAINER=kafka-1 BOOTSTRAP=kafka-1:19093 \
  ./scripts/kafka.sh kafka-topics.sh --describe --topic orders

# Возвращаем
docker start kafka-2
```

Если увидели переизбрание лидера — окружение развёрнуто корректно. Можно начинать этап 1.

---

## 4. Структура репозитория

```
kafka-learning/
├── pom.xml                          # родительский POM, Java 17, версии зависимостей
├── docker/
│   ├── single-broker.yml            # 1 брокер KRaft + UI
│   ├── three-broker.yml             # 3 брокера KRaft + UI
│   ├── full-stack.yml               # + Schema Registry, Connect, Postgres, мониторинг
│   ├── prometheus/                  # конфиг Prometheus
│   ├── grafana/provisioning/        # автоподключение datasource и дашбордов
│   └── jmx-exporter/                # правила JMX -> Prometheus
├── scripts/
│   ├── up.sh / down.sh              # управление окружением
│   ├── kafka.sh                     # обёртка над CLI Kafka в контейнере
│   ├── create-topics.sh             # учебные топики
│   └── download-jmx-exporter.sh
├── common/                          # общий код: конфиг, доменное событие, JSON serde
├── 00-environment/                  # заметки по окружению
├── 01-fundamentals/                 # CLI-эксперименты (без Java)
├── 02-producers/                    # ✅ рабочий код + TODO
├── 03-consumers/                    # ✅ рабочий код + TODO
├── 04-serialization-schema-registry/ # Avro-схемы + заготовки
├── 05-delivery-semantics-transactions/
├── 06-kafka-connect/                # конфиги Debezium, SQL, инструкции REST API
├── 07-kafka-streams/                # заготовка топологии + теста
├── 08-ksqldb/
├── 09-operations-monitoring/        # нагрузочные тесты, дашборды
├── 10-security/
├── 11-advanced-patterns/            # Outbox, идемпотентный консьюмер
├── capstone/                        # финальный проект
├── docs/
└── GLOSSARY.md                      # ваш словарь терминов, пополняйте по ходу
```

**В каждой папке этапа:** `README.md` (задачи), `NOTES.md` (ваш конспект — заполняете сами),
`src/` (код), `screenshots/` (доказательства выполнения).

---

## 5. Ежедневные команды

```bash
# Окружение
./scripts/up.sh single|cluster|core|connect|monitoring|all
./scripts/down.sh          # остановить
./scripts/down.sh -v       # остановить и стереть данные

# Топики
./scripts/kafka.sh kafka-topics.sh --list
./scripts/kafka.sh kafka-topics.sh --describe --topic orders
./scripts/kafka.sh kafka-topics.sh --create --topic test --partitions 3 --replication-factor 1

# Консольные продюсер/консьюмер
./scripts/kafka.sh kafka-console-producer.sh --topic orders --property parse.key=true --property key.separator=:
./scripts/kafka.sh kafka-console-consumer.sh --topic orders --from-beginning --property print.key=true

# Консьюмер-группы и lag
./scripts/kafka.sh kafka-consumer-groups.sh --list
./scripts/kafka.sh kafka-consumer-groups.sh --describe --group orders-manual-commit

# KRaft-кворум
./scripts/kafka.sh kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

# Сборка
mvn -q clean compile                 # весь проект
mvn -q -pl 02-producers -am compile  # один модуль с зависимостями
mvn test                             # тесты

# Логи
docker logs -f kafka
docker compose -f docker/full-stack.yml --profile all logs -f connect
```

---

## 6. Этапы курса

| # | Тема | Статус | Ключевой результат |
|---|---|---|---|
| 0 | Окружение | ☐ | KRaft-кластер поднят, CLI работает |
| 1 | Фундамент и архитектура | ☐ | Понимание лога, партиций, ISR, compaction |
| 2 | Producers | ☐ | Свой партиционер, замеры throughput vs acks |
| 3 | Consumers | ☐ | Ручной коммит, ребалансировка, lag |
| 4 | Сериализация + Schema Registry | ☐ | Avro, эволюция схем, режимы совместимости |
| 5 | Семантика доставки, транзакции | ☐ | Рабочий EOS-пайплайн |
| 6 | Kafka Connect | ☐ | CDC Postgres → Kafka → sink, DLQ |
| 7 | Kafka Streams | ☐ | Оконная агрегация, join, тесты топологии |
| 8 | ksqlDB (опц.) | ☐ | То же на SQL |
| 9 | Эксплуатация и мониторинг | ☐ | Дашборд Grafana, нагрузочный тест, тюнинг |
| 10 | Безопасность | ☐ | TLS + SASL/SCRAM + ACL + квоты |
| 11 | Продвинутые паттерны | ☐ | Outbox, идемпотентность, MirrorMaker 2 |
| — | Capstone | ☐ | Система end-to-end одной командой |

Подробное описание теории и задач каждого этапа — в `README.md` соответствующей папки.

---

## 7. Диагностика проблем

### `permission denied` при обращении к Docker

Не перелогинились после `usermod -aG docker`. Выполните `newgrp docker` или перезайдите в систему.

### Контейнер Kafka перезапускается по кругу

```bash
docker logs kafka --tail 100
```

Частые причины:
- **Несовпадение `CLUSTER_ID`** после смены compose-файла на том же томе.
  Лечится: `./scripts/down.sh -v` (удалит данные) и запуск заново.
- **Занятый порт** — см. шаг 2.12.
- **Нехватка памяти** — уменьшите число сервисов, используйте профили.

### Java-клиент не может подключиться (`Connection refused`)

Проверьте, к какому адресу подключаетесь. С хост-машины это `localhost:9092`
(single) или `localhost:19092,...` (cluster). Адреса вида `kafka-1:19093` работают
**только изнутри docker-сети**. Это следствие `KAFKA_ADVERTISED_LISTENERS` —
брокер сообщает клиенту, куда ходить, и если там внутреннее имя, снаружи оно не резолвится.

### `UnknownTopicOrPartitionException`

Автосоздание топиков намеренно выключено (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).
Создайте топик явно: `./scripts/create-topics.sh single`.

### Maven тянет не ту версию Java

```bash
mvn -version    # смотрим строку Java version
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### Не хватает места на диске

```bash
docker system df               # что занимает место
docker system prune -a         # удалить неиспользуемые образы и контейнеры
docker volume ls               # тома с данными Kafka
```

### Полный сброс окружения

```bash
./scripts/down.sh -v
docker system prune -af --volumes    # ОСТОРОЖНО: удалит ВСЕ docker-данные на машине
```

---

## Литература

- [Официальная документация Apache Kafka](https://kafka.apache.org/documentation/) — первоисточник
- **«Kafka: The Definitive Guide», 2-е изд.** — Shapira, Palino, Sivaram, Petty
- **«Designing Data-Intensive Applications»** — Kleppmann (гл. 5, 7, 11)
- **«Kafka Streams in Action», 2-е изд.** — Bejeck
- [Confluent Developer](https://developer.confluent.io/) — бесплатные курсы
- [Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)

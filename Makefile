.PHONY: help up-single up-cluster up-core up-connect up-monitoring down clean build topics lag

help:
	@echo "make up-single      - 1 брокер KRaft + Kafka UI"
	@echo "make up-cluster     - 3 брокера KRaft + Kafka UI"
	@echo "make up-core        - 3 брокера + Schema Registry"
	@echo "make up-connect     - + Postgres + Kafka Connect (Debezium)"
	@echo "make up-monitoring  - + Prometheus + Grafana"
	@echo "make down           - остановить всё"
	@echo "make clean          - остановить и удалить данные"
	@echo "make build          - mvn clean compile"
	@echo "make topics         - создать учебные топики"
	@echo "make lag GROUP=...  - показать consumer lag"

up-single:     ; ./scripts/up.sh single
up-cluster:    ; ./scripts/up.sh cluster
up-core:       ; ./scripts/up.sh core
up-connect:    ; ./scripts/up.sh connect
up-monitoring: ; ./scripts/up.sh monitoring

down:  ; ./scripts/down.sh
clean: ; ./scripts/down.sh -v

build: ; mvn -q clean compile

topics: ; ./scripts/create-topics.sh $(MODE)

lag:
	mvn -q -pl 03-consumers exec:java \
	  -Dexec.mainClass=dev.kafkalearn.consumers.LagMonitor \
	  -Dexec.args="$(GROUP)"

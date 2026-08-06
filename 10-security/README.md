# Этап 10 — Безопасность

**Цель:** закрыть кластер так, как это делается в продакшене.

## Теория
- [Docs: Security](https://kafka.apache.org/documentation/#security) — читать целиком
- «Kafka: The Definitive Guide», гл. 11
- Разделы: SSL/TLS, SASL (PLAIN, SCRAM, GSSAPI, OAUTHBEARER), Authorization и ACL, Quotas

## Задачи
- [ ] 1. Сгенерировать CA, keystore и truststore (`configs/generate-certs.sh`).
- [ ] 2. Включить TLS-листенер на брокере, подключиться защищённым клиентом.
- [ ] 3. Настроить SASL/SCRAM-SHA-512, завести пользователей `producer-app` и `consumer-app`.
- [ ] 4. Настроить ACL: `producer-app` может писать в `orders`, но не читать `users`.
      Проверить, что запрет реально работает (`TopicAuthorizationException`).
- [ ] 5. Настроить квоту (`producer_byte_rate`) и продемонстрировать throttling.
- [ ] 6. Настроить TLS и для inter-broker трафика.

> **Секреты в git не коммитим.** Сертификаты и keystore уже в `.gitignore`.

## Контрольные вопросы
1. Почему TLS без аутентификации недостаточно для многопользовательского кластера?
2. Чем SCRAM лучше SASL/PLAIN?
3. Как ACL соотносится с principal, извлечённым из TLS-сертификата?
4. Что произойдёт, если включить authorizer без единого ACL?
5. Как квоты защищают кластер от «шумного соседа»?

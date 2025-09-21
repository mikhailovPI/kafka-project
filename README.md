# kafka-project

---

## Архитектура

### Модель

- **Message** — объект сообщения (`topic`, `key`, `data`).
- **TopicRequest** — DTO для передачи имени топика в контроллеры.

### Сериализация / десериализация

- **MessageSerializer** — Kafka `Serializer<Message>` (Jackson).
- **MessageDeserializer** — Kafka `Deserializer<Message>` (Jackson).
  > Благодаря этому продюсер и консьюмеры работают с типом `<String, Message>` и оперируют объектами, а не строками.

### Конфигурация

- **KafkaConfig** — создаёт:
    - `KafkaProducer<String, Message>`;
    - настройки для консьюмера с автокоммитом;
    - настройки для консьюмера с ручным коммитом батчей.

### Сервисы

- **ProducerService** — метод `publish(Message message)` отправляет объект `Message` в Kafka и логирует результат.
- **ConsumerService**:
    - `singleConsumeMessage(String topic)` — запускает консьюмера с автокоммитом;
    - `batchConsumeMessage(String topic)` — запускает консьюмера с буферизацией (≥10 сообщений или по таймауту) и
      `commitSync()` после обработки;
    - `stopAll()` — останавливает оба консьюмера.

### Контроллеры

- **KafkaProducerController**
    - `POST /api/send` — принимает JSON-боди `Message`, возвращает метаданные записи.
- **KafkaConsumerController**
    - `POST /api/consume/single` — запуск одиночного чтения;
    - `POST /api/consume/batch` — запуск пакетного чтения;
    - `POST /api/consume/stop` — остановка консьюмеров.

---

## Запуск

1. Поднимите Kafka (например, через `docker-compose.yml`).
2. Создайте топик (например, `test-topic`).
3. Соберите и запустите приложение:
   ```bash
   mvn clean package
   java -jar target/kafka-project-*.jar

## Проверка работоспособности

Примеры команд для тестирования (CMD / Windows):

### Запись в топик

```cmd
curl -X POST "http://localhost:8080/api/send" -H "Content-Type: application/json" -d "{\"topic\":\"demo-topic\",\"key\":\"user-1\",\"data\":\"hello json!\"}"
```

### Подготовка пачки сообщений (пример: 12 сообщений)

```cmd
for /L %i in (1,1,12) do curl -s -X POST "http://localhost:8080/api/send" -H "Content-Type: application/json" -d "{\"topic\":\"demo-topic\",\"key\":\"user-%i\",\"data\":\"payload-%i\"}" > NUL
```

### Одиночное чтение из топика

```cmd
curl -X POST "http://localhost:8080/api/consume/single" -H "Content-Type: application/json" -d "{\"topic\":\"demo-topic\"}"
```

### Батч-чтение сообщений

```cmd
curl -X POST "http://localhost:8080/api/consume/batch" -H "Content-Type: application/json" -d "{\"topic\":\"demo-topic\"}"
```

### Остановка консьюмеров

```cmd
curl -X POST http://localhost:8080/api/consume/stop
```
# Читання даних з топіку


from kafka import KafkaConsumer
from kafka import KafkaProducer
from conspect_create_configs import kafka_config
import json
import uuid

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",  # Зчитування повідомлень з початку
    enable_auto_commit=True,  # Автоматичне підтвердження зчитаних повідомлень
    group_id="my_consumer_group_3",  # Ідентифікатор групи споживачів
)

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Назва топіку
my_name = "lina"
building_sensors = f"{my_name}_building_sensors"
temperature_alerts = f"{my_name}_temperature_alerts"
humidity_alerts = f"{my_name}_humidity_alerts"

# Підписка на тему
consumer.subscribe([building_sensors])

print(f"Subscribed to topic '{building_sensors}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(
            f"Received message: {message.value} with key: {message.key}, partition {message.partition}"
        )
        temp = message.value["temperature"]
        if temp > 40:
            print(f"Temperature alert: {temp}")
            producer.send(
                temperature_alerts, key=str(uuid.uuid4()), value=message.value
            )
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        hum = message.value["humidity"]
        if hum < 20 or hum > 80:
            print(f"Humidity alert: {hum}")
            producer.send(humidity_alerts, key=str(uuid.uuid4()), value=message.value)
            producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

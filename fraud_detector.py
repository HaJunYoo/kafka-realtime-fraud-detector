from kafka import KafkaConsumer, KafkaProducer
import json

PAYMENT_TOPIC = "payments"
FRAUD_TOPIC = "fraud_payments"
LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(
    PAYMENT_TOPIC,
    bootstrap_servers=brokers,
    api_version=(0, 11, 5)
)

producer = KafkaProducer(
    bootstrap_servers=brokers,
    api_version=(0, 11, 5)
)


def is_suspicious(transactions):
    if transactions["PAYMENT_TYPE"] == "BITCOIN" or transactions["TO"] == "stranger":
        return True
    return False


for message in consumer:
    msg = json.loads(message.value.decode())
    topic = FRAUD_TOPIC if is_suspicious(msg) else LEGIT_TOPIC
    producer.send(topic, json.dumps(msg).encode("utf-8"))
    print(topic, is_suspicious(msg), msg["PAYMENT_TYPE"])

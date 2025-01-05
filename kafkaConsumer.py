from kafka import KafkaConsumer
import json

# Kafka Consumer setup
consumer = KafkaConsumer(
    'wikimedia',  # Topic name
    bootstrap_servers='localhost:9092',
    group_id='wikimedia-group',
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

try:
    print("Kafka Consumer is running...")
    for message in consumer:
        # Process the received message
        data = message.value
        print(f"Received message: User: {data['user']}, Title: {data['title']}")
except KeyboardInterrupt:
    print("Consumer interrupted by user.")
finally:
    consumer.close()
    print("Kafka consumer closed.")

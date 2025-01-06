from kafka import KafkaConsumer
import json
from opensearchpy import OpenSearch

# Kafka Consumer setup
consumer = KafkaConsumer(
    'wikimedia',  # Topic name
    bootstrap_servers='localhost:9092',
    group_id='wikimedia-group',
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# OpenSearch Client setup
os_client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('admin', 'sdlSDFqkjgsd6842sdgkjsqdg+dfs:'),
    use_ssl=True,
    verify_certs=False
)

index_name = "wikimedia"
# Ensure the index exists
if not os_client.indices.exists(index=index_name):
    os_client.indices.create(index=index_name)

try:
    print("Kafka Consumer is running...")
    for message in consumer:
        # Process the received message
        data = message.value
        # Index the message into OpenSearch
        try:
            os_client.index(index=index_name, body=data)
            print(f"Received message: User: {data['user']}, Title: {data['title']}")
        except:
            pass
except KeyboardInterrupt:
    print("Consumer interrupted by user.")
finally:
    consumer.close()
    print("Kafka consumer closed.")

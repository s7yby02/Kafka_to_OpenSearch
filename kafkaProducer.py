from kafka import KafkaProducer
import json
# Server Sent Event (SSE)
from sseclient import SSEClient as EventSource

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Wikimedia API URL
url = "https://stream.wikimedia.org/v2/stream/recentchange"

try:
    # Stream data and send to Kafka
    for event in EventSource(url, last_id=None):
        if event.event == "message":
            try:
                data = json.loads(event.data)
                print(data)
                producer.send('wikimedia', value=data)
                print("Sent to Kafka:", data['user'], data['title'])
            except json.JSONDecodeError as json_error:
                print("!!!!!!JSON parsing error:", json_error)
            except Exception as e:
                print("!!!!!!Unexpected error:", e)
except KeyboardInterrupt:
    print("Streaming interrupted by user.")
finally:
    producer.close()
    print("Kafka producer closed.")

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("In ascolto dei messaggi sul topic 'test_topic'...")

for message in consumer:
    print(f"Ricevuto messaggio: {message.value}")

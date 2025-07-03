from kafka import KafkaProducer
import json

def on_send_success(record_metadata):
    print(f"Messaggio inviato con successo a topic '{record_metadata.topic}', partizione {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Errore durante l'invio del messaggio: {excp}")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Inserisci messaggi da inviare al topic 'test_topic'. Digita 'exit' per terminare.")

while True:
    message = input("> ")
    if message.lower() == 'exit':
        break
    future = producer.send('test_topic', {'message': message})
    future.add_callback(on_send_success)
    future.add_errback(on_send_error)

producer.flush()
producer.close()
print("Producer terminato.")
from kafka import KafkaConsumer
import oracledb

# Oracle DB connection
connection = oracledb.connect(
    user="appuser",
    password="appuserpwd",
    dsn="localhost/XEPDB1"
)

cursor = connection.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

print("Listening for messages on Kafka topic...")

try:
    for message in consumer:
        decoded_msg = message.value.decode('utf-8')
        print(f"Consumed: {decoded_msg}")

        # Insert into Oracle
        cursor.execute(
            """
            INSERT INTO kafka_messages (message_content)
            VALUES (:1)
            """,
            [decoded_msg]
        )
        connection.commit()

except KeyboardInterrupt:
    print("Stopped by user.")

finally:
    cursor.close()
    connection.close()

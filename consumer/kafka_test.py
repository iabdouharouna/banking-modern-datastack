from kafka import KafkaConsumer
import json

# Configuration options for the consumer
config = {
    'bootstrap_servers': ['host.docker.internal:29092'], # Address of the Kafka broker
    'group_id': 'group-3',            # Consumer group name
    'auto_offset_reset': 'earliest',         # Start reading from the beginning of the topic if no offset is committed
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) # Deserialize JSON messages
}

# Create a consumer instance
consumer = KafkaConsumer('banking_server.public.customers', **config)

# Start reading messages in a loop
print(f"Subscribed to topic: {'banking_server.public.customers'}. Consuming messages...")
try:
    for message in consumer:
        # message is a ConsumerRecord namedtuple with topic, partition, offset, key, and value
        print(f"Received message: offset={message.offset}, value={message.value}")
except KeyboardInterrupt:
    pass
finally:
    # Always close the consumer to ensure partitions are re-assigned to other members immediately
    consumer.close()

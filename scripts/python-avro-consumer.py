#!/usr/bin/env python3

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Use Confluent compatibility API endpoint
schema_registry_conf = {'url': 'http://localhost:8081/apis/ccompat/v7'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro deserializer
avro_deserializer = AvroDeserializer(schema_registry_client)

# Kafka consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:29093',
    'group.id': 'avro-test-consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['target-users'])

print("Consuming Avro messages from target-users topic...")
print("Press Ctrl+C to stop\n")

try:
    msg_count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        user = avro_deserializer(msg.value(), SerializationContext('target-users', MessageField.VALUE))

        msg_count += 1
        print(f"Message {msg_count}:")
        print(f"  Key: {msg.key().decode('utf-8')}")
        print(f"  Value: {user}")
        print()

except KeyboardInterrupt:
    print(f"\nConsumed {msg_count} messages")
finally:
    consumer.close()
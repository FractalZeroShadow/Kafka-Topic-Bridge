#!/usr/bin/env python3

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json

# Read schema
with open('schemas/user.avsc', 'r') as f:
    schema_str = f.read()

# Use Confluent compatibility API endpoint
schema_registry_conf = {'url': 'http://localhost:8080/apis/ccompat/v7'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'avro-test-producer'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f'Failed: {err}')
    else:
        print(f'Sent: {msg.key().decode("utf-8")} to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

# Test data
test_users = [
    {'id': 'user-001', 'username': 'alice', 'email': 'alice@example.com'},
    {'id': 'user-002', 'username': 'bob', 'email': 'bob@example.com'},
    {'id': 'user-003', 'username': 'charlie', 'email': 'charlie@example.com'}
]

print("Producing Avro messages to source-users topic...")

for user in test_users:
    try:
        producer.produce(
            topic='source-users',
            key=user['id'].encode('utf-8'),
            value=avro_serializer(user, SerializationContext('source-users', MessageField.VALUE)),
            on_delivery=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        print(f"Error: {e}")

producer.flush()
print("\nAll messages sent!")
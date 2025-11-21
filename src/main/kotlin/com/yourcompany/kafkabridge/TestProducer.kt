package com.yourcompany.kafkabridge

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

fun main() {
    // 1. Define 3 distinct Schemas
    val userSchemaStr = """
        {
          "type": "record", "name": "User", "namespace": "com.test.avro",
          "fields": [
            {"name": "username", "type": "string"},
            {"name": "age", "type": "int"}
          ]
        }
    """
    val productSchemaStr = """
        {
          "type": "record", "name": "Product", "namespace": "com.test.avro",
          "fields": [
            {"name": "sku", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "active", "type": "boolean"}
          ]
        }
    """
    val orderSchemaStr = """
        {
          "type": "record", "name": "Order", "namespace": "com.test.avro",
          "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "double"}
          ]
        }
    """

    // 2. Configure Producer (Targeting Source Confluent Cluster)
    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
        "schema.registry.url" to "http://localhost:8081",
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
        "acks" to "all"
    )

    KafkaProducer<String, Any>(config).use { producer ->
        val parser = Schema.Parser()

        val scenarios = listOf(
            Triple("test-source-1", parser.parse(userSchemaStr)) { schema: Schema ->
                GenericData.Record(schema).apply {
                    put("username", "alice")
                    put("age", 30)
                }
            },
            Triple("test-source-2", parser.parse(productSchemaStr)) { schema: Schema ->
                GenericData.Record(schema).apply {
                    put("sku", "PROD-123")
                    put("price", 99.99)
                    put("active", true)
                }
            },
            Triple("test-source-3", parser.parse(orderSchemaStr)) { schema: Schema ->
                GenericData.Record(schema).apply {
                    put("orderId", "ORD-555")
                    put("amount", 150.50)
                }
            }
        )

        println("--- STARTING MULTI-TOPIC PRODUCER ---")

        scenarios.forEach { (topic, schema, recordCreator) ->
            val record = recordCreator(schema)
            val key = "key-${System.currentTimeMillis()}"

            producer.send(ProducerRecord(topic, key, record)) { meta, ex ->
                if (ex != null) {
                    println("Failed $topic: ${ex.message}")
                } else {
                    println("Sent to '$topic' (Schema: ${schema.name}) @ offset ${meta.offset()}")
                }
            }
        }
        producer.flush()
    }
    println("Done.")
}
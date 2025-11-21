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
    val bootstrapServers = "localhost:29092"
    val schemaRegistryUrl = "http://localhost:8081"

    // ======================================================
    // PHASE 1: Original Schemas (V1)
    // ======================================================
    val userSchemaV1 = """
        {
          "type": "record", "name": "User", "namespace": "com.test.avro",
          "fields": [
            {"name": "username", "type": "string"},
            {"name": "age", "type": "int"}
          ]
        }
    """
    val productSchemaV1 = """
        {
          "type": "record", "name": "Product", "namespace": "com.test.avro",
          "fields": [
            {"name": "sku", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "active", "type": "boolean"}
          ]
        }
    """
    val orderSchemaV1 = """
        {
          "type": "record", "name": "Order", "namespace": "com.test.avro",
          "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "double"}
          ]
        }
    """

    // ======================================================
    // PHASE 2: Schema Evolution (V2 - Backward Compatible)
    // ======================================================
    val userSchemaV2 = """
        {
          "type": "record", "name": "User", "namespace": "com.test.avro",
          "fields": [
            {"name": "username", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "city", "type": ["null", "string"], "default": null}
          ]
        }
    """
    val productSchemaV2 = """
        {
          "type": "record", "name": "Product", "namespace": "com.test.avro",
          "fields": [
            {"name": "sku", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "active", "type": "boolean"},
            {"name": "category", "type": "string", "default": "General"}
          ]
        }
    """
    val orderSchemaV2 = """
        {
          "type": "record", "name": "Order", "namespace": "com.test.avro",
          "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "currency", "type": "string", "default": "USD"}
          ]
        }
    """

    val avroConfig = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
        "schema.registry.url" to schemaRegistryUrl,
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
        "acks" to "all"
    )

    println("--- 1. SENDING NORMAL & EVOLVED AVRO MESSAGES ---")

    KafkaProducer<String, Any>(avroConfig).use { producer ->

        fun send(topic: String, schemaStr: String, fieldSetter: (GenericData.Record) -> Unit) {
            val schema = Schema.Parser().parse(schemaStr)
            val record = GenericData.Record(schema).apply(fieldSetter)
            val key = "key-${System.currentTimeMillis()}"

            producer.send(ProducerRecord(topic, key, record)) { meta, ex ->
                if (ex != null) println("Failed $topic: ${ex.message}")
                else println("Sent to '$topic' (Schema: ${schema.name}) offset ${meta.offset()}")
            }
        }

        // --- Batch 1: Original V1 ---
        send("test-source-1", userSchemaV1) {
            it.put("username", "alice")
            it.put("age", 30)
        }
        send("test-source-2", productSchemaV1) {
            it.put("sku", "PROD-111")
            it.put("price", 99.99)
            it.put("active", true)
        }
        send("test-source-3", orderSchemaV1) {
            it.put("orderId", "ORD-555")
            it.put("amount", 150.50)
        }

        Thread.sleep(1000)

        // --- Batch 2: Evolved V2 ---
        send("test-source-1", userSchemaV2) {
            it.put("username", "bob_evolved")
            it.put("age", 25)
            it.put("city", "New York")
        }
        send("test-source-2", productSchemaV2) {
            it.put("sku", "PROD-222")
            it.put("price", 199.99)
            it.put("active", true)
            it.put("category", "Electronics")
        }
        send("test-source-3", orderSchemaV2) {
            it.put("orderId", "ORD-777")
            it.put("amount", 300.00)
            it.put("currency", "EUR")
        }

        producer.flush()
    }

    println("\n--- 2. SENDING POISON PILL (TRIGGER DLT) ---")

    val rawConfig = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
    )

    KafkaProducer<String, String>(rawConfig).use { rawProducer ->
        val poisonTopic = "test-source-1"
        val key = "poison-key-${System.currentTimeMillis()}"
        val value = "THIS_IS_NOT_AVRO_DATA_AND_WILL_CRASH_THE_BRIDGE"

        rawProducer.send(ProducerRecord(poisonTopic, key, value)) { meta, ex ->
            if (ex != null) println("Failed sending poison: ${ex.message}")
            else println("Sent POISON PILL to '$poisonTopic' @ offset ${meta.offset()}")
        }
        rawProducer.flush()
    }

    println("\nDone. Check Bridge logs for Retries and DLT messages.")
}
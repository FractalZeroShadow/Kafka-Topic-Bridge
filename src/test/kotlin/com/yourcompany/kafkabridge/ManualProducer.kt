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

    val avroConfig = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,
        "schema.registry.url" to schemaRegistryUrl,
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,
        "acks" to "all"
    )

    println("--- STARTING VALID DATA PRODUCER ---")

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

        // 1. Send V1 (Original)
        println(">>> Sending V1 Data")
        send("test-source-1", userSchemaV1) {
            it.put("username", "alice_v1")
            it.put("age", 30)
        }
        Thread.sleep(500)

        // 2. Send V2 (Evolved - Added City)
        println(">>> Sending V2 Data (Schema Evolution)")
        send("test-source-1", userSchemaV2) {
            it.put("username", "bob_v2")
            it.put("age", 25)
            it.put("city", "New York")
        }
        Thread.sleep(500)

        // 3. Send V1 Again (Backward Compatibility Check)
        println(">>> Sending V1 Data Again")
        send("test-source-1", userSchemaV1) {
            it.put("username", "charlie_v1_again")
            it.put("age", 40)
        }

        producer.flush()
    }
    println("--- DONE ---")
}
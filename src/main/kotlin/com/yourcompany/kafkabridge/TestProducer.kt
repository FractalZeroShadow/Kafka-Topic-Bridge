package com.yourcompany.kafkabridge

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.net.HttpURLConnection
import java.net.URL

fun main() {
    val schema = org.apache.avro.Schema.Parser().parse("""
        {
          "type": "record",
          "name": "User",
          "namespace": "com.yourcompany.kafkabridge.avro",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "username", "type": "string"},
            {"name": "email", "type": "string"}
          ]
        }
    """)

    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java,

        // Source Registry (Confluent)
        "schema.registry.url" to "http://localhost:8081",
        // Explicitly enable auto-register (Default is true, but being verbose helps debugging)
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to true,

        "acks" to "all"
    )

    println("--- STARTING PRODUCER ---")

    KafkaProducer<String, Any>(config).use { producer ->
        listOf(
            Triple("user-001", "alice", "alice@example.com"),
            Triple("user-002", "bob", "bob@example.com"),
            Triple("user-003", "charlie", "charlie@example.com")
        ).forEach { (id, username, email) ->
            val user = GenericData.Record(schema).apply {
                put("id", id)
                put("username", username)
                put("email", email)
            }

            // Topic matches the KEY in application.yaml mappings
            val topic = "source-users-v1-legacy"

            producer.send(ProducerRecord(topic, id, user)) { metadata, ex ->
                if (ex != null) {
                    println("Failed to send $id: ${ex.message}")
                    ex.printStackTrace()
                } else {
                    println("Sent: $id to '$topic' @ offset ${metadata.offset()}")
                }
            }
        }
        producer.flush()
    }

    println("\n--- VERIFYING REGISTRY CONTENT ---")
    try {
        // Simple check to list subjects in Source Registry
        val url = URL("http://localhost:8081/subjects")
        with(url.openConnection() as HttpURLConnection) {
            requestMethod = "GET"
            println("Registry Response Code: $responseCode")
            inputStream.bufferedReader().use {
                val response = it.readText()
                println("Registered Subjects: $response")
                if (response.contains("source-users-v1-legacy-value")) {
                    println("SUCCESS: Schema found in Source Registry!")
                } else {
                    println("WARNING: Schema NOT found in list (it might be empty []).")
                }
            }
        }
    } catch (e: Exception) {
        println("Could not connect to Registry API to verify: ${e.message}")
    }

    println("Done.")
}
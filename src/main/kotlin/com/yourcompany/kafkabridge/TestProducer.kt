package com.yourcompany.kafkabridge

import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

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
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to AvroKafkaSerializer::class.java,
        "apicurio.registry.url" to "http://localhost:8080/apis/registry/v2",
        "apicurio.registry.auto-register" to "true"
    )

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

            producer.send(ProducerRecord("source-users", id, user)) { metadata, ex ->
                if (ex != null) println("Failed: ${ex.message}")
                else println("Sent: $id @ ${metadata.offset()}")
            }
        }
        producer.flush()
    }
    println("Done!")
}
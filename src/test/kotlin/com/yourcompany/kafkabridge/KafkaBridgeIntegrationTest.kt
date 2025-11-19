package com.yourcompany.kafkabridge

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import mu.KotlinLogging
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import java.time.Duration
import java.util.*
import org.junit.jupiter.api.Disabled

private val logger = KotlinLogging.logger {}

@Disabled("Run manually - requires Kafka infrastructure")
@SpringBootTest
@TestPropertySource(
    properties = [
        "spring.kafka.bootstrap-servers=localhost:29092",
        "bridge.target-kafka-brokers=localhost:29093",
        "spring.kafka.consumer.group-id=test-group",
        "spring.kafka.consumer.properties.apicurio.registry.url=http://localhost:8080/apis/registry/v2",
        "spring.kafka.producer.properties.apicurio.registry.url=http://localhost:8081/apis/registry/v2",
        "bridge.source-topic=test-source-topic",
        "bridge.target-topic=test-target-topic"
    ]
)
class KafkaBridgeIntegrationTest {

    companion object {
        private const val SOURCE_BOOTSTRAP = "localhost:29092"
        private const val TARGET_BOOTSTRAP = "localhost:29093"
        private const val SOURCE_REGISTRY = "http://localhost:8080/apis/registry/v2"
        private const val TARGET_REGISTRY = "http://localhost:8081/apis/registry/v2"
        private const val TEST_SOURCE_TOPIC = "test-source-topic"
        private const val TEST_TARGET_TOPIC = "test-target-topic"

        private const val USER_SCHEMA = """
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
        """
    }

    @Test
    fun contextLoads() {
        logger.info { "Spring context loaded successfully" }
    }

    @Test
    fun testEndToEndBridge() {
        logger.info { "Starting end-to-end bridge test" }

        // Step 1: Produce a message to source cluster
        val testUserId = "test-user-${System.currentTimeMillis()}"
        val testUsername = "testuser"
        val testEmail = "test@example.com"

        produceToSource(testUserId, testUsername, testEmail)

        // Step 2: Wait for bridge to process (adjust timeout as needed)
        Thread.sleep(5000)

        // Step 3: Consume from target cluster and verify
        val receivedMessage = consumeFromTarget(testUserId)

        assertNotNull(receivedMessage, "Should receive message from target cluster")
        assertEquals(testUserId, receivedMessage!!["id"].toString())
        assertEquals(testUsername, receivedMessage["username"].toString())
        assertEquals(testEmail, receivedMessage["email"].toString())

        logger.info { "End-to-end test passed successfully" }
    }

    private fun produceToSource(id: String, username: String, email: String) {
        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer::class.java.name)
            put("apicurio.registry.url", SOURCE_REGISTRY)
            put("apicurio.registry.auto-register", "true")
        }

        KafkaProducer<String, Any>(producerProps).use { producer ->
            val schema = org.apache.avro.Schema.Parser().parse(USER_SCHEMA)
            val user: Any = GenericData.Record(schema).apply {
                put("id", id)
                put("username", username)
                put("email", email)
            }

            val record = ProducerRecord<String, Any>(TEST_SOURCE_TOPIC, id, user)
            val metadata = producer.send(record).get()

            logger.info {
                "Produced to source - Topic: ${metadata.topic()}, " +
                        "Partition: ${metadata.partition()}, " +
                        "Offset: ${metadata.offset()}"
            }
        }
    }

    private fun consumeFromTarget(expectedKey: String): GenericRecord? {
        val consumerProps = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TARGET_BOOTSTRAP)
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put("apicurio.registry.url", TARGET_REGISTRY)
            put("apicurio.registry.use-specific-avro-reader", "false")
        }

        KafkaConsumer<String, GenericRecord>(consumerProps).use { consumer ->
            consumer.subscribe(listOf(TEST_TARGET_TOPIC))

            val endTime = System.currentTimeMillis() + 30000 // 30 second timeout

            while (System.currentTimeMillis() < endTime) {
                val records = consumer.poll(Duration.ofMillis(1000))

                for (record in records) {
                    logger.info {
                        "Consumed from target - Key: ${record.key()}, " +
                                "Partition: ${record.partition()}, " +
                                "Offset: ${record.offset()}"
                    }

                    if (record.key() == expectedKey) {
                        return record.value()
                    }
                }
            }
        }

        return null
    }
}
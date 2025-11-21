package com.yourcompany.kafkabridge

import io.apicurio.registry.rest.client.RegistryClient
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData
import io.apicurio.registry.rest.v2.beans.VersionMetaData
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.context.TestPropertySource
import java.io.InputStream
import java.time.Duration

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = ["test-source-1", "test-target-1", "test-target-1-dlt"],
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092", "port=9092"]
)
@TestPropertySource(properties = [
    "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
    "bridge.target-kafka-brokers=\${spring.embedded.kafka.brokers}",
    "bridge.topic-mappings.test-source-1=test-target-1",
    "spring.kafka.consumer.auto-offset-reset=earliest"
])
class SerdeIntegrationTest {

    @Autowired
    private lateinit var embeddedKafkaBroker: EmbeddedKafkaBroker

    @Autowired
    private lateinit var sourceTemplate: KafkaTemplate<String, Any>

    @Test
    fun `test avro serde migration from confluent to apicurio`() {
        // 1. Define Avro Schema
        val userSchemaStr = """
            {
              "type": "record", "name": "User", "namespace": "com.test.avro",
              "fields": [
                {"name": "username", "type": "string"},
                {"name": "age", "type": "int"}
              ]
            }
        """
        val schema = Schema.Parser().parse(userSchemaStr)

        // 2. Create Record
        val record = GenericData.Record(schema).apply {
            put("username", "ci-cd-user")
            put("age", 42)
        }

        // 3. Send to Source Topic (Simulating Confluent Source)
        sourceTemplate.send("test-source-1", "user-123", record).get()

        // 4. Consume from Target Topic (Simulating Apicurio Target)
        val consumerProps = KafkaTestUtils.consumerProps("test-validator", "true", embeddedKafkaBroker)
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        // We use StringDeserializer for value because we don't have a real Apicurio Registry to fetch the schema from.
        // We just want to prove the bytes arrived.
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        val consumer = DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()
        consumer.subscribe(listOf("test-target-1"))

        val records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(10000))

        if (records.count() == 0) {
            throw RuntimeException("Bridge did not produce message to target!")
        }

        val targetRecord = records.iterator().next()
        assertNotNull(targetRecord)
        assertEquals("user-123", targetRecord.key())

        println("✅ Successfully bridged message! Offset: ${targetRecord.offset()}")
    }

    @TestConfiguration
    @EnableKafka
    class TestConfig {

        // --- MOCKS ---
        val mockConfluentRegistry = MockSchemaRegistryClient()

        val mockApicurioRegistry: RegistryClient = mock {
            on {
                createArtifact(
                    any<String>(),
                    any<String>(),
                    any<String>(),
                    any<InputStream>()
                )
            } doReturn ArtifactMetaData().apply {
                globalId = 123L
                id = "User"
                version = "1"
                contentId = 456L
            }

            on {
                getArtifactVersionMetaDataByContent(
                    any<String>(),
                    any<String>(),
                    any<Boolean>(),
                    any<InputStream>()
                )
            } doReturn VersionMetaData().apply {
                globalId = 123L
                id = "User"
                version = "1"
                contentId = 456L
            }
        }

        // --- 1. SOURCE SIDE (Confluent) ---
        @Bean
        @Primary
        fun consumerFactory(
            @Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String
        ): ConsumerFactory<String, Any> {
            val config = mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "test-bridge-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                "schema.registry.url" to "mock://unused"
            )

            val avroDeserializer = KafkaAvroDeserializer(mockConfluentRegistry)
            avroDeserializer.configure(mapOf("schema.registry.url" to "mock://unused", "specific.avro.reader" to "false"), false)

            return DefaultKafkaConsumerFactory(
                config,
                StringDeserializer(),
                avroDeserializer as org.apache.kafka.common.serialization.Deserializer<Any>
            )
        }

        @Bean
        fun sourceTemplate(
            @Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String
        ): KafkaTemplate<String, Any> {
            val config = mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                "schema.registry.url" to "mock://unused"
            )
            val avroSerializer = KafkaAvroSerializer(mockConfluentRegistry)
            avroSerializer.configure(config, false)

            // EXPLICIT TYPES: <String, Any>
            val pf = DefaultKafkaProducerFactory<String, Any>(
                config,
                StringSerializer(),
                avroSerializer as Serializer<Any>
            )
            return KafkaTemplate<String, Any>(pf)
        }

        // --- 2. TARGET SIDE (Apicurio) ---
        @Bean
        @Primary
        fun targetProducerFactory(
            @Value("\${bridge.target-kafka-brokers}") bootstrapServers: String
        ): ProducerFactory<String, Any> {
            val config = mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.ACKS_CONFIG to "all",
                "apicurio.registry.url" to "http://mock-registry",
                "apicurio.registry.auto-register" to true
            )

            // FIX: Added <Any> to satisfy the generic type inference 'U'
            val apicurioSerializer = AvroKafkaSerializer<Any>(mockApicurioRegistry)
            apicurioSerializer.configure(config, false)

            // EXPLICIT TYPES: <String, Any>
            return DefaultKafkaProducerFactory<String, Any>(
                config,
                StringSerializer(),
                apicurioSerializer as Serializer<Any>
            )
        }

        @Bean
        @Primary
        fun dltRawProducerFactory(@Value("\${bridge.target-kafka-brokers}") bootstrapServers: String): ProducerFactory<String, Any> {
            return DefaultKafkaProducerFactory<String, Any>(
                mapOf<String, Any>(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
            )
        }
    }
}
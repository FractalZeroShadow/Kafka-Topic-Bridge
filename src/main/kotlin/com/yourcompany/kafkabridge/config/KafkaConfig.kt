package com.yourcompany.kafkabridge.config

import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution

private val logger = KotlinLogging.logger {}

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val sourceBootstrapServers: String,
    @Value("\${bridge.target-kafka-brokers}") private val targetBootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val consumerGroupId: String,
    @Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val sourceRegistryUrl: String,
    @Value("\${spring.kafka.producer.properties.apicurio.registry.url}") private val targetRegistryUrl: String,
    private val bridgeProperties: BridgeProperties,
    private val applicationContext: ApplicationContext
) {

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to sourceBootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",

            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to StringDeserializer::class.java.name,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to "io.confluent.kafka.serializers.KafkaAvroDeserializer",
            "schema.registry.url" to sourceRegistryUrl,
            "specific.avro.reader" to "false",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "100"
        )
        return DefaultKafkaConsumerFactory(config)
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Any>,
        targetKafkaTemplate: KafkaTemplate<String, Any>,
        dltRawKafkaTemplate: KafkaTemplate<String, Any>
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = consumerFactory
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

            // --- 1. Smart DLT Routing ---
            // Map value types to specific templates:
            // - ByteArray (Failed Deserialization) -> Raw Template (ByteArraySerializer)
            // - Object/GenericRecord (Infrastructure Failure) -> Target Template (AvroKafkaSerializer)
            val templates = mapOf<Class<*>, KafkaOperations<*, *>>(
                ByteArray::class.java to dltRawKafkaTemplate,
                Object::class.java to targetKafkaTemplate
            )

            val dltRecoverer = DeadLetterPublishingRecoverer(templates) { record, _ ->
                val dltTopic = bridgeProperties.dltMappings[record.topic()]
                    ?: "${record.topic()}.DLT"
                logger.warn { "Sending failed record to DLT: $dltTopic" }
                TopicPartition(dltTopic, -1)
            }

            // --- 2. Conditional Stop Strategy ---
            val smartRecoverer = { record: org.apache.kafka.clients.consumer.ConsumerRecord<*, *>, ex: Exception ->
                // A. Always send to DLT first
                try {
                    dltRecoverer.accept(record, ex)
                } catch (dltEx: Exception) {
                    logger.error(dltEx) { "Failed to publish to DLT!" }
                }

                // B. Check if Fatal or Retriable
                if (isFatal(ex)) {
                    logger.error(ex) { "FATAL (Serialization) Error. Record skipped. Application continuing." }
                    // Do NOT stop the app
                } else {
                    logger.error(ex) { "CRITICAL: Infrastructure retries exhausted. Service stopping." }
                    Thread {
                        SpringApplication.exit(applicationContext, { 1 })
                        System.exit(1)
                    }.start()
                }
            }

            // --- 3. BackOff Strategy (30s -> 3m -> 15m) ---
            val customBackOff = object : BackOff {
                override fun start(): BackOffExecution {
                    return object : BackOffExecution {
                        private val intervals = longArrayOf(30_000L, 180_000L, 900_000L)
                        private var index = 0
                        override fun nextBackOff(): Long =
                            if (index < intervals.size) intervals[index++] else BackOffExecution.STOP
                    }
                }
            }

            val errorHandler = DefaultErrorHandler(smartRecoverer, customBackOff)

            // --- 4. Define Non-Retriable Exceptions ---
            // These will skip the BackOff and go straight to DLT -> Continue
            errorHandler.addNotRetryableExceptions(
                SerializationException::class.java,
                DeserializationException::class.java,
                ClassCastException::class.java,
                IllegalArgumentException::class.java
            )

            setCommonErrorHandler(errorHandler)
        }
    }

    /**
     * Helper to identify fatal exceptions that should NOT trigger a restart.
     */
    private fun isFatal(ex: Throwable): Boolean {
        // Unpack Spring wrappers if necessary
        val cause = ex.cause ?: ex
        return cause is SerializationException ||
                cause is DeserializationException ||
                cause is ClassCastException ||
                cause is IllegalArgumentException
    }

    @Bean
    fun targetProducerFactory(): ProducerFactory<String, Any> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to targetBootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to AvroKafkaSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            "apicurio.registry.url" to targetRegistryUrl,
            "apicurio.registry.auto-register" to true,
            "apicurio.registry.artifact-id-strategy" to "io.apicurio.registry.serde.strategy.TopicNameStrategy",
            "apicurio.registry.headers.enabled" to false,
            "apicurio.registry.as-confluent" to true
        )
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun targetKafkaTemplate(targetProducerFactory: ProducerFactory<String, Any>): KafkaTemplate<String, Any> {
        return KafkaTemplate(targetProducerFactory)
    }

    // --- NEW: DLT Raw Producer (For Poison Pills/Bytes) ---
    @Bean
    fun dltRawProducerFactory(): ProducerFactory<String, Any> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to targetBootstrapServers,
            // DLT Keys are likely Strings (topic-partition), but if source key failed, it might be bytes.
            // StringSerializer is usually safe for keys in this context.
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,

            // CRITICAL: Use ByteArraySerializer for value to handle the raw "Poison Pill" bytes
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all"
        )
        logger.info { "Configured DLT RAW Producer (ByteArray Mode)" }
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun dltRawKafkaTemplate(dltRawProducerFactory: ProducerFactory<String, Any>): KafkaTemplate<String, Any> {
        return KafkaTemplate(dltRawProducerFactory)
    }
}
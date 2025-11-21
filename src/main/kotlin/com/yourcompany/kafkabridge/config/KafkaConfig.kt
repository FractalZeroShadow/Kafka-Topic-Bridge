package com.yourcompany.kafkabridge.config

import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.util.backoff.FixedBackOff

private val logger = KotlinLogging.logger {}

@Configuration
class KafkaConfig(
    @Value("\${spring.kafka.bootstrap-servers}") private val sourceBootstrapServers: String,
    @Value("\${bridge.target-kafka-brokers}") private val targetBootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id}") private val consumerGroupId: String,
    @Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val sourceRegistryUrl: String,
    @Value("\${spring.kafka.producer.properties.apicurio.registry.url}") private val targetRegistryUrl: String
) {

    // ... [Consumer Factory remains unchanged] ...
    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to sourceBootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
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
        consumerFactory: ConsumerFactory<String, Any>
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = consumerFactory
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

            // NEW: retries 3 times, then STOPS the consumer (Safe, requires restart)
            val errorHandler = DefaultErrorHandler(FixedBackOff(1000L, 3L))
            errorHandler.setSeeksAfterError(false) // Don't skip
            // If you want it to stop completely:
            // errorHandler.setClassifications(mapOf(Exception::class.java to true), false)
            // Or simply rely on monitoring to catch the error logs.

            setCommonErrorHandler(errorHandler)
        }
    }

    @Bean
    fun targetProducerFactory(): ProducerFactory<String, Any> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to targetBootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,

            // Use Apicurio Serializer
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to AvroKafkaSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",

            "apicurio.registry.url" to targetRegistryUrl,
            "apicurio.registry.auto-register" to true, // Pass as Boolean!
            "apicurio.registry.artifact-id-strategy" to "io.apicurio.registry.serde.strategy.TopicNameStrategy",

            // --- FIX: Use Booleans, not Strings ---
            // 1. Force ID into the Payload (Magic Byte + ID + Data)
            "apicurio.registry.headers.enabled" to false,

            // 2. Use 4-Byte Integer ID (Standard/Confluent format)
            "apicurio.registry.as-confluent" to true
        )

        logger.info { "Configured TARGET Producer (Apicurio/Confluent Mode) -> $targetRegistryUrl" }
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun targetKafkaTemplate(targetProducerFactory: ProducerFactory<String, Any>): KafkaTemplate<String, Any> {
        return KafkaTemplate(targetProducerFactory)
    }
}
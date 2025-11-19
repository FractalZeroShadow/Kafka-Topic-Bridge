package com.yourcompany.kafkabridge.config

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
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
    @Value("\${spring.kafka.consumer.properties.apicurio.registry.url}") private val sourceRegistryUrl: String,
    @Value("\${spring.kafka.producer.properties.apicurio.registry.url}") private val targetRegistryUrl: String
) {

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val config = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to sourceBootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,

            // Use ErrorHandlingDeserializer to wrap the actual deserializers
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,

            // Delegate to actual deserializers
            ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to StringDeserializer::class.java.name,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to AvroKafkaDeserializer::class.java.name,

            // Apicurio Registry settings for consumer
            "apicurio.registry.url" to sourceRegistryUrl,
            "apicurio.registry.use-specific-avro-reader" to "false",

            // Additional consumer configs for reliability
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "100",
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to "30000",
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to "10000"
        )

        logger.info { "Configured consumer - Bootstrap: $sourceBootstrapServers, Registry: $sourceRegistryUrl" }
        return DefaultKafkaConsumerFactory(config)
    }

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Any>
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = consumerFactory
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL

            // Error handler - now can handle SerializationExceptions
            setCommonErrorHandler(
                DefaultErrorHandler(
                    FixedBackOff(1000L, 3L) // Retry 3 times with 1 second interval
                )
            )

            // Concurrency - adjust based on your needs
            setConcurrency(1)
        }
    }

    @Bean
    fun targetProducerFactory(): ProducerFactory<String, Any> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to targetBootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to AvroKafkaSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",

            // Apicurio Registry settings for producer
            "apicurio.registry.url" to targetRegistryUrl,
            "apicurio.registry.auto-register" to "true",
            "apicurio.registry.use-specific-avro-reader" to "false",

            // Additional producer configs for reliability
            ProducerConfig.RETRIES_CONFIG to "3",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to "5",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "snappy",

            // Timeouts
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to "30000",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to "120000"
        )

        logger.info { "Configured producer - Bootstrap: $targetBootstrapServers, Registry: $targetRegistryUrl" }
        return DefaultKafkaProducerFactory(config)
    }

    @Bean
    fun targetKafkaTemplate(targetProducerFactory: ProducerFactory<String, Any>): KafkaTemplate<String, Any> {
        return KafkaTemplate(targetProducerFactory)
    }
}
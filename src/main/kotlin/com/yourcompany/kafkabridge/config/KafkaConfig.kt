package com.yourcompany.kafkabridge.config

import com.yourcompany.kafkabridge.error.BridgeSmartRecoverer
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution
import java.net.ConnectException
import java.net.SocketTimeoutException

private val logger = KotlinLogging.logger {}

@Configuration
@Suppress("unused")
class KafkaConfig(
    @param:Value("\${spring.kafka.bootstrap-servers}") private val sourceBootstrapServers: String,
    @param:Value("\${bridge.target-kafka-brokers}") private val targetBootstrapServers: String,
    @param:Value("\${spring.kafka.consumer.group-id}") private val consumerGroupId: String,
    @param:Value("\${spring.kafka.consumer.properties.schema.registry.url}") private val sourceRegistryUrl: String,
    @param:Value("\${spring.kafka.producer.properties.apicurio.registry.url}") private val targetRegistryUrl: String,
    private val bridgeProperties: BridgeProperties
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
        bridgeSmartRecoverer: BridgeSmartRecoverer
    ): ConcurrentKafkaListenerContainerFactory<String, Any> {
        return ConcurrentKafkaListenerContainerFactory<String, Any>().apply {
            this.consumerFactory = consumerFactory
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

            // Log the intervals to verify configuration is loaded
            logger.info { "Initializing BackOff with intervals: ${bridgeProperties.retryIntervalsMs}" }

            // --- BackOff Strategy ---
            val customBackOff = BackOff {
                object : BackOffExecution {
                    var index = 0
                    val intervals = bridgeProperties.retryIntervalsMs

                    override fun nextBackOff(): Long {
                        return if (index < intervals.size) {
                            intervals[index++]
                        } else {
                            BackOffExecution.STOP
                        }
                    }
                }
            }

            val errorHandler = DefaultErrorHandler(bridgeSmartRecoverer, customBackOff)

            // 1. FORCE RETRY on the entire chain of Network/Serialization errors
            // We must include SerializationException because it wraps the SocketTimeout
            // and is fatal by default.
            errorHandler.addRetryableExceptions(
                DeserializationException::class.java,
                SerializationException::class.java,
                SocketTimeoutException::class.java,
                ConnectException::class.java
            )

            // 2. FAIL FAST on Specific Permanent Errors (Override the above)
            errorHandler.addNotRetryableExceptions(
                ClassCastException::class.java,
                IllegalArgumentException::class.java,
                ArtifactNotFoundException::class.java // 404: Schema missing. Do not retry.
            )

            setCommonErrorHandler(errorHandler)
        }
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

    @Bean
    fun dltRawProducerFactory(): ProducerFactory<String, Any> {
        val config = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to targetBootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
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
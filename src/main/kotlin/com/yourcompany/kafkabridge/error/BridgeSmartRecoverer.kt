package com.yourcompany.kafkabridge.error

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import io.apicurio.registry.rest.client.exception.RestClientException
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.SerializationException
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.stereotype.Component
import java.net.ConnectException
import java.net.SocketTimeoutException

private val logger = KotlinLogging.logger {}

@Component
class BridgeSmartRecoverer(
    private val bridgeProperties: BridgeProperties,
    private val targetKafkaTemplate: KafkaTemplate<String, Any>,
    private val dltRawKafkaTemplate: KafkaTemplate<String, Any>,
    private val shutdownManager: ShutdownManager // Injected Manager
) : ConsumerRecordRecoverer {

    private val dltRecoverer: DeadLetterPublishingRecoverer by lazy {
        val templates = mapOf<Class<*>, KafkaOperations<*, *>>(
            ByteArray::class.java to dltRawKafkaTemplate,
            Object::class.java to targetKafkaTemplate
        )
        DeadLetterPublishingRecoverer(templates) { record, _ ->
            val dltTopic = bridgeProperties.dltMappings[record.topic()]
                ?: "${record.topic()}.DLT"
            logger.warn { "Routing failed record to DLT: $dltTopic" }
            TopicPartition(dltTopic, -1)
        }
    }

    override fun accept(record: ConsumerRecord<*, *>, ex: Exception) {
        // 1. Always attempt DLT first
        try {
            dltRecoverer.accept(record, ex)
        } catch (dltEx: Exception) {
            logger.error(dltEx) { "CRITICAL: Failed to publish to DLT!" }
        }

        // 2. Analyze Fatal vs Retryable
        val cause = ex.cause ?: ex

        if (cause is DeserializationException) {
            logger.error(ex) { "[CONSUMER-ERR] Source Poison Pill. Skipped to DLT (Raw)." }
        } else if (cause is SerializationException || cause is RestClientException) {
            if (isDataException(cause)) {
                logger.error(ex) { "[PRODUCER-ERR] Target Data Error (Schema/404). Skipped to DLT (Avro)." }
            } else {
                logger.error(ex) { "[PRODUCER-ERR] CRITICAL: Infrastructure retries exhausted. Service Stopping." }
                shutdownManager.shutdown() // Safe delegation
            }
        } else {
            logger.error(ex) { "[UNKNOWN-ERR] Unexpected fatal error. Service Stopping." }
            shutdownManager.shutdown() // Safe delegation
        }
    }

    private fun isDataException(ex: Throwable): Boolean {
        val cause = ex.cause ?: ex
        if (cause is DeserializationException) return true
        if (cause is ArtifactNotFoundException) return true
        if (cause is IllegalArgumentException) return true

        if (cause is RestClientException) {
            if (cause.cause is ConnectException || cause.cause is SocketTimeoutException) return false
            return true
        }
        if (cause is SerializationException) {
            val inner = cause.cause ?: return true
            return isDataException(inner)
        }
        if (cause is ConnectException || cause is SocketTimeoutException) return false
        return true
    }
}

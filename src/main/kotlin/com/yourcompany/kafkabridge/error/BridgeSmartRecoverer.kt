package com.yourcompany.kafkabridge.error

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
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

        // 2. Unwrap Spring Exceptions to find the real root cause
        val cause = ex.cause ?: ex
        val rootCause = findRootCause(ex)

        // 3. Analyze Fatal (Data) vs Retryable (Infra)
        if (isInfrastructureError(rootCause)) {
            logger.error(ex) { "[PRODUCER-ERR] CRITICAL: Infrastructure retries exhausted (Registry/Network). Service Stopping." }
            shutdownManager.shutdown()
        } else if (cause is DeserializationException) {
            logger.error(ex) { "[CONSUMER-ERR] Source Poison Pill. Skipped to DLT (Raw)." }
        } else if (isDataException(cause)) {
            logger.error(ex) { "[PRODUCER-ERR] Target Data Error (Schema/404). Skipped to DLT (Avro)." }
        } else {
            logger.error(ex) { "[UNKNOWN-ERR] Unexpected fatal error. Service Stopping." }
            shutdownManager.shutdown()
        }
    }

    private fun findRootCause(ex: Throwable): Throwable {
        var current = ex
        while (current.cause != null && current.cause != current) {
            current = current.cause!!
        }
        return current
    }

    private fun isInfrastructureError(ex: Throwable): Boolean {
        val msg = ex.message?.lowercase() ?: ""
        return ex is ConnectException ||
                ex is SocketTimeoutException ||
                msg.contains("connection refused") ||
                msg.contains("timed out") ||
                msg.contains("timeout")
    }

    private fun isDataException(ex: Throwable): Boolean {
        // If we already ruled out Infra, these are Data errors
        if (ex is DeserializationException) return true
        if (ex is ArtifactNotFoundException) return true
        if (ex is IllegalArgumentException) return true
        if (ex is SerializationException) return true
        return false
    }
}

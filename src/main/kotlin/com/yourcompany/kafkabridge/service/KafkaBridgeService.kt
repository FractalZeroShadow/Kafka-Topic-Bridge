package com.yourcompany.kafkabridge.service

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

private val logger = KotlinLogging.logger {}

@Service
class KafkaBridgeService(
    private val targetKafkaTemplate: KafkaTemplate<String, Any>,
    private val meterRegistry: MeterRegistry,
    // CHANGED: We inject BridgeProperties instead of @Value for timestamp/topics
    private val bridgeProperties: BridgeProperties
) {

    private val messagesProcessed: Counter = Counter.builder("kafka.bridge.messages.processed").register(meterRegistry)
    private val messagesFailed: Counter = Counter.builder("kafka.bridge.messages.failed").register(meterRegistry)
    private val bridgeTimer: Timer = Timer.builder("kafka.bridge.processing.time").register(meterRegistry)

    /**
     * SpEL expression fetches the set of keys from the 'topicMappings' map in BridgeProperties bean.
     */
    @KafkaListener(
        topics = ["#{bridgeProperties.topicMappings.keySet()}"],
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeAndProduce(
        record: ConsumerRecord<String, Any>,
        acknowledgment: Acknowledgment
    ) {
        val sample = Timer.start(meterRegistry)

        try {
            // 1. Resolve Target Topic from Map
            val targetTopicName = bridgeProperties.topicMappings[record.topic()]
                ?: throw IllegalStateException("No target mapping found for source topic: ${record.topic()}")

            // 2. Deserialize Value (GenericRecord from Confluent Deserializer)
            val avroValue = record.value()

            // 3. Timestamp Strategy from Properties
            val targetTimestamp = if (bridgeProperties.preserveTimestamp) {
                record.timestamp()
            } else {
                null
            }

            // 4. Header Filtering
            val ignoredHeaders = setOf(
                "apicurio.registry.global-id",
                "apicurio.registry.version",
                "apicurio.registry.content-hash"
            )

            val cleanHeaders = record.headers().mapNotNull { header ->
                if (header.key() in ignoredHeaders) null
                else RecordHeader(header.key(), header.value())
            }

            // 5. Construct ProducerRecord with MAPPED target name
            val producerRecord = ProducerRecord(
                targetTopicName,
                null,
                targetTimestamp,
                record.key(),
                avroValue,
                cleanHeaders
            )

            // 6. Send
            val future: CompletableFuture<SendResult<String, Any>> =
                targetKafkaTemplate.send(producerRecord)

            future.whenComplete { result, ex ->
                if (ex == null) {
                    logger.debug {
                        "Bridged ${record.topic()} -> $targetTopicName [Offset: ${result.recordMetadata.offset()}]"
                    }
                    acknowledgment.acknowledge()
                    messagesProcessed.increment()
                    sample.stop(bridgeTimer)
                } else {
                    logger.error(ex) { "Failed to send to target topic: $targetTopicName" }
                    messagesFailed.increment()
                    sample.stop(bridgeTimer)
                    // Throwing here triggers the container's ErrorHandler (Retry/Seek)
                    throw RuntimeException("Bridge send failed", ex)
                }
            }

        } catch (e: Exception) {
            logger.error(e) { "Fatal error bridging message key: ${record.key()}" }
            messagesFailed.increment()
            sample.stop(bridgeTimer)
            throw e
        }
    }
}
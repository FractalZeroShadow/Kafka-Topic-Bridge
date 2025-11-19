package com.yourcompany.kafkabridge.service

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
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
    @Value("\${bridge.target-topic}") private val targetTopic: String
) {

    private val messagesProcessed: Counter = Counter.builder("kafka.bridge.messages.processed")
        .description("Total number of messages successfully bridged")
        .register(meterRegistry)

    private val messagesFailed: Counter = Counter.builder("kafka.bridge.messages.failed")
        .description("Total number of messages that failed to bridge")
        .register(meterRegistry)

    private val bridgeTimer: Timer = Timer.builder("kafka.bridge.processing.time")
        .description("Time taken to bridge messages")
        .register(meterRegistry)

    @KafkaListener(
        topics = ["\${bridge.source-topic}"],
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeAndProduce(
        record: ConsumerRecord<String, Any>,
        acknowledgment: Acknowledgment
    ) {
        val sample = Timer.start(meterRegistry)

        try {
            logger.debug {
                "Received message - Topic: ${record.topic()}, " +
                        "Partition: ${record.partition()}, " +
                        "Offset: ${record.offset()}, " +
                        "Key: ${record.key()}"
            }

            // The value is a GenericRecord (Avro)
            val avroValue = record.value()

            // Log schema information if it's a GenericRecord
            if (avroValue is GenericRecord) {
                logger.debug { "Schema: ${avroValue.schema.fullName}" }
            }

            // Send to target cluster - the AvroKafkaSerializer will automatically
            // register the schema in the target registry
            val future: CompletableFuture<SendResult<String, Any>> =
                targetKafkaTemplate.send(targetTopic, record.key(), avroValue)

            val sendResult = future.get()

            logger.info {
                "Successfully bridged message - " +
                        "Key: ${record.key()}, " +
                        "Source: ${record.topic()}/${record.partition()}@${record.offset()}, " +
                        "Target: ${sendResult.recordMetadata.topic()}/" +
                        "${sendResult.recordMetadata.partition()}@${sendResult.recordMetadata.offset()}"
            }

            acknowledgment.acknowledge()
            messagesProcessed.increment()
            sample.stop(bridgeTimer)

        } catch (e: Exception) {
            logger.error(e) {
                "Failed to bridge message - " +
                        "Topic: ${record.topic()}, " +
                        "Partition: ${record.partition()}, " +
                        "Offset: ${record.offset()}, " +
                        "Key: ${record.key()}"
            }
            messagesFailed.increment()
            sample.stop(bridgeTimer)

            // Don't acknowledge - this will cause a retry based on your error handling strategy
            throw e
        }
    }
}
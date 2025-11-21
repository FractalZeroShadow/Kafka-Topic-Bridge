package com.yourcompany.kafkabridge.service

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import java.util.Optional
import java.util.concurrent.CompletableFuture

class KafkaBridgeServiceTest {

    private val kafkaTemplate: KafkaTemplate<String, Any> = mock()
    private val meterRegistry = SimpleMeterRegistry()
    private val bridgeProperties = BridgeProperties()

    private val service = KafkaBridgeService(kafkaTemplate, meterRegistry, bridgeProperties)

    @Test
    fun `should route to target topic and preserve headers`() {
        // Setup Mappings
        bridgeProperties.topicMappings = mapOf("source-topic" to "target-topic")
        bridgeProperties.preserveTimestamp = true

        // Setup Input with Headers
        val headers = RecordHeaders()
        headers.add(RecordHeader("my-header", "my-value".toByteArray()))
        headers.add(RecordHeader("apicurio.registry.global-id", "123".toByteArray())) // Should be filtered

        // Use full constructor to set a valid timestamp (>= 0)
        val inputRecord = ConsumerRecord<String, Any>(
            "source-topic",                   // topic
            0,                             // partition
            100L,                            // offset
            System.currentTimeMillis(),  // timestamp
            TimestampType.CREATE_TIME,               // timestampType
            0,                     // serializedKeySize
            0,                    // serializedValueSize
            "key",                            // key
            "value",                         // value
            headers,                                // headers
            Optional.empty()           // leaderEpoch
        )

        val ack: Acknowledgment = mock()

        // Mock Template Response
        whenever(kafkaTemplate.send(any<ProducerRecord<String, Any>>())).thenReturn(CompletableFuture.completedFuture(mock()))

        // Act
        service.consumeAndProduce(inputRecord, ack)

        // Assert
        argumentCaptor<ProducerRecord<String, Any>> {
            verify(kafkaTemplate).send(capture())

            val captured = firstValue
            assertEquals("target-topic", captured.topic())
            assertEquals("key", captured.key())
            assertEquals("value", captured.value())

            // Check Headers (Filter logic)
            val headerKeys = captured.headers().map { it.key() }
            assertEquals(listOf("my-header"), headerKeys, "Should preserve user headers and filter apicurio headers")
        }

        verify(ack).acknowledge()
    }
}
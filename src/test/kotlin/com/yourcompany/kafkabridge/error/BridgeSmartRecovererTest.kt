package com.yourcompany.kafkabridge.error

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import io.apicurio.registry.rest.client.exception.RestClientException
import io.apicurio.registry.rest.v2.beans.Error
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.lenient
import org.mockito.Mockito.never
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.check
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.DeserializationException
import java.net.ConnectException
import java.util.Optional
import java.util.concurrent.CompletableFuture

@ExtendWith(MockitoExtension::class)
class BridgeSmartRecovererTest {

    @Mock
    lateinit var bridgeProperties: BridgeProperties
    @Mock
    lateinit var targetKafkaTemplate: KafkaTemplate<String, Any>
    @Mock
    lateinit var dltRawKafkaTemplate: KafkaTemplate<String, Any>
    @Mock
    lateinit var shutdownManager: ShutdownManager

    private lateinit var recoverer: BridgeSmartRecoverer

    @BeforeEach
    fun setup() {
        whenever(bridgeProperties.dltMappings).thenReturn(mapOf("source-topic" to "target-dlt"))

        // Use typed any() to avoid ambiguity and allow safe stubbing
        lenient().whenever(targetKafkaTemplate.send(any<ProducerRecord<String, Any>>()))
            .thenReturn(CompletableFuture.completedFuture(null))
        lenient().whenever(dltRawKafkaTemplate.send(any<ProducerRecord<String, Any>>()))
            .thenReturn(CompletableFuture.completedFuture(null))

        recoverer = BridgeSmartRecoverer(
            bridgeProperties,
            targetKafkaTemplate,
            dltRawKafkaTemplate,
            shutdownManager
        )
    }

    @Test
    fun `should route Poison Pill (DeserializationException) to RAW DLT template`() {
        val rawData = "bad-bytes".toByteArray()
        val exception = DeserializationException("Bad bytes", rawData, false, RuntimeException("Cause"))

        // FIX: Pass 'rawData' as the record value.
        // This ensures the Recoverer has the data even if extracting it from the exception fails in the mock environment.
        val record = createRecord("source-topic", "key", rawData)

        recoverer.accept(record, exception)

        // Use 'check' with SAFE CAST (as?) to prevent NPE crashes
        verify(dltRawKafkaTemplate).send(check<ProducerRecord<String, Any>> {
            assertEquals("target-dlt", it.topic())

            val value = it.value()
            assertNotNull(value, "DLT value should not be null")

            // Safe cast: if value is not ByteArray, assertions will handle it cleanly
            assertArrayEquals(rawData, value as? ByteArray, "Poison bytes did not match")
        })

        // Verify Target template was NOT used
        verify(targetKafkaTemplate, never()).send(any<ProducerRecord<String, Any>>())
    }

    @Test
    fun `should route Producer Data Error (ArtifactNotFound) to TARGET DLT template`() {
        val avroValue = "some-avro-object"
        val apiError = Error()
        apiError.message = "Schema 1 not found"
        apiError.errorCode = 404
        val exception = ArtifactNotFoundException(apiError)
        val record = createRecord("source-topic", "key", avroValue)

        recoverer.accept(record, exception)

        verify(targetKafkaTemplate).send(check<ProducerRecord<String, Any>> {
            assertEquals("target-dlt", it.topic())
            assertEquals(avroValue, it.value())
        })

        verify(dltRawKafkaTemplate, never()).send(any<ProducerRecord<String, Any>>())
    }

    @Test
    fun `should route Infrastructure Error (Timeout) to DLT and Trigger Shutdown`() {
        val apiError = Error()
        apiError.message = "Timeout"
        apiError.errorCode = 500
        val exception = RestClientException(apiError)
        val cause = ConnectException("Connection refused")
        exception.initCause(cause)

        val record = createRecord("source-topic", "key", "valid-value")

        recoverer.accept(record, exception)

        // 1. Verify DLT attempt
        verify(targetKafkaTemplate).send(any<ProducerRecord<String, Any>>())

        // 2. Verify Shutdown is requested
        verify(shutdownManager).shutdown()
    }

    private fun createRecord(topic: String, key: String, value: Any?): ConsumerRecord<String, Any> {
        return ConsumerRecord(
            topic, 0, 0L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
            0, 0, key, value, RecordHeaders(), Optional.empty()
        )
    }
}
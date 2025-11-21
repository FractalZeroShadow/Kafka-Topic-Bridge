package com.yourcompany.kafkabridge

import io.apicurio.registry.rest.client.RegistryClient
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData
import io.apicurio.registry.rest.v2.beans.VersionMetaData
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import java.util.concurrent.CompletableFuture

class SerdeUnitTest {

    @Test
    fun `should serialize record using Apicurio after reading from Confluent`() {
        // --- 1. PREPARE DATA ---
        val validMetadata = ArtifactMetaData().apply {
            globalId = 999L
            id = "TestArtifact"
            version = "1"
            contentId = 456L
        }
        val validVersionMetadata = VersionMetaData().apply {
            globalId = 999L
            id = "TestArtifact"
            version = "1"
            contentId = 456L
        }

        // --- 2. CREATE SMART MOCK ---
        // 'returnType' is now the subject of when expression
        val defaultAnswer = Answer { invocation ->
            val returnType = invocation.method.returnType
            when (returnType) {
                ArtifactMetaData::class.java -> validMetadata
                VersionMetaData::class.java -> validVersionMetadata
                // Some older Apicurio versions return CompletableFuture
                CompletableFuture::class.java -> CompletableFuture.completedFuture(validMetadata)
                else -> Mockito.RETURNS_DEFAULTS.answer(invocation)
            }
        }

        val apicurioRegistry: RegistryClient = Mockito.mock(RegistryClient::class.java, defaultAnswer)

        // --- 3. SETUP SERIALIZERS ---
        val confluentRegistry = MockSchemaRegistryClient()

        val confluentSerializer = KafkaAvroSerializer(confluentRegistry)
        confluentSerializer.configure(mapOf("schema.registry.url" to "mock://src"), false)

        val confluentDeserializer = KafkaAvroDeserializer(confluentRegistry)
        confluentDeserializer.configure(mapOf("schema.registry.url" to "mock://src", "specific.avro.reader" to "false"), false)

        val apicurioSerializer = AvroKafkaSerializer<Any>(apicurioRegistry)
        apicurioSerializer.configure(mapOf(
            "apicurio.registry.url" to "http://mock-target",
            "apicurio.registry.auto-register" to true
        ), false)

        // --- 4. EXECUTE FLOW ---
        val schemaStr = """{"type":"record","name":"Test","fields":[{"name":"f1","type":"string"}]}"""
        val schema = Schema.Parser().parse(schemaStr)
        val originalRecord = GenericData.Record(schema).apply { put("f1", "test-value") }

        // Confluent: Serialize -> Deserialize
        val sourceBytes = confluentSerializer.serialize("topic-A", originalRecord)
        val deserializedRecord = confluentDeserializer.deserialize("topic-A", sourceBytes) as GenericData.Record

        // Apicurio: Serialize (Triggers our Smart Mock)
        val targetBytes = apicurioSerializer.serialize("topic-B", deserializedRecord)

        // --- 5. ASSERT ---
        assertTrue(targetBytes.isNotEmpty())
        assertEquals(0x00.toByte(), targetBytes[0], "Should have Magic Byte")

        println("SerDe Logic Verified. Confluent Bytes: ${sourceBytes.size} -> Apicurio Bytes: ${targetBytes.size}")
    }
}
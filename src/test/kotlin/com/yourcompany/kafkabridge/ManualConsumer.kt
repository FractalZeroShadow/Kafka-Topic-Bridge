package com.yourcompany.kafkabridge

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import kotlin.system.exitProcess

fun main() {
    println("--- STARTING APICURIO TARGET CONSUMER (SMART EXIT) ---")

    val props = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:29093",
        ConsumerConfig.GROUP_ID_CONFIG to "manual-cli-consumer-${System.currentTimeMillis()}",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to AvroKafkaDeserializer::class.java.name,
        "apicurio.registry.url" to "http://localhost:8082/apis/registry/v2",
        "apicurio.registry.use-specific-avro-reader" to "false",
        "apicurio.registry.as-confluent" to "true"
    )

    var totalRecords = 0
    var emptyPolls = 0
    val maxEmptyPolls = 5 // Exit after 5 seconds of silence

    try {
        KafkaConsumer<String, GenericRecord>(props).use { consumer ->
            val topics = listOf("test-target-1", "test-target-2", "test-target-3")
            consumer.subscribe(topics)
            println("Subscribed to: $topics")
            println("Reading messages... (Will exit after $maxEmptyPolls seconds of silence)")

            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))

                if (records.isEmpty) {
                    emptyPolls++
                    print(".") // heartbeat dot
                    if (emptyPolls >= maxEmptyPolls) {
                        println("\n\n--- IDLE TIMEOUT REACHED ---")
                        break
                    }
                } else {
                    // Reset idle counter if we got data
                    emptyPolls = 0
                    for (record in records) {
                        totalRecords++
                        println("\n[${record.topic()} | Offset: ${record.offset()}] Key: ${record.key()}")
                        // Simple output to avoid flooding console
                        println("   -> Schema: ${record.value().schema.name}")
                        println("   -> Value:  ${record.value()}")
                    }
                }
            }
        }
    } catch (e: Exception) {
        println("\n\n!!! CRITICAL FAILURE !!!")
        e.printStackTrace()
        exitProcess(1)
    }

    println("\n--- SUMMARY ---")
    if (totalRecords > 0) {
        println("SUCCESS: Consumed $totalRecords records.")
        exitProcess(0)
    } else {
        println("FAILURE: No records found before timeout.")
        exitProcess(1)
    }
}
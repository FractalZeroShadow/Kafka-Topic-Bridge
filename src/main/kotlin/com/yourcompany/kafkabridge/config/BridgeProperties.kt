package com.yourcompany.kafkabridge.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component("bridgeProperties")
@ConfigurationProperties(prefix = "bridge")
class BridgeProperties {

    // Map of Source Topic -> Target Topic.
    var topicMappings: Map<String, String> = mutableMapOf()

    // Map of Source Topic -> Dead Letter Topic (DLT).
    var dltMappings: Map<String, String> = mutableMapOf()

    var preserveTimestamp: Boolean = true

    lateinit var targetKafkaBrokers: String

    // Default Backoff: 30s, 5m, 15m
    var retryIntervalsMs: List<Long> = listOf(30_000L, 300_000L, 900_000L)
}
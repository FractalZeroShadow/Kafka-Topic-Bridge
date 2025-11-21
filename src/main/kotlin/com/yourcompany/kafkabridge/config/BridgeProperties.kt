package com.yourcompany.kafkabridge.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "bridge")
class BridgeProperties {
    /**
     * Map of Source Topic -> Target Topic.
     * Key: The topic name to consume from (e.g. "bad-legacy-name")
     * Value: The topic name to produce to (e.g. "clean-new-name")
     */
    var topicMappings: Map<String, String> = mutableMapOf()

    var preserveTimestamp: Boolean = true

    lateinit var targetKafkaBrokers: String
}
package com.yourcompany.kafkabridge.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "bridge")
class BridgeProperties {

     // Map of Source Topic -> Target Topic.
    var topicMappings: Map<String, String> = mutableMapOf()

     // Map of Source Topic -> Dead Letter Topic (DLT). Used when retries are exhausted.
    var dltMappings: Map<String, String> = mutableMapOf()

    var preserveTimestamp: Boolean = true

    lateinit var targetKafkaBrokers: String
}

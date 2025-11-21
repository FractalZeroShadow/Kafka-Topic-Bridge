package com.yourcompany.kafkabridge

import com.yourcompany.kafkabridge.config.BridgeProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(BridgeProperties::class)
class KafkaBridgeApplication

fun main(args: Array<String>) {
    runApplication<KafkaBridgeApplication>(*args)
}
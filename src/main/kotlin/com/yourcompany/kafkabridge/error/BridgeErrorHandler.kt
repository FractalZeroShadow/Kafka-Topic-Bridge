package com.yourcompany.kafkabridge.error

import com.yourcompany.kafkabridge.config.BridgeProperties
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.stereotype.Component
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.BackOffExecution

@Component
class BridgeErrorHandler(
    recoverer: BridgeSmartRecoverer,
    properties: BridgeProperties
) : DefaultErrorHandler(
    recoverer, // Pass logic to super
    ConfigurableBackOff(properties.retryIntervalsMs) // Pass backoff to super
) {

    init {
        // Define Fatal Exceptions (Skip Backoff, go straight to Recoverer)
        this.addNotRetryableExceptions(
            DeserializationException::class.java,
            ClassCastException::class.java,
            IllegalArgumentException::class.java,
            ArtifactNotFoundException::class.java
        )
    }

    private class ConfigurableBackOff(private val intervals: List<Long>) : BackOff {
        override fun start(): BackOffExecution {
            return object : BackOffExecution {
                private var index = 0
                override fun nextBackOff(): Long {
                    return if (index < intervals.size) intervals[index++] else BackOffExecution.STOP
                }
            }
        }
    }
}
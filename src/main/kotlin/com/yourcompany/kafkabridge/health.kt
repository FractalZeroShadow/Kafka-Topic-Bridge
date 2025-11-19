package com.yourcompany.kafkabridge.health

import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component
import java.util.Properties
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

@Component
class KafkaHealthIndicator(
    @Value("\${spring.kafka.bootstrap-servers}") private val sourceBootstrapServers: String,
    @Value("\${bridge.target-kafka-brokers}") private val targetBootstrapServers: String
) : HealthIndicator {

    override fun health(): Health {
        return try {
            val sourceHealthy = checkKafkaCluster(sourceBootstrapServers, "source")
            val targetHealthy = checkKafkaCluster(targetBootstrapServers, "target")

            if (sourceHealthy && targetHealthy) {
                Health.up()
                    .withDetail("source-cluster", sourceBootstrapServers)
                    .withDetail("target-cluster", targetBootstrapServers)
                    .build()
            } else {
                Health.down()
                    .withDetail("source-cluster-healthy", sourceHealthy)
                    .withDetail("target-cluster-healthy", targetHealthy)
                    .build()
            }
        } catch (e: Exception) {
            logger.error(e) { "Health check failed" }
            Health.down(e).build()
        }
    }

    private fun checkKafkaCluster(bootstrapServers: String, clusterName: String): Boolean {
        return try {
            val props = Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
            }

            AdminClient.create(props).use { adminClient ->
                val result = adminClient.listTopics()
                result.names().get(5, TimeUnit.SECONDS)
                logger.debug { "$clusterName cluster is healthy" }
                true
            }
        } catch (e: Exception) {
            logger.warn { "$clusterName cluster health check failed: ${e.message}" }
            false
        }
    }
}
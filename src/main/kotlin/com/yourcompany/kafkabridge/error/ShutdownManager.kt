package com.yourcompany.kafkabridge.error

import org.springframework.boot.SpringApplication
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import kotlin.system.exitProcess

@Component
class ShutdownManager(private val applicationContext: ApplicationContext) {

    fun shutdown() {
        Thread {
            // Pass the lambda INSIDE parentheses
            val exitCode = SpringApplication.exit(applicationContext, { 1 })
            exitProcess(exitCode)
        }.start()
    }
}
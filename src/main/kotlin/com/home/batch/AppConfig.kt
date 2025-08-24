package com.home.batch

import org.springframework.batch.core.repository.ExecutionContextSerializer
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AppConfig {
    @Bean
    fun jacksonSerializer(): ExecutionContextSerializer {
        return Jackson2ExecutionContextStringSerializer()
    }
}

package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.CacheManager
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CacheConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun cacheManager(meterRegistry: MeterRegistry) = CacheManager(meterRegistry) {
        inMemory()
    }
}
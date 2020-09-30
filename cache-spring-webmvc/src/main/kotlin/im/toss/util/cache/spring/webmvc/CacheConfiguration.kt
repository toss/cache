package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.CacheManager
import im.toss.util.cache.CacheResourcesDsl
import im.toss.util.cache.spring.TossCacheProperties
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CacheConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun tossCacheManager(
        meterRegistry: MeterRegistry,
        tossCacheProperties: TossCacheProperties,
        dsl: List<CacheResourcesDsl>
    ) = CacheManager(meterRegistry).also { cacheManager ->
        cacheManager.loadProperties(tossCacheProperties.cache, "toss.cache")
        dsl.forEach { cacheManager.resources(it) }
    }
}
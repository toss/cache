package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.spring.TossCacheConfiguration
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(
    TossCacheConfiguration::class,
    ResponseCacheFilter::class,
    ResponseCacheSupportAnnotationAspect::class,
    EvictCacheAnnotationAspect::class
)
annotation class EnableCacheSupport

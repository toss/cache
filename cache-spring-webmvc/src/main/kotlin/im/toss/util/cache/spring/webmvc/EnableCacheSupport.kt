package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.spring.CacheGroupManager
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(
    CacheGroupManager::class,
    ResponseCacheFilter::class,
    ResponseCacheSupportAnnotationAspect::class,
    EvictCacheAnnotationAspect::class
)
annotation class EnableCacheSupport

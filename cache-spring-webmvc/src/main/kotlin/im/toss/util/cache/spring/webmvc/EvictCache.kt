package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.CacheManager
import mu.KotlinLogging
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.stereotype.Component
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes

private val logger = KotlinLogging.logger {}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class EvictCache(val groupId: Array<String>)

@Component
@ConditionalOnBean(annotation = [EnableCacheSupport::class])
@Aspect
class EvictCacheAnnotationAspect(
    val cacheManager: CacheManager
) {
    @Around("@annotation(im.toss.util.cache.spring.webmvc.EvictCache)")
    fun endPoint(pjp: ProceedingJoinPoint): Any? {
        val definition = pjp.getMethodAnnotation<EvictCache>() ?: return pjp.proceed()
        val attributes = RequestContextHolder.currentRequestAttributes() as? ServletRequestAttributes ?: return pjp.proceed()
        val method = (pjp.signature as MethodSignature).method
        val cacheKey = CacheKey.getIdentifier(method)
        if (cacheKey.isEmpty()) {
            logger.warn("ignored cache evict because the cache key definition not found.\nmethod: ${method}")
            return pjp.proceed()
        }
        val key  = cacheKey.get(attributes.request)
        logger.debug("EvictCache: groupId=\"${definition.groupId}\", cacheKey=\"${key}\"")

        return try {
            pjp.proceed()
        } finally {
            definition.groupId.forEach {
                try {
                    cacheManager.multiFieldCache<String>(it, "ByteArraySerializer").blocking.evict(key)
                } catch (e: Throwable) {
                    logger.warn("failed to evict cache: groupId=\"${definition.groupId}\", cacheKey=\"${key}\"")
                }
            }
        }
    }
}


package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.spring.CacheGroupManager
import im.toss.util.data.container.Pack
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
annotation class ResponseCacheSupport(val groupId: String)

@Component
@ConditionalOnBean(annotation = [EnableCacheSupport::class])
@Aspect
class ResponseCacheSupportAnnotationAspect(
    val cacheGroupManager: CacheGroupManager
) {
    @Around("@annotation(im.toss.util.cache.spring.webmvc.ResponseCacheSupport)")
    fun endPoint(pjp: ProceedingJoinPoint): Any? {
        val definition = pjp.getMethodAnnotation<ResponseCacheSupport>() ?: return pjp.proceed()
        val attributes = RequestContextHolder.currentRequestAttributes() as? ServletRequestAttributes ?: return pjp.proceed()
        attributes.request.getBestMatchingPattern() ?: return pjp.proceed()
        val response = attributes.response as? CaptureHttpServletResponse ?: return pjp.proceed()
        val method = (pjp.signature as MethodSignature).method
        val cacheKeyBuilder = CacheKeyBuilder.from(method)
        if (cacheKeyBuilder.keys.isEmpty()) {
            logger.warn("ignored cache because the cache key definition not found.\nmethod: ${method}")
            return pjp.proceed()
        }

        val key = cacheKeyBuilder.buildKey(attributes.request)
        val field = cacheKeyBuilder.buildField(attributes.request)

        logger.debug("ResponseCacheSupport: groupId=\"${definition.groupId}\", cacheKey=\"${key}\", cacheField=\"${field}\"")

        val cacheControl = attributes.request.parseHeadersForCache()
        val cache = cacheGroupManager.getBlocking(definition.groupId)
        val command = when {
            // 캐시 필드의 내용을 강제 로딩 & 저장 한다.
            cacheControl.isNeedsInvalidate -> {
                cache.optimisticLockForLoad<ByteArray>(key, field).let {
                    CacheCommand.LoadToCache(key, field, it)
                }
            }

            // 캐시된 응답이 있으면, 캐싱된 응답을 하고, 없으면 캐시에 로딩한다
            else -> {
                val result = cache.getOrLockForLoad<ByteArray>(key, field)
                when {
                    result.value != null -> {
                        try {
                            CacheCommand.CachedResponse(key, field, Pack(result.value!!).validate())
                        } catch (e: Throwable) {
                            cache.optimisticLockForLoad<ByteArray>(key, field).let {
                                CacheCommand.LoadToCache(key, field, it)
                            }
                        }
                    }
                    result.loader != null -> CacheCommand.LoadToCache(key, field, result.loader!!)
                    else -> CacheCommand.Noop() // 캐시를 사용하지 않는다. coldTime이거나 evictionOnly인 경우일 수 있다.
                }
            }
        }

        response.activateCaptureMode()
        return CacheCommand.setCurrent(command).fetchOriginIfNeeds { pjp.proceed() }
    }
}

inline fun <reified T : Annotation?> ProceedingJoinPoint.getMethodAnnotation(): T? {
    val signature = signature as MethodSignature
    return signature.method.getAnnotation(T::class.java)
}

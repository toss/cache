package im.toss.util.cache

import kotlinx.coroutines.TimeoutCancellationException
import mu.KotlinLogging
import java.lang.Exception

private val logger = KotlinLogging.logger {}

/**
 * 캐시 장애시의 정책
 */
enum class CacheFailurePolicy {
    /**
     * 예외를 발생시킨다.
     */
    ThrowException,

    /**
     * Origin 데이터로 Fallback을 한다.
     *
     * 이 정책을 사용하면, 캐시 장애 시에도 origin으로부터 데이터는 제공되지만, origin의 접근이 증가하게 되고,
     * origin에 변경이 발생했을 때, 장애 해소 이후 캐시 저장소가 이전 상태로 복구되면 데이터 불일치가 발생한다.
     * 장애 이후에는, 캐시 저장소를 비워주거나, 캐시 버전업을 권장한다.
     */
    FallbackToOrigin
}

fun CacheFailurePolicy.handle(message: String, e: Throwable) {
    if (this == CacheFailurePolicy.FallbackToOrigin) {
        logger.warn("[SKIPPED] $message", e)
    } else {
        logger.warn("[FAILURE] message", e)
        if (e is TimeoutCancellationException) {
            throw e
        } else {
            throw CacheFailureException(e)
        }
    }
}

class CacheFailureException(cause: Throwable): Exception(cause)

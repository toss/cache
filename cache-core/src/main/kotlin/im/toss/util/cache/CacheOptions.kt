package im.toss.util.cache

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Duration
import java.util.concurrent.TimeUnit

data class CacheOptions(
    @field:JsonProperty("version")
    var version: String,

    @field:JsonProperty("cache-mode")
    var cacheMode: CacheMode = CacheMode.NORMAL,

    @field:JsonProperty("ttl")
    var ttl: Duration = Duration.ofSeconds(10L),

    @field:JsonProperty("apply-ttl-if-hit")
    var applyTtlIfHit: Boolean = true,

    @field:JsonProperty("cold-time")
    var coldTime: Duration = Duration.ofSeconds(-1L),

    @field:JsonProperty("lock-timeout")
    var lockTimeout: Duration = Duration.ofSeconds(30L),

    @field:JsonProperty("operation-timeout")
    var operationTimeout: Duration = Duration.ofSeconds(-1L),

    @field:JsonProperty("cache-failure-policy")
    var cacheFailurePolicy: CacheFailurePolicy = CacheFailurePolicy.ThrowException
)

fun cacheOptions(
    version: String? = null,
    cacheMode: CacheMode? = null,
    ttl: Long? = null,
    ttlTimeUnit: TimeUnit? = null,
    applyTtlIfHit: Boolean? = null,
    coldTime: Long? = null,
    coldTimeUnit: TimeUnit? = null,
    lockTimeout: Long? = null,
    lockTimeoutTimeUnit: TimeUnit? = null,
    cacheFailurePolicy: CacheFailurePolicy? = null
): CacheOptions {
    return CacheOptions(
        version = version ?: "0001",
        cacheMode = cacheMode ?: CacheMode.NORMAL,
        ttl = Duration.ofMillis((ttlTimeUnit?:TimeUnit.SECONDS).toMillis(ttl ?: 10L)),
        applyTtlIfHit = applyTtlIfHit ?: false,
        coldTime = Duration.ofMillis((coldTimeUnit?:TimeUnit.SECONDS).toMillis(coldTime ?: -1L)),
        lockTimeout = Duration.ofMillis((lockTimeoutTimeUnit?:TimeUnit.SECONDS).toMillis(lockTimeout ?: 30L)),
        cacheFailurePolicy = cacheFailurePolicy ?: CacheFailurePolicy.ThrowException
    )
}
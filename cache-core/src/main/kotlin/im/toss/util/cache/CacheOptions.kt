package im.toss.util.cache

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.random.Random

data class CacheOptions(
    @field:JsonProperty("version")
    var version: String,

    @field:JsonProperty("cache-mode")
    var cacheMode: CacheMode = CacheMode.NORMAL,

    @field:JsonProperty("ttl")
    var ttl: Duration = Duration.ofSeconds(10L),

    @field:JsonProperty("apply-ttl-if-hit")
    var applyTtlIfHit: Boolean = true,

    @field:JsonProperty("apply-ttl-if-hit-ratio")
    var applyTtlIfHitRatio: Float = 1.0f,

    @field:JsonProperty("cold-time")
    var coldTime: Duration = Duration.ofSeconds(-1L),

    @field:JsonProperty("lock-timeout")
    var lockTimeout: Duration = Duration.ofSeconds(30L),

    @field:JsonProperty("operation-timeout")
    var operationTimeout: Duration = Duration.ofSeconds(-1L),

    @field:JsonProperty("cache-failure-policy")
    var cacheFailurePolicy: CacheFailurePolicy = CacheFailurePolicy.ThrowException,

    @field:JsonProperty("isolation-by-type")
    var isolationByType: Boolean = false,

    @Deprecated("changed to run-with-isolated-context, remove after 0.6.0")
    @field:JsonProperty("run-with-isolated-thread")
    var runWithIsolatedThread: Boolean = false,

    @field:JsonProperty("run-with-isolated-context")
    var runWithIsolatedContext: Boolean = false,

    @field:JsonProperty("enable-pessimistic-lock")
    var enablePessimisticLock: Boolean = true,

    @field:JsonProperty("enable-optimistic-lock")
    var enableOptimisticLock: Boolean = true,

    @field:JsonProperty("multi-parallelism")
    var multiParallelism: Int = 4,
)

fun CacheOptions.isApplyTtlIfHit(): Boolean {
    return applyTtlIfHit && ttl.toMillis() > 0L && isProbability(applyTtlIfHitRatio)
}

fun isProbability(n: Float): Boolean = Random.nextFloat() < n

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
    cacheFailurePolicy: CacheFailurePolicy? = null,
    isolationByType: Boolean? = null,
    enableOptimisticLock: Boolean? = null,
    enablePessimisticLock: Boolean? = null,
    multiParallelism: Int? = null
): CacheOptions {
    return CacheOptions(
        version = version ?: "0001",
        cacheMode = cacheMode ?: CacheMode.NORMAL,
        ttl = Duration.ofMillis((ttlTimeUnit?:TimeUnit.SECONDS).toMillis(ttl ?: 10L)),
        applyTtlIfHit = applyTtlIfHit ?: false,
        coldTime = Duration.ofMillis((coldTimeUnit?:TimeUnit.SECONDS).toMillis(coldTime ?: -1L)),
        lockTimeout = Duration.ofMillis((lockTimeoutTimeUnit?:TimeUnit.SECONDS).toMillis(lockTimeout ?: 30L)),
        cacheFailurePolicy = cacheFailurePolicy ?: CacheFailurePolicy.ThrowException,
        isolationByType = isolationByType ?: false,
        enablePessimisticLock = enablePessimisticLock ?: true,
        enableOptimisticLock = enableOptimisticLock ?: true,
        multiParallelism = multiParallelism ?: 4
    )
}
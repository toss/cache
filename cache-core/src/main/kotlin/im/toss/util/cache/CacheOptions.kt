package im.toss.util.cache

import java.util.concurrent.TimeUnit

class CacheOptions(
    val cacheFailurePolicy: CacheFailurePolicy = CacheFailurePolicy.ThrowException
) {
    var version: String = "0001"
    var cacheMode: CacheMode = CacheMode.NORMAL
    var ttl: Long = 10L
    var ttlTimeUnit: TimeUnit = TimeUnit.SECONDS
    var applyTtlIfHit: Boolean = true
    var coldTime: Long = -1L
    var coldTimeUnit: TimeUnit = TimeUnit.SECONDS
    var lockTimeout: Long = 30L
    var lockTimeoutTimeUnit: TimeUnit = TimeUnit.SECONDS
}

fun cacheOptions(
    version: String? = null,
    cacheMode: CacheMode? = null,
    ttl: Long? = null,
    ttlTimeUnit: TimeUnit? = null,
    applyTtlIfHit: Boolean? = null,
    coldTime: Long? = null,
    coldTimeUnit: TimeUnit? = null,
    lockTimeout: Long? = null,
    lockTImeoutTimeUnit: TimeUnit? = null,
    cacheFailurePolicy: CacheFailurePolicy? = null
): CacheOptions {
    val options = if (cacheFailurePolicy != null) {
        CacheOptions(cacheFailurePolicy)
    } else {
        CacheOptions()
    }

    if (version != null) options.version = version
    if (cacheMode != null) options.cacheMode = cacheMode
    if (ttl != null) options.ttl = ttl
    if (ttlTimeUnit != null) options.ttlTimeUnit = ttlTimeUnit
    if (applyTtlIfHit != null) options.applyTtlIfHit = applyTtlIfHit
    if (coldTime != null) options.coldTime = coldTime
    if (coldTimeUnit != null) options.coldTimeUnit = coldTimeUnit
    if (lockTimeout != null) options.lockTimeout = lockTimeout
    if (lockTImeoutTimeUnit != null) options.lockTimeoutTimeUnit = lockTImeoutTimeUnit

    return options
}
package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.runOrRetry
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.Serializer
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.TimeoutCancellationException
import mu.KotlinLogging
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

data class MultiFieldCache<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    val options: CacheOptions,
    private val metrics: CacheMetrics = CacheMetrics(name),
    private val typeName: String = "MultiFieldCache"
) : Cache, CacheMeter by metrics {
    val blocking by lazy { BlockingMultiFieldCache(this) }

    private val keys = Keys<TKey>(name, keyFunction, options)

    suspend fun evict(key: TKey) {
        metrics.incrementEvictCount()
        setColdTime(key)
        setEvicted(key)
        repository.delete(keys.key(key))
    }

    suspend fun <T: Any> load(key: TKey, field: String, fetch: (suspend () -> T)) {
        runWithTimeout(options.operationTimeout.toMillis()) {
            if (options.cacheMode == CacheMode.EVICTION_ONLY) {
                evict(key)
            } else {
                try {
                    runOrRetry {
                        loadToCache(key, field, fetch)
                    }
                } catch (e: Throwable) {
                    options.cacheFailurePolicy.handle(
                        "$typeName.load(): exception occured on load: cache=$name, key=$key, field=$field", e
                    )
                }
            }
        }
    }

    private suspend fun <T: Any> loadToCache(key: TKey, field: String, fetch: (suspend () -> T)): T {
        val lock = lockForLoad<T>(key, field)
        try {
            val value = fetch()
            lock.load(value)
            return value
        } finally {
            lock.release()
        }
    }

    @Throws(MutexLock.FailedAcquireException::class)
    suspend fun <T:Any> lockForLoad(key: TKey, field: String, timeout: Long = -1): CacheValueLoader<T> {
        val lockKey = keys.fetchKey(key, field)
        if (!lock.acquire(lockKey, timeout)) {
            throw MutexLock.FailedAcquireException
        }
        return PessimisticKeyFieldCacheValueLoader(key, field, setNotEvicted(key, field), lockKey)
    }

    suspend fun <T:Any> optimisticLockForLoad(key: TKey, field: String): CacheValueLoader<T> {
        return OptimisticKeyFieldCacheValueLoader(key, field, setNotEvicted(key, field))
    }

    private suspend fun <T:Any> compareAndLoad(key: TKey, field: String, version: Long, value: T): LoadResult<T> {
        return if (isNotEvicted(key, field, version)) {
            val dataBytes = serializer.serialize(value)
            repository.set(keys.key(key), keys.field(field), dataBytes, options.ttl.toMillis(), TimeUnit.MILLISECONDS)
            metrics.incrementPutCount()
            LoadResult(value)
        } else {
            repository.delete(keys.key(key), keys.field(field))
            logger.info("optimistic lock failure occured on load, purged cached data. cache=$name, key=$key, field=$field")
            LoadResult(value, success = false, isOptimisticLockFailure = true)
        }
    }

    inner class PessimisticKeyFieldCacheValueLoader<T: Any>(
        val key: TKey,
        val field: String,
        override val version: Long,
        private val lockKey: String
    ): CacheValueLoader<T> {
        private var loaded = AtomicBoolean(false)
        override suspend fun load(value: T): LoadResult<T> {
            if (loaded.compareAndSet(false, true)) {
                return try {
                    compareAndLoad(key, field, version, value)
                } finally {
                    lock.release(lockKey)
                }
            } else {
                throw AlreadyLoadedException()
            }
        }

        override suspend fun release() {
            if (loaded.compareAndSet(false, true)) {
                lock.release(lockKey)
            }
        }
    }

    inner class OptimisticKeyFieldCacheValueLoader<T: Any>(
        val key: TKey,
        val field: String,
        override val version: Long
    ): CacheValueLoader<T> {
        private var loaded = AtomicBoolean(false)
        override suspend fun load(value: T): LoadResult<T> {
            if (loaded.compareAndSet(false, true)) {
                return compareAndLoad(key, field, version, value)
            } else {
                throw AlreadyLoadedException()
            }
        }

        override suspend fun release() {}
    }

    suspend fun <T: Any> get(key: TKey, field: String): T? {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            return null
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                when (val cached = readFromCache<T>(key, field)) {
                    null -> {
                        metrics.incrementMissCount()
                        null
                    }
                    else -> {
                        metrics.incrementHitCount()
                        if (options.applyTtlIfHit && options.ttl.toMillis() > 0L) {
                            repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                        }
                        cached
                    }
                }
            }
        } catch (e: TimeoutCancellationException) {
            options.cacheFailurePolicy.handle("$typeName.get(): timeout occured on read from cache: cache=$name, key=$key, field=$field", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("$typeName.get(): exception occured on read from cache: cache=$name, key=$key, field=$field", e)
        }
        metrics.incrementMissCount()
        return null
    }

    suspend fun <T: Any> getOrLoad(key: TKey, field: String, fetch: (suspend () -> T)): T {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            metrics.incrementMissCount()
            return fetch()
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                runOrRetry {
                    when (val cached = readFromCache<T>(key, field)) {
                        null -> if (isColdTime(key)) {
                            metrics.incrementMissCount()
                            fetch()
                        } else loadToCache(key, field) {
                            metrics.incrementMissCount()
                            fetch()
                        }
                        else -> {
                            metrics.incrementHitCount()
                            if (options.applyTtlIfHit && options.ttl.toMillis() > 0L) {
                                repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                            }
                            cached
                        }
                    }
                }
            }
        } catch (e: TimeoutCancellationException) {
            options.cacheFailurePolicy.handle("$typeName.getOrLoad(): timeout occured on read from cache: cache=$name, key=$key, field=$field", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("$typeName.getOrLoad(): exception occured on read from cache: cache=$name, key=$key, field=$field", e)
        }
        metrics.incrementMissCount()
        return fetch()
    }

    suspend fun <T: Any> getOrLockForLoad(key: TKey, field: String): ResultGetOrLockForLoad<T> {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            metrics.incrementMissCount()
            return ResultGetOrLockForLoad()
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                runOrRetry {
                    when (val cached = readFromCache<T>(key, field)) {
                        null -> if (isColdTime(key)) {
                            metrics.incrementMissCount()
                            ResultGetOrLockForLoad()
                        } else {
                            metrics.incrementMissCount()
                            ResultGetOrLockForLoad(loader = lockForLoad(key, field))
                        }
                        else -> {
                            metrics.incrementHitCount()
                            if (options.applyTtlIfHit && options.ttl.toMillis() > 0L) {
                                repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                            }
                            ResultGetOrLockForLoad(cached)
                        }
                    }
                }
            }
        } catch (e: TimeoutCancellationException) {
            options.cacheFailurePolicy.handle("$typeName.getOrLockForLoad(): timeout occured on read from cache: cache=$name, key=$key, field=$field", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("$typeName.getOrLockForLoad(): exception occured on read from cache: cache=$name, key=$key, field=$field", e)
        }
        metrics.incrementMissCount()

        return ResultGetOrLockForLoad()
    }


    private suspend fun <T> readFromCache(key: TKey, field: String): T? {
        val cachedData = repository.get(keys.key(key), keys.field(field))
        return if (cachedData == null) {
            null
        } else {
            try {
                serializer.deserialize<T>(cachedData)
            } catch (e: Throwable) {
                null
            }
        }
    }

    private class Keys<K: Any>(
        private val name: String,
        private val keyFunction: Cache.KeyFunction,
        val options: CacheOptions
    ) {
        fun key(key: K): String = keyFunction.function(name, key)
        fun field(field: String): String = "$field|${options.version}"

        fun lock(key: K, postfix: String): String = "${key(key)}|$postfix"
        fun cold(key:K) = lock(key, "@COLD")
        fun fetchKey(key:K, field: String) = lock(key, "$field@FETCH")
        fun notEvicted(key: K) = lock(key, "@NOTEVICT")

    }

    private fun useColdTime(): Boolean {
        return options.coldTime.toMillis() > 0L
    }

    private suspend fun setColdTime(key: TKey) {
        if (useColdTime()) {
            lock.acquire(keys.cold(key), options.coldTime.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    private suspend fun isColdTime(key: TKey): Boolean {
        return if (useColdTime()) {
            return lock.isAcquired(keys.cold(key))
        } else false
    }

    private suspend fun setEvicted(key: TKey) = repository.delete(keys.notEvicted(key))
    private suspend fun setNotEvicted(key: TKey, field: String) = repository.incrBy(keys.notEvicted(key), field, 1, options.lockTimeout.toMillis(), TimeUnit.MILLISECONDS)
    private suspend fun isNotEvicted(key: TKey, field: String, version: Long) = version == repository.incrBy(keys.notEvicted(key), field, 1, options.lockTimeout.toMillis(), TimeUnit.MILLISECONDS) - 1
}
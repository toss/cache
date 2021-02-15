package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.runOrRetry
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.Serializer
import im.toss.util.reflection.TypeDigest
import im.toss.util.reflection.getType
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.TimeoutCancellationException
import mu.KotlinLogging
import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

abstract class MultiFieldCache<TKey: Any> {
    val blocking by lazy { BlockingMultiFieldCache(this) }
    abstract val options: CacheOptions

    abstract suspend fun evict(key: TKey)
    abstract suspend fun <T: Any> get(key: TKey, field: String, type: Type?): T?
    abstract suspend fun <T: Any> load(key: TKey, field: String, type: Type?, fetch: (suspend () -> T))
    abstract suspend fun <T: Any> getOrLoad(key: TKey, field: String, type: Type?, fetch: (suspend () -> T)): T
    @Throws(MutexLock.FailedAcquireException::class)
    abstract suspend fun <T: Any> lockForLoad(key: TKey, field: String, type: Type?, timeout: Long = -1): CacheValueLoader<T>
    abstract suspend fun <T: Any> getOrLockForLoad(key: TKey, field: String, type: Type?): ResultGetOrLockForLoad<T>
    abstract suspend fun <T: Any> optimisticLockForLoad(key: TKey, field: String, type: Type?): CacheValueLoader<T>

    suspend inline fun <reified T: Any> optimisticLockForLoad(key: TKey, field: String): CacheValueLoader<T> = optimisticLockForLoad(key, field, getType<T>())
    @Throws(MutexLock.FailedAcquireException::class)
    suspend inline fun <reified T: Any> lockForLoad(key: TKey, field: String, timeout: Long = -1): CacheValueLoader<T> = lockForLoad(key, field, getType<T>(), timeout)
    suspend inline fun <reified T: Any> getOrLockForLoad(key: TKey, field: String): ResultGetOrLockForLoad<T> = getOrLockForLoad(key, field, getType<T>())
    suspend inline fun <reified T: Any> getOrLoad(key: TKey, field: String, noinline fetch: (suspend () -> T)): T = getOrLoad(key, field, getType<T>(), fetch)
    suspend inline fun <reified T: Any> get(key: TKey, field: String): T? = get(key, field, getType<T>())
    suspend inline fun <reified T: Any> load(key: TKey, field: String, noinline fetch: (suspend () -> T)) = load(key, field, getType<T>(), fetch)
}

data class MultiFieldCacheImpl<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    override val options: CacheOptions,
    private val metrics: CacheMetrics = CacheMetrics(name),
    private val typeName: String = "MultiFieldCache"
) : Cache, CacheMeter by metrics, MultiFieldCache<TKey>() {
    private val typeDigest: TypeDigest = TypeDigest(
        environments = mapOf(
            "serializer.name" to serializer.name
        )
    )

    private val keys = Keys<TKey>(name, keyFunction, options, typeDigest)

    override suspend fun evict(key: TKey) {
        metrics.incrementEvictCount()
        setColdTime(key)
        setModified(key)
        repository.delete(keys.key(key))
    }

    override suspend fun <T: Any> load(key: TKey, field: String, type: Type?, fetch: (suspend () -> T)) {
        runWithTimeout(options.operationTimeout.toMillis()) {
            if (options.cacheMode == CacheMode.EVICTION_ONLY) {
                evict(key)
            } else {
                try {
                    runOrRetry {
                        loadToCache(key, field, type, fetch)
                    }
                } catch (e: Throwable) {
                    options.cacheFailurePolicy.handle(
                        "$typeName.load(): exception occured on load: cache=$name, key=$key, field=$field", e
                    )
                }
            }
        }
    }

    private suspend fun <T: Any> loadToCache(key: TKey, field: String, type: Type?, fetch: (suspend () -> T)): T {
        val lock = lockForLoad<T>(key, field, type)
        try {
            val value = fetch()
            lock.load(value)
            return value
        } finally {
            lock.release()
        }
    }


    @Throws(MutexLock.FailedAcquireException::class)
    override suspend fun <T:Any> lockForLoad(key: TKey, field: String, type: Type?, timeout: Long): CacheValueLoader<T> {
        val lockKey = keys.fetchKey(key, field)
        if (!lock.acquire(lockKey, timeout)) {
            throw MutexLock.FailedAcquireException
        }
        return PessimisticKeyFieldCacheValueLoader(key, field, type, acquireNewVersion(key, field), lockKey)
    }

    override suspend fun <T:Any> optimisticLockForLoad(key: TKey, field: String, type: Type?): CacheValueLoader<T> {
        return OptimisticKeyFieldCacheValueLoader(key, field, type, acquireNewVersion(key, field))
    }

    private suspend fun <T:Any> compareAndLoad(key: TKey, field: String, version: Long, type: Type?, value: T): LoadResult<T> {
        return if (compareAndAcquire(key, field, version)) {
            val dataBytes = serializer.serialize(value)
            repository.set(keys.key(key), keys.field(field, type), dataBytes, options.ttl.toMillis(), TimeUnit.MILLISECONDS)
            metrics.incrementPutCount()
            LoadResult(value)
        } else {
            repository.delete(keys.key(key), keys.field(field, type))
            logger.info("optimistic lock failure occured on load, purged cached data. cache=$name, key=$key, field=$field")
            LoadResult(value, success = false, isOptimisticLockFailure = true)
        }
    }

    inner class PessimisticKeyFieldCacheValueLoader<T: Any>(
        val key: TKey,
        val field: String,
        val type: Type?,
        override val version: Long,
        private val lockKey: String
    ): CacheValueLoader<T> {
        private var loaded = AtomicBoolean(false)
        override suspend fun load(value: T): LoadResult<T> {
            if (loaded.compareAndSet(false, true)) {
                return try {
                    compareAndLoad(key, field, version, type, value)
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
        val type: Type?,
        override val version: Long
    ): CacheValueLoader<T> {
        private var loaded = AtomicBoolean(false)
        override suspend fun load(value: T): LoadResult<T> {
            if (loaded.compareAndSet(false, true)) {
                return compareAndLoad(key, field, version, type, value)
            } else {
                throw AlreadyLoadedException()
            }
        }

        override suspend fun release() {}
    }

    override suspend fun <T: Any> get(key: TKey, field: String, type: Type?): T? {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            return null
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                when (val cached = readFromCache<T>(key, field, type)) {
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

    override suspend fun <T: Any> getOrLoad(key: TKey, field: String, type: Type?, fetch: (suspend () -> T)): T {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            metrics.incrementMissCount()
            return fetch()
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                runOrRetry {
                    when (val cached = readFromCache<T>(key, field, type)) {
                        null -> if (isColdTime(key)) {
                            metrics.incrementMissCount()
                            fetch()
                        } else loadToCache(key, field, type) {
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

    override suspend fun <T: Any> getOrLockForLoad(key: TKey, field: String, type: Type?): ResultGetOrLockForLoad<T> {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            metrics.incrementMissCount()
            return ResultGetOrLockForLoad()
        }

        try {
            return runWithTimeout(options.operationTimeout.toMillis()) {
                runOrRetry {
                    when (val cached = readFromCache<T>(key, field, type)) {
                        null -> if (isColdTime(key)) {
                            metrics.incrementMissCount()
                            ResultGetOrLockForLoad()
                        } else {
                            metrics.incrementMissCount()
                            ResultGetOrLockForLoad(loader = lockForLoad(key, field, type))
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


    private suspend fun <T> readFromCache(key: TKey, field: String, type: Type?): T? {
        val cachedData = repository.get(keys.key(key), keys.field(field, type))
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
        val options: CacheOptions,
        val digest: TypeDigest
    ) {
        fun key(key: K): String = keyFunction.function(name, key)
        fun field(field: String, type: Type?): String = if (options.isolationByType) {
            "$field|${options.version}|${digest.digest(type)}"
        } else {
            "$field|${options.version}"
        }

        fun lock(key: K, postfix: String): String = "${key(key)}|$postfix"
        fun cold(key:K) = lock(key, "@COLD")
        fun fetchKey(key:K, field: String) = lock(key, "$field@FETCH")
        fun optimisticLock(key: K) = lock(key, "@NOTEVICT")

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

    private suspend fun setModified(key: TKey) = repository.delete(keys.optimisticLock(key))
    private suspend fun acquireNewVersion(key: TKey, field: String) = repository.incrBy(keys.optimisticLock(key), field, 1, options.lockTimeout.toMillis(), TimeUnit.MILLISECONDS)
    private suspend fun compareAndAcquire(key: TKey, field: String, version: Long) = version == acquireNewVersion(key, field) - 1
}
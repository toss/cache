package im.toss.util.cache.impl

import im.toss.util.cache.*
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.runOrRetry
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.Serializer
import im.toss.util.reflection.TypeDigest
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

data class MultiFieldCacheImpl<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    val context: CoroutineContext?,
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

    private suspend fun <T> runWithContext(context: CoroutineContext?, block: suspend () -> T): T {
        return if (context == null) {
            block()
        } else {
            withContext(context) {
                block()
            }
        }
    }

    override suspend fun evict(key: TKey) {
        runWithContext(context) {
            metrics.incrementEvictCount()
            setColdTime(key)
            setModified(key)
            repository.delete(keys.key(key))
        }
    }

    override suspend fun <T: Any> load(key: TKey, field: String, forceLoad: Boolean, type: Type?, fetch: (suspend () -> T)) {
        runWithContext(context) {
            runWithTimeout(options.operationTimeout.toMillis()) {
                if (options.cacheMode == CacheMode.EVICTION_ONLY) {
                    evict(key)
                } else {
                    try {
                        runOrRetry {
                            if (forceLoad || !isColdTime(key)) {
                                loadToCache(key, field, type, fetch)
                            }
                        }
                    } catch (e: Throwable) {
                        options.cacheFailurePolicy.handle(
                            "$typeName.load(): exception occured on load: cache=$name, key=$key, field=$field", e
                        )
                    }
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
    override suspend fun <T: Any> lockForLoad(
        key: TKey,
        field: String,
        type: Type?,
        timeout: Long
    ): CacheValueLoader<T> {
        return if (options.enablePessimisticLock) {
            pessimisticLockForLoad(key, field, type, timeout)
        } else {
            basicLockForLoad(key, field, type)
        }
    }

    override suspend fun <T: Any> pessimisticLockForLoad(key: TKey, field: String, type: Type?, timeout: Long): CacheValueLoader<T> {
        if (!options.enablePessimisticLock)
            throw NotSupportPessimisticLockException()

        val lockKey = keys.fetchKey(key, field)
        if (!lock.acquire(lockKey, timeout)) {
            throw MutexLock.FailedAcquireException
        }
        return PessimisticKeyFieldCacheValueLoader(key, field, type, acquireNewVersion(key, field), lockKey)
    }


    override suspend fun <T: Any> optimisticLockForLoad(key: TKey, field: String, type: Type?): CacheValueLoader<T> {
        if (!options.enableOptimisticLock)
            throw NotSupportOptimisticLockException()

        return basicLockForLoad(key, field, type)
    }

    private suspend fun <T: Any> basicLockForLoad(key: TKey, field: String, type: Type?): CacheValueLoader<T> {
        return BasicKeyFieldCacheValueLoader(key, field, type, acquireNewVersion(key, field))
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

    inner class BasicKeyFieldCacheValueLoader<T: Any>(
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

        return runWithContext(context) {
            try {
                return@runWithContext runWithTimeout(options.operationTimeout.toMillis()) {
                    when (val cached = readFromCache<T>(key, field, type)) {
                        null -> {
                            metrics.incrementMissCount()
                            null
                        }
                        else -> {
                            metrics.incrementHitCount()
                            if (options.isApplyTtlIfHit()) {
                                repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                            }
                            cached
                        }
                    }
                }
            } catch (e: TimeoutCancellationException) {
                options.cacheFailurePolicy.handle(
                    "$typeName.get(): timeout occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            } catch (e: Throwable) {
                options.cacheFailurePolicy.handle(
                    "$typeName.get(): exception occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            }
            metrics.incrementMissCount()
            null
        }
    }

    override suspend fun <T: Any> getOrLoad(key: TKey, field: String, type: Type?, fetch: (suspend () -> T)): T {
        return runWithContext(context) {
            if (options.cacheMode == CacheMode.EVICTION_ONLY) {
                metrics.incrementMissCount()
                return@runWithContext fetch()
            }

            try {
                return@runWithContext runWithTimeout(options.operationTimeout.toMillis()) {
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
                                if (options.isApplyTtlIfHit()) {
                                    repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                                }
                                cached
                            }
                        }
                    }
                }
            } catch (e: TimeoutCancellationException) {
                options.cacheFailurePolicy.handle(
                    "$typeName.getOrLoad(): timeout occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            } catch (e: Throwable) {
                options.cacheFailurePolicy.handle(
                    "$typeName.getOrLoad(): exception occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            }
            metrics.incrementMissCount()
            fetch()
        }
    }

    override suspend fun <T: Any> getOrLockForLoad(key: TKey, field: String, type: Type?): ResultGetOrLockForLoad<T> {
        return runWithContext(context) {
            if (options.cacheMode == CacheMode.EVICTION_ONLY) {
                metrics.incrementMissCount()
                return@runWithContext ResultGetOrLockForLoad()
            }

            try {
                return@runWithContext runWithTimeout(options.operationTimeout.toMillis()) {
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
                                if (options.isApplyTtlIfHit()) {
                                    repository.expire(keys.key(key), options.ttl.toMillis(), TimeUnit.MILLISECONDS)
                                }
                                ResultGetOrLockForLoad(cached)
                            }
                        }
                    }
                }
            } catch (e: TimeoutCancellationException) {
                options.cacheFailurePolicy.handle(
                    "$typeName.getOrLockForLoad(): timeout occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            } catch (e: Throwable) {
                options.cacheFailurePolicy.handle(
                    "$typeName.getOrLockForLoad(): exception occured on read from cache: cache=$name, key=$key, field=$field",
                    e
                )
            }
            metrics.incrementMissCount()

            ResultGetOrLockForLoad()
        }
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

    private class Keys<K : Any>(
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
        fun cold(key: K) = lock(key, "@COLD")
        fun fetchKey(key: K, field: String) = lock(key, "$field@FETCH")
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

    private suspend fun acquireNewVersion(key: TKey, field: String) =
        if (options.enableOptimisticLock) {
            repository.incrBy(keys.optimisticLock(key), field, 1, options.lockTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } else {
            -1L
        }

    private suspend fun compareAndAcquire(key: TKey, field: String, version: Long) =
        if (options.enableOptimisticLock) {
            version == acquireNewVersion(key, field) - 1
        } else {
            true
        }
}

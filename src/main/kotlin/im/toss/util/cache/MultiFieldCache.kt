package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.run
import im.toss.util.concurrent.lock.runOrRetry
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.Serializer
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.TimeoutCancellationException

class MultiFieldCache<TKey: Any>(
    override val name: String,
    keyFunction: KeyFunction,
    private val lock: MutexLock,
    private val repository: KeyFieldValueRepository,
    private val serializer: Serializer,
    val options: CacheOptions
) : Cache(name) {
    fun blocking() = BlockingMultiFieldCache(this)

    private val keys = Keys<TKey>(name, keyFunction, options)

    suspend fun evict(key: TKey) {
        incrementEvictCount()
        setColdTime(key)
        setEvicted(key)
        repository.delete(keys.key(key))
    }

    suspend fun <T: Any> load(key: TKey, field: String, fetch: (suspend () -> T)) {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            evict(key)
        } else {
            try {
                runOrRetry {
                    loadToCache(key, field, fetch)
                }
            } catch (e: Throwable) {
                options.cacheFailurePolicy.handle(
                    "MultiFieldCache.load(): exception occured on load: cache=$name, key=$key, field=$field", e
                )
            }
        }
    }

    private suspend fun <T: Any> loadToCache(key: TKey, field: String, fetch: (suspend () -> T)): T {
        return lock.run(keys.fetchKey(key, field)) {
            setNotEvicted(key, field)
            val fetched = fetch()
            if (isNotEvicted(key, field)) {
                val dataBytes = serializer.serialize(fetched)
                repository.set(keys.key(key), field, dataBytes, options.ttl, options.ttlTimeUnit)
                incrementPutCount()
            }
            fetched
        }
    }

    suspend fun <T: Any> get(key: TKey, field: String): T? {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            return null
        }

        try {
            return when (val cached = readFromCache<T>(key, field)) {
                null -> {
                    incrementMissCount()
                    null
                }
                else -> {
                    incrementHitCount()
                    if (options.applyTtlIfHit && options.ttl > 0L) {
                        repository.expire(keys.key(key), options.ttl, options.ttlTimeUnit)
                    }
                    cached
                }
            }
        } catch (e: TimeoutCancellationException) {
            options.cacheFailurePolicy.handle("MultiFieldCache.get(): timeout occured on read from cache: cache=$name, key=$key, field=$field", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("MultiFieldCache.get(): exception occured on read from cache: cache=$name, key=$key, field=$field", e)
        }
        incrementMissCount()
        return null
    }

    suspend fun <T: Any> getOrLoad(key: TKey, field: String, fetch: (suspend () -> T)): T {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            incrementMissCount()
            return fetch()
        }

        try {
            return runOrRetry {
                when (val cached = readFromCache<T>(key, field)) {
                    null -> if (isColdTime(key)) {
                        incrementMissCount()
                        fetch()
                    } else loadToCache(key, field) {
                        incrementMissCount()
                        fetch()
                    }
                    else -> {
                        incrementHitCount()
                        if (options.applyTtlIfHit && options.ttl > 0L) {
                            repository.expire(keys.key(key), options.ttl, options.ttlTimeUnit)
                        }
                        cached
                    }
                }
            }
        } catch (e: TimeoutCancellationException) {
            options.cacheFailurePolicy.handle("MultiFieldCache.getOrLoad(): timeout occured on read from cache: cache=$name, key=$key, field=$field", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("MultiFieldCache.getOrLoad(): exception occured on read from cache: cache=$name, key=$key, field=$field", e)
        }
        incrementMissCount()
        return fetch()
    }

    private suspend fun <T> readFromCache(key: TKey, field: String): T? {
        val cachedData = repository.get(keys.key(key), field)
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
        private val keyFunction: KeyFunction,
        val options: CacheOptions
    ) {
        fun key(key: K): String = keyFunction.function(name, options.version, key)

        fun lock(key: K, postfix: String): String = "${key(key)}|$postfix"
        fun cold(key:K) = lock(key, "@COLD")
        fun fetchKey(key:K, field: String) = lock(key, "$field@FETCH")
        fun notEvicted(key: K) = lock(key, "@NOTEVICT")

    }

    private fun useColdTime(): Boolean {
        return options.coldTime > 0L
    }

    private suspend fun setColdTime(key: TKey) {
        if (useColdTime()) {
            lock.acquire(keys.cold(key), options.coldTime, options.coldTimeUnit)
        }
    }

    private suspend fun isColdTime(key: TKey): Boolean {
        return if (useColdTime()) {
            return lock.isAcquired(keys.cold(key))
        } else false
    }

    private suspend fun setEvicted(key: TKey) = repository.delete(keys.notEvicted(key))
    private suspend fun setNotEvicted(key: TKey, field: String) = repository.set(keys.notEvicted(key), field, "1".toByteArray(), options.evictCheckTTL, options.evictCheckTimeUnit)
    private suspend fun isNotEvicted(key: TKey, field: String) = repository.delete(keys.notEvicted(key), field)
}
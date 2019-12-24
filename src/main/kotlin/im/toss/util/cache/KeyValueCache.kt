package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingKeyValueCache
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.run
import im.toss.util.concurrent.lock.runOrRetry
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.Serializer
import im.toss.util.repository.KeyValueRepository
import kotlinx.coroutines.TimeoutCancellationException

class KeyValueCache<TKey: Any>(
    override val name: String,
    keyFunction: KeyFunction,
    private val lock: MutexLock,
    private val repository: KeyValueRepository,
    private val serializer: Serializer,
    val options: CacheOptions
) : Cache(name) {
    fun blocking() = BlockingKeyValueCache(this)

    private val keys = Keys<TKey>(name, keyFunction, options)

    suspend fun evict(key: TKey) {
        incrementEvictCount()
        setColdTime(key)
        setEvicted(key)
        repository.delete(keys.key(key))
    }

    suspend fun <T: Any> load(key: TKey, fetch: (suspend () -> T)) {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            evict(key)
        } else {
            try {
                runOrRetry {
                    loadToCache(key, fetch)
                }
            } catch (e: Throwable) {
                options.cacheFailurePolicy.handle("KeyValueCache.load(): exception occured on load: cache=$name, key=$key", e)
            }
        }
    }

    private suspend fun <T: Any> loadToCache(key: TKey, fetch: (suspend () -> T)): T {
        return lock.run(keys.fetchKey(key), options.readTimeout) {
            setNotEvicted(key)
            val fetched = fetch()
            if (isNotEvicted(key)) {
                val dataBytes = serializer.serialize(fetched)
                repository.set(keys.key(key), dataBytes, options.ttl, options.ttlTimeUnit)
                incrementPutCount()
            }
            fetched
        }
    }

    suspend fun <T: Any> get(key: TKey): T? {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            incrementMissCount()
            return null
        }

        try {
            return when (val cached = readFromCache<T>(key)) {
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
            options.cacheFailurePolicy.handle("KeyValueCache.get(): timeout occured on read from cache: cache=$name, key=$key", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("KeyValueCache.get(): exception occured on read from cache: cache=$name, key=$key", e)
        }
        incrementMissCount()
        return null
    }

    suspend fun <T: Any> getOrLoad(key: TKey, fetch: (suspend () -> T)): T {
        if (options.cacheMode == CacheMode.EVICTION_ONLY) {
            incrementMissCount()
            return fetch()
        }

        try {
            return runOrRetry {
                when (val cached = readFromCache<T>(key)) {
                    null -> if (isColdTime(key)) {
                        incrementMissCount()
                        fetch()
                    } else loadToCache(key) {
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
            options.cacheFailurePolicy.handle("KeyValueCache.getOrLoad(): timeout occured on read from cache: cache=$name, key=$key", e)
        } catch (e: Throwable) {
            options.cacheFailurePolicy.handle("KeyValueCache.getOrLoad(): exception occured on read from cache: cache=$name, key=$key", e)
        }
        incrementMissCount()
        return fetch()
    }

    private suspend fun <T> readFromCache(key: TKey): T? {
        val cachedData = runWithTimeout(options.readTimeout) { repository.get(keys.key(key)) }
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
        fun fetchKey(key:K) = lock(key, "@FETCH")
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
    private suspend fun setNotEvicted(key: TKey) = repository.set(keys.notEvicted(key), "1".toByteArray(), options.evictCheckTTL, options.evictCheckTimeUnit)
    private suspend fun isNotEvicted(key: TKey) = repository.delete(keys.notEvicted(key))
}
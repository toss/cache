package im.toss.util.cache.impl

import im.toss.util.cache.*
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.data.serializer.Serializer
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.lang.reflect.Type
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.CoroutineContext
import kotlin.math.round

class NotSupportLockException: Exception("check options: enable-optimistic-lock, enable-pessimistic-lock")

data class KeyValueCacheImpl<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    val context: CoroutineContext?,
    override val options: CacheOptions,
    private val metrics: CacheMetrics = CacheMetrics(name)
) : Cache, CacheMeter by metrics, KeyValueCache<TKey>() {
    val cache by lazy { MultiFieldCacheImpl<TKey>(name, keyFunction, lock, repository, serializer, context, options, metrics, "KeyValueCache") }
    val field = ".value"

    override suspend fun evict(key: TKey) = cache.evict(key)

    override suspend fun <T : Any> get(key: TKey, type: Type?): T? = cache.get(key, field, type)
    override suspend fun <T : Any> load(key: TKey, type: Type?, fetch: suspend () -> T) = cache.load(key, field, type, fetch)
    override suspend fun <T : Any> getOrLoad(key: TKey, type: Type?, fetch: suspend () -> T): T = cache.getOrLoad(key, field, type, fetch)
    @Throws(MutexLock.FailedAcquireException::class)
    override suspend fun <T : Any> lockForLoad(key: TKey, type: Type?, timeout: Long): CacheValueLoader<T> = cache.lockForLoad(key, field, type, timeout)
    override suspend fun <T : Any> getOrLockForLoad(key: TKey, type: Type?): ResultGetOrLockForLoad<T> = cache.getOrLockForLoad(key, field, type)
    override suspend fun <T : Any> pessimisticLockForLoad(key: TKey, type: Type?, timeout: Long): CacheValueLoader<T> = cache.pessimisticLockForLoad(key, field, type, timeout)
    override suspend fun <T : Any> optimisticLockForLoad(key: TKey, type: Type?): CacheValueLoader<T> = cache.optimisticLockForLoad(key, field, type)

    override suspend fun <T : Any> multiGetTo(keys: Set<TKey>, missedKeys: MutableSet<TKey>, result: MutableMap<TKey, T>, type: Type?): Map<TKey, T> {
        return result.apply {
            val missed = ConcurrentLinkedQueue<TKey>()
            val cached = keys.mapNotNull(options.multiParallelism) {
                val cached = get<T>(it, type)
                if (cached == null) {
                    missed.add(it)
                    null
                } else {
                    it to cached
                }
            }
            cached.toMap(result)
            missedKeys += missed
        }
    }

    override suspend fun <T : Any> multiGet(keys: Set<TKey>, type: Type?): Map<TKey, T?> {
        return mutableMapOf<TKey, T?>().apply {
            val missedKeys = mutableSetOf<TKey>()
            multiGetTo<T>(keys, missedKeys, mutableMapOf(), type).forEach { (k, v) -> put(k, v) }
            missedKeys.forEach { put(it, null) }
        }
    }

    override suspend fun <T : Any> multiLoad(keyValues: Map<TKey, T>, type: Type?) {
        keyValues.entries.forEach(options.multiParallelism) {
            load(it.key, type) { it.value }
        }
    }

    /**
     * multiGetOrLoad
     * lock을 모두 사용하지 않는 경우에 bulk load를 지원
     */
    override suspend fun <T : Any> multiGetOrLoad(keys: Set<TKey>, type: Type?, fetch: suspend (Set<TKey>) -> Map<TKey, T>): Map<TKey, T> =
        if (options.enableOptimisticLock || options.enablePessimisticLock) {
            multiGetOrLoadWithLock(keys, type, fetch)
        } else {
            multiGetOrLoadNoLock(keys, type, fetch)
        }

    private suspend fun <T : Any> multiGetOrLoadWithLock(keys: Set<TKey>, type: Type?, fetch: suspend (Set<TKey>) -> Map<TKey, T>): Map<TKey, T> {
        return keys
            .map(options.multiParallelism) { key ->
                // simple parallel getOrLoad
                key to getOrLoad(key, type) {
                    fetch(setOf(key)).getValue(key)
                }
            }
            .toMap()
    }

    private suspend fun <T : Any> multiGetOrLoadNoLock(keys: Set<TKey>, type: Type?, fetch: suspend (Set<TKey>) -> Map<TKey, T>): Map<TKey, T>  {
        if (options.enableOptimisticLock || options.enablePessimisticLock) {
            throw NotSupportLockException()
        }

        return mutableMapOf<TKey, T>().also { result ->
            val missedKeys = mutableSetOf<TKey>()
            multiGetTo(keys, missedKeys, result, type)
            if (missedKeys.isNotEmpty()) {
                val fetched = fetch(missedKeys).apply { toMap(result) }
                multiLoad(fetched, type)
            }
        }
    }
}


private fun <T> Collection<T>.divide(size: Int): List<List<T>> {
    if (size == 0) return chunked(this.size)
    val chunkSize = round((this.size / size.toDouble())).toInt().coerceAtLeast(1)
    return chunked(chunkSize)
}

private suspend fun <T> Collection<T>.forEach(parallel: Int, block: suspend (T) -> Unit) = coroutineScope {
    divide(parallel)
        .map {
            async {
                it.forEach {
                    block(it)
                }
            }
        }
        .awaitAll()
}

private suspend fun <T, R> Collection<T>.map(parallel: Int, block: suspend (T) -> R): List<R> = coroutineScope {
    divide(parallel)
        .map {
            async {
                it.map {
                    block(it)
                }
            }
        }
        .awaitAll()
        .flatten()
}

private suspend fun <T, R> Collection<T>.mapNotNull(parallel: Int, block: suspend (T) -> R?): List<R> = coroutineScope {
    divide(parallel)
        .map {
            async {
                it.mapNotNull {
                    block(it)
                }
            }
        }
        .awaitAll()
        .flatten()
}

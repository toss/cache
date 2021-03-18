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

data class KeyValueCacheImpl<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    override val options: CacheOptions,
    private val metrics: CacheMetrics = CacheMetrics(name)
) : Cache, CacheMeter by metrics, KeyValueCache<TKey>() {
    val cache by lazy { MultiFieldCacheImpl<TKey>(name, keyFunction, lock, repository, serializer, options, metrics, "KeyValueCache") }
    val field = ".value"

    override suspend fun evict(key: TKey) = cache.evict(key)

    override suspend fun <T : Any> get(key: TKey, type: Type?): T? = cache.get(key, field, type)
    override suspend fun <T : Any> load(key: TKey, type: Type?, fetch: suspend () -> T) = cache.load(key, field, type, fetch)
    override suspend fun <T : Any> getOrLoad(key: TKey, type: Type?, fetch: suspend () -> T): T = cache.getOrLoad(key, field, type, fetch)
    @Throws(MutexLock.FailedAcquireException::class)
    override suspend fun <T : Any> lockForLoad(key: TKey, type: Type?, timeout: Long): CacheValueLoader<T> = cache.lockForLoad(key, field, type, timeout)
    override suspend fun <T : Any> getOrLockForLoad(key: TKey, type: Type?): ResultGetOrLockForLoad<T> = cache.getOrLockForLoad(key, field, type)
    override suspend fun <T : Any> optimisticLockForLoad(key: TKey, type: Type?): CacheValueLoader<T> = cache.optimisticLockForLoad(key, field, type)

    override suspend fun <T : Any> multiGet(keys: Set<TKey>, type: Type?): Map<TKey, T?> = coroutineScope {
        // parallel get
        keys
            .chunked(100) // TODO configurable
            .flatMap {
                it
                    .map { key -> async { key to get<T>(key, type) } }
                    .awaitAll()
            }
            .toMap()
    }

    /**
     * multiGetOrLoad
     * 이 구현체는 bulk load를 지원하지 않는다
     */
    override suspend fun <T : Any> multiGetOrLoad(keys: Set<TKey>, type: Type?, fetch: suspend (Set<TKey>) -> Map<TKey, T>): Map<TKey, T?> = coroutineScope {
        // parallel getOrLoad
        keys
            .chunked(100) // TODO configurable
            .flatMap {
                it
                    .map { key ->
                        async {
                            key to getOrLoad(key, type) {
                                fetch(setOf(key)).getValue(key)
                            }
                        }
                    }
                    .awaitAll()
            }
            .toMap()
    }
}
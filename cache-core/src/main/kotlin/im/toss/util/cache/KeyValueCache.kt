package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingKeyValueCache
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.data.serializer.Serializer
import im.toss.util.repository.KeyFieldValueRepository

data class KeyValueCache<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    val options: CacheOptions,
    private val metrics: CacheMetrics = CacheMetrics(name)
) : Cache, CacheMeter by metrics {
    val blocking by lazy { BlockingKeyValueCache(this) }

    private val cache by lazy { MultiFieldCache<TKey>(name, keyFunction, lock, repository, serializer, options, metrics, "KeyValueCache") }
    private val field = ".value"

    suspend fun evict(key: TKey) = cache.evict(key)
    suspend fun <T: Any> load(key: TKey, fetch: (suspend () -> T)) = cache.load(key, field, fetch)
    suspend fun <T: Any> get(key: TKey): T? = cache.get(key, field)
    suspend fun <T: Any> getOrLoad(key: TKey, fetch: (suspend () -> T)): T = cache.getOrLoad(key, field, fetch)
    suspend fun <T: Any> getOrLockForLoad(key: TKey): ResultGetOrLockForLoad<T> = cache.getOrLockForLoad(key, field)
    @Throws(MutexLock.FailedAcquireException::class)
    suspend fun <T:Any> lockForLoad(key: TKey, timeout: Long = -1): CacheValueLoader<T> = cache.lockForLoad(key, field, timeout)
    suspend fun <T:Any> optimisticLockForLoad(key: TKey): CacheValueLoader<T> = cache.optimisticLockForLoad(key, field)
}
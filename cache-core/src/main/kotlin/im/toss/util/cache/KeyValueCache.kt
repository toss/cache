package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingKeyValueCache
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.data.serializer.Serializer
import im.toss.util.reflection.TypeDigest
import im.toss.util.repository.KeyFieldValueRepository

data class KeyValueCache<TKey: Any>(
    override val name: String,
    val keyFunction: Cache.KeyFunction,
    val lock: MutexLock,
    val repository: KeyFieldValueRepository,
    val serializer: Serializer,
    val options: CacheOptions,
    private val typeDigest: TypeDigest = TypeDigest(),
    private val metrics: CacheMetrics = CacheMetrics(name)
) : Cache, CacheMeter by metrics {
    val blocking by lazy { BlockingKeyValueCache(this) }

    val cache by lazy { MultiFieldCache<TKey>(name, keyFunction, lock, repository, serializer, options, typeDigest, metrics, "KeyValueCache") }
    val field = ".value"

    suspend fun evict(key: TKey) = cache.evict(key)
    suspend inline fun <reified T: Any> load(key: TKey, noinline fetch: (suspend () -> T)) = cache.load(key, field, fetch)
    suspend inline fun <reified T: Any> get(key: TKey): T? = cache.get(key, field)
    suspend inline fun <reified T: Any> getOrLoad(key: TKey, noinline fetch: (suspend () -> T)): T = cache.getOrLoad(key, field, fetch)
    suspend inline fun <reified T: Any> getOrLockForLoad(key: TKey): ResultGetOrLockForLoad<T> = cache.getOrLockForLoad(key, field)
    @Throws(MutexLock.FailedAcquireException::class)
    suspend inline fun <reified T:Any> lockForLoad(key: TKey, timeout: Long = -1): CacheValueLoader<T> = cache.lockForLoad(key, field, timeout)
    suspend inline fun <reified T:Any> optimisticLockForLoad(key: TKey): CacheValueLoader<T> = cache.optimisticLockForLoad(key, field)
}
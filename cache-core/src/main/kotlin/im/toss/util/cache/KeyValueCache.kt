package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingKeyValueCache
import im.toss.util.cache.metrics.CacheMeter
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.data.serializer.Serializer
import im.toss.util.reflection.getType
import im.toss.util.repository.KeyFieldValueRepository
import java.lang.reflect.Type

abstract class KeyValueCache<TKey: Any> {
    val blocking by lazy { BlockingKeyValueCache(this) }
    abstract val options: CacheOptions

    abstract suspend fun evict(key: TKey)
    abstract suspend fun <T: Any> get(key: TKey, type: Type?): T?
    abstract suspend fun <T: Any> load(key: TKey, type: Type?, fetch: (suspend () -> T))
    abstract suspend fun <T: Any> getOrLoad(key: TKey, type: Type?, fetch: (suspend () -> T)): T
    @Throws(MutexLock.FailedAcquireException::class)
    abstract suspend fun <T: Any> lockForLoad(key: TKey, type: Type?, timeout: Long = -1): CacheValueLoader<T>
    abstract suspend fun <T: Any> getOrLockForLoad(key: TKey, type: Type?): ResultGetOrLockForLoad<T>
    abstract suspend fun <T: Any> optimisticLockForLoad(key: TKey, type: Type?): CacheValueLoader<T>

    suspend inline fun <reified T: Any> optimisticLockForLoad(key: TKey): CacheValueLoader<T> = optimisticLockForLoad(key, getType<T>())
    @Throws(MutexLock.FailedAcquireException::class)
    suspend inline fun <reified T: Any> lockForLoad(key: TKey, timeout: Long = -1): CacheValueLoader<T> = lockForLoad(key, getType<T>(), timeout)
    suspend inline fun <reified T: Any> getOrLockForLoad(key: TKey): ResultGetOrLockForLoad<T> = getOrLockForLoad(key, getType<T>())
    suspend inline fun <reified T: Any> getOrLoad(key: TKey, noinline fetch: (suspend () -> T)): T = getOrLoad(key, getType<T>(), fetch)
    suspend inline fun <reified T: Any> get(key: TKey): T? = get(key, getType<T>())
    suspend inline fun <reified T: Any> load(key: TKey, noinline fetch: (suspend () -> T)) = load(key, getType<T>(), fetch)
}


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
}
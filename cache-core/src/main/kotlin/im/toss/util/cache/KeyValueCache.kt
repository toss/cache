package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingKeyValueCache
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.reflection.getType
import java.lang.reflect.Type

abstract class KeyValueCache<TKey: Any> {
    val blocking by lazy { BlockingKeyValueCache(this) }
    abstract val options: CacheOptions

    abstract suspend fun evict(key: TKey)
    abstract suspend fun <T: Any> get(key: TKey, type: Type?): T?
    abstract suspend fun <T: Any> load(key: TKey, forceLoad: Boolean = false, type: Type?, fetch: (suspend () -> T))
    abstract suspend fun <T: Any> getOrLoad(key: TKey, type: Type?, fetch: (suspend () -> T)): T
    @Throws(MutexLock.FailedAcquireException::class)
    abstract suspend fun <T: Any> lockForLoad(key: TKey, type: Type?, timeout: Long = -1): CacheValueLoader<T>
    abstract suspend fun <T: Any> getOrLockForLoad(key: TKey, type: Type?): ResultGetOrLockForLoad<T>
    @Throws(NotSupportPessimisticLockException::class)
    abstract suspend fun <T : Any> pessimisticLockForLoad(key: TKey, type: Type?, timeout: Long = -1): CacheValueLoader<T>
    @Throws(NotSupportOptimisticLockException::class)
    abstract suspend fun <T: Any> optimisticLockForLoad(key: TKey, type: Type?): CacheValueLoader<T>

    abstract suspend fun <T: Any> multiGet(keys: Set<TKey>, type: Type?): Map<TKey, T?>
    abstract suspend fun <T: Any> multiGetTo(keys: Set<TKey>, missedKeys: MutableSet<TKey>, result: MutableMap<TKey, T>, type: Type?): Map<TKey, T>
    abstract suspend fun <T: Any> multiLoad(keyValues: Map<TKey, T>, forceLoad: Boolean = false, type: Type?)
    abstract suspend fun <T: Any> multiGetOrLoad(keys: Set<TKey>, type: Type?, fetch: (suspend (Set<TKey>) -> Map<TKey, T>)): Map<TKey, T>

    @Throws(NotSupportPessimisticLockException::class)
    suspend inline fun <reified T: Any> pessimisticLockForLoad(key: TKey, timeout: Long = -1): CacheValueLoader<T> = pessimisticLockForLoad(key, getType<T>(), timeout)
    @Throws(NotSupportOptimisticLockException::class)
    suspend inline fun <reified T: Any> optimisticLockForLoad(key: TKey): CacheValueLoader<T> = optimisticLockForLoad(key, getType<T>())
    @Throws(MutexLock.FailedAcquireException::class)
    suspend inline fun <reified T: Any> lockForLoad(key: TKey, timeout: Long = -1): CacheValueLoader<T> = lockForLoad(key, getType<T>(), timeout)
    suspend inline fun <reified T: Any> getOrLockForLoad(key: TKey): ResultGetOrLockForLoad<T> = getOrLockForLoad(key, getType<T>())
    suspend inline fun <reified T: Any> getOrLoad(key: TKey, noinline fetch: (suspend () -> T)): T = getOrLoad(key, getType<T>(), fetch)
    suspend inline fun <reified T: Any> get(key: TKey): T? = get(key, getType<T>())
    suspend inline fun <reified T: Any> load(key: TKey, forceLoad: Boolean = false, noinline fetch: (suspend () -> T)) = load(key, forceLoad, getType<T>(), fetch)

    suspend inline fun <reified T: Any> multiGet(keys: Set<TKey>): Map<TKey, T?> = multiGet(keys, getType<T>())
    suspend inline fun <reified T: Any> multiGetTo(keys: Set<TKey>, missedKeys: MutableSet<TKey>, result: MutableMap<TKey, T>): Map<TKey, T> = multiGetTo(keys, missedKeys, result, getType<T>())
    suspend inline fun <reified T: Any> multiLoad(keyValues: Map<TKey, T>, forceLoad: Boolean = false) = multiLoad(keyValues, forceLoad, getType<T>())
    suspend inline fun <reified T: Any> multiGetOrLoad(keys: Set<TKey>, noinline fetch: (suspend (Set<TKey>) -> Map<TKey, T>)): Map<TKey, T> = multiGetOrLoad(keys, getType<T>(), fetch)
}

package im.toss.util.cache

import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.reflection.getType
import java.lang.reflect.Type


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

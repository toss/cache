package im.toss.util.cache.blocking

import im.toss.util.cache.CacheMode
import im.toss.util.cache.CacheValueLoader
import im.toss.util.cache.MultiFieldCache
import im.toss.util.concurrent.lock.MutexLock
import kotlinx.coroutines.runBlocking

class BlockingMultiFieldCache<TKey: Any>(val cache: MultiFieldCache<TKey>) {
    var mode: CacheMode
        get() = cache.options.cacheMode
        set(value) {
            cache.options.cacheMode = value
        }

    fun evict(key: TKey) = runBlocking {
        cache.evict(key)
    }

    inline fun <reified T : Any> load(key: TKey, field: String, crossinline fetch: () -> T) = runBlocking {
        cache.load(key, field) {
            fetch()
        }
    }

    inline fun <reified T : Any> get(key: TKey, field: String): T? = runBlocking {
        cache.get<T>(key, field)
    }

    inline fun <reified T : Any> getOrLoad(key: TKey, field: String, crossinline fetch: () -> T): T = runBlocking {
        cache.getOrLoad(key, field) {
            fetch()
        }
    }

    inline fun <reified T: Any> getOrLockForLoad(key: TKey, field: String): ResultBlockingGetOrLockForLoad<T> = runBlocking {
        cache.getOrLockForLoad<T>(key, field).blocking()
    }

    @Throws(MutexLock.FailedAcquireException::class)
    inline fun <reified T : Any> lockForLoad(key: TKey, field: String, timeout: Long = -1): BlockingCacheValueLoader<T> = runBlocking {
        cache.lockForLoad<T>(key, field, timeout).blocking()
    }

    inline fun <reified T:Any> optimisticLockForLoad(key: TKey, field: String): BlockingCacheValueLoader<T> = runBlocking {
        cache.optimisticLockForLoad<T>(key, field).blocking()
    }
}
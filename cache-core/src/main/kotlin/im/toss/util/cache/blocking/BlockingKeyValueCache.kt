package im.toss.util.cache.blocking

import im.toss.util.cache.CacheMode
import im.toss.util.cache.KeyValueCache
import im.toss.util.concurrent.lock.MutexLock
import kotlinx.coroutines.runBlocking

class BlockingKeyValueCache<TKey: Any>(val cache: KeyValueCache<TKey>) {
    var mode: CacheMode
        get() = cache.options.cacheMode
        set(value) {
            cache.options.cacheMode = value
        }

    fun evict(key: TKey) = runBlocking {
        cache.evict(key)
    }

    inline fun <reified T: Any> load(key: TKey, crossinline fetch: () -> T) = runBlocking {
        cache.load(key) {
            fetch()
        }
    }

    inline fun <reified T: Any> get(key: TKey): T? = runBlocking {
        cache.get<T>(key)
    }

    inline fun <reified T: Any> getOrLoad(key: TKey, crossinline fetch: () -> T): T = runBlocking {
        cache.getOrLoad(key) {
            fetch()
        }
    }

    inline fun <reified T: Any> getOrLockForLoad(key: TKey): ResultBlockingGetOrLockForLoad<T> = runBlocking {
        cache.getOrLockForLoad<T>(key).blocking()
    }

    @Throws(MutexLock.FailedAcquireException::class)
    inline fun <reified T : Any> lockForLoad(key: TKey, timeout: Long = -1): BlockingCacheValueLoader<T> = runBlocking {
        cache.lockForLoad<T>(key, timeout).blocking()
    }

    inline fun <reified T:Any> optimisticLockForLoad(key: TKey): BlockingCacheValueLoader<T> = runBlocking {
        cache.optimisticLockForLoad<T>(key).blocking()
    }
}
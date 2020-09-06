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

    fun <T: Any> load(key: TKey, fetch: () -> T) = runBlocking {
        cache.load(key) {
            fetch()
        }
    }

    fun <T: Any> get(key: TKey): T? = runBlocking {
        cache.get<T>(key)
    }

    fun <T: Any> getOrLoad(key: TKey, fetch: () -> T): T = runBlocking {
        cache.getOrLoad(key) {
            fetch()
        }
    }

    fun <T: Any> getOrLockForLoad(key: TKey): ResultBlockingGetOrLockForLoad<T> = runBlocking {
        cache.getOrLockForLoad<T>(key).blocking()
    }

    @Throws(MutexLock.FailedAcquireException::class)
    fun <T : Any> lockForLoad(key: TKey, timeout: Long = -1): BlockingCacheValueLoader<T> = runBlocking {
        cache.lockForLoad<T>(key, timeout).blocking()
    }

    fun <T:Any> optimisticLockForLoad(key: TKey): BlockingCacheValueLoader<T> = runBlocking {
        cache.optimisticLockForLoad<T>(key).blocking()
    }
}
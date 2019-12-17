package im.toss.util.cache.blocking

import im.toss.util.cache.CacheMode
import im.toss.util.cache.MultiFieldCache
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

    fun <T: Any> load(key: TKey, field: String, fetch: () -> T) = runBlocking {
        cache.load(key, field) {
            fetch()
        }
    }

    fun <T: Any> get(key: TKey, field: String): T? = runBlocking {
        cache.get<T>(key, field)
    }

    fun <T: Any> getOrLoad(key: TKey, field: String, fetch: () -> T): T = runBlocking {
        cache.getOrLoad(key, field) {
            fetch()
        }
    }
}
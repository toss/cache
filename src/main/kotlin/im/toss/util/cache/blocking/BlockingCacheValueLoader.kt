package im.toss.util.cache.blocking

import im.toss.util.cache.AlreadyLoadedException
import im.toss.util.cache.CacheValueLoader
import im.toss.util.cache.ResultGetOrLockForLoad
import kotlinx.coroutines.runBlocking

class BlockingCacheValueLoader<T: Any>(private val loader: CacheValueLoader<T>) {
    @Throws(AlreadyLoadedException::class)
    fun load(value: T): T = runBlocking {
        loader.load(value)
    }
}

class ResultBlockingGetOrLockForLoad<T: Any>(
    val value: T? = null,
    val loader: BlockingCacheValueLoader<T>? = null
)

fun <T: Any> CacheValueLoader<T>.blocking() = BlockingCacheValueLoader(this)
fun <T: Any> ResultGetOrLockForLoad<T>.blocking() = ResultBlockingGetOrLockForLoad(value, loader?.blocking())

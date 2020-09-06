package im.toss.util.cache.blocking

import im.toss.util.cache.AlreadyLoadedException
import im.toss.util.cache.CacheValueLoader
import im.toss.util.cache.LoadResult
import im.toss.util.cache.ResultGetOrLockForLoad
import kotlinx.coroutines.runBlocking

class BlockingCacheValueLoader<T: Any>(private val loader: CacheValueLoader<T>) {
    val version: Long get() = loader.version

    @Throws(AlreadyLoadedException::class)
    fun load(value: T): LoadResult<T> = runBlocking {
        loader.load(value)
    }

    fun release() = runBlocking { loader.release() }
}

class ResultBlockingGetOrLockForLoad<T: Any>(
    val value: T? = null,
    val loader: BlockingCacheValueLoader<T>? = null
)

fun <T: Any> CacheValueLoader<T>.blocking() = BlockingCacheValueLoader(this)
fun <T: Any> ResultGetOrLockForLoad<T>.blocking() = ResultBlockingGetOrLockForLoad(value, loader?.blocking())

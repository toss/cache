package im.toss.util.cache

import java.lang.Exception

interface CacheValueLoader<T: Any> {
    val version: Long

    @Throws(AlreadyLoadedException::class)
    suspend fun load(value: T): LoadResult<T>
    suspend fun release()
}

class AlreadyLoadedException : Exception()

class ResultGetOrLockForLoad<T: Any>(
    val value: T? = null,
    val loader: CacheValueLoader<T>? = null
)

data class LoadResult<T>(
    val value: T,
    val success: Boolean = true,
    val isOptimisticLockFailure: Boolean = false
)

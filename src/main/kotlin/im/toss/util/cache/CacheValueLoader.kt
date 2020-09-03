package im.toss.util.cache

import java.lang.Exception

interface CacheValueLoader<T: Any> {
    @Throws(AlreadyLoadedException::class)
    suspend fun load(value: T): T
}

class AlreadyLoadedException : Exception()

class ResultGetOrLockForLoad<T: Any>(
    val value: T? = null,
    val loader: CacheValueLoader<T>? = null
)

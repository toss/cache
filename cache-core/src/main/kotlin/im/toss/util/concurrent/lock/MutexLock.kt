package im.toss.util.concurrent.lock

import im.toss.util.coroutine.runWithTimeout
import kotlinx.coroutines.delay
import java.util.concurrent.TimeUnit
import kotlin.random.Random

abstract class MutexLock {
    object FailedAcquireException : Exception()

    abstract suspend fun acquire(key: String, timeout: Long = -1L, timeUnit: TimeUnit = TimeUnit.SECONDS): Boolean
    abstract suspend fun release(key: String): Boolean
    abstract suspend fun isAcquired(key: String): Boolean
}

private val random = Random(System.currentTimeMillis())

suspend fun <T> runOrRetry(fetch: (suspend () -> T)): T {
    var retryInterval = 3L
    while(true) {
        try {
            return fetch()
        } catch (e: MutexLock.FailedAcquireException) {
            val interval = random.nextLong(retryInterval, retryInterval * 2)
            retryInterval++
            delay(interval)
        }
    }
}

suspend fun <T> MutexLock.run(key: String, timeout: Long = -1, block: suspend () -> T): T {
    if (!acquire(key)) {
        throw MutexLock.FailedAcquireException
    }
    return try {
        runWithTimeout(timeout) {
            block()
        }
    } finally {
        release(key)
    }
}

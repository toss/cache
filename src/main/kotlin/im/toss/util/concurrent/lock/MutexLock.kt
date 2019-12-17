package im.toss.util.concurrent.lock

import im.toss.util.coroutine.runWithTimeout
import java.util.concurrent.TimeUnit

abstract class MutexLock {
    object FailedAcquireException : Exception()

    abstract suspend fun acquire(key: String, timeout: Long = -1L, timeUnit: TimeUnit = TimeUnit.SECONDS): Boolean
    abstract suspend fun release(key: String): Boolean
    abstract suspend fun isAcquired(key: String): Boolean
}

suspend fun <T> MutexLock.run(key: String, timeout: Long, block: suspend () -> T): T {
    return runWithTimeout(timeout) {
        if (!acquire(key)) {
            throw MutexLock.FailedAcquireException
        }
        try {
            block()
        } finally {
            release(key)
        }
    }
}

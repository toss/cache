package im.toss.util.coroutine

import kotlinx.coroutines.withTimeout

suspend fun <T> runWithTimeout(timeMillis: Long, block: suspend () -> T): T {
    return if (timeMillis > 0)
        withTimeout(timeMillis) {
            block()
        }
    else
        block()
}


package im.toss.util.cache

import im.toss.util.concurrent.lock.MutexLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class LocalMutexLock(private val timeoutMillis:Long) : MutexLock() {
    private val items = ConcurrentHashMap<String, Long>()
    override suspend fun acquire(key: String, timeout: Long, timeUnit: TimeUnit): Boolean {
        synchronized(this) {
            return if (internalIsLocked(key)) {
                false
            } else {
                val ttl = when {
                    timeout == 0L -> 0L
                    timeout < 0 -> timeoutMillis
                    else -> timeUnit.toMillis(timeout)
                }
                items[key] = System.currentTimeMillis() + ttl
                true
            }
        }
    }

    override suspend fun release(key: String):Boolean = synchronized(this) {
        val isLocked = internalIsLocked(key)
        items.remove(key)
        isLocked
    }

    override suspend fun isAcquired(key: String): Boolean = internalIsLocked(key)

    private fun internalIsLocked(key: String): Boolean = synchronized(this) {
        val now = System.currentTimeMillis()
        val expired = items[key]
        return if (expired == null) false else now < expired
    }
}
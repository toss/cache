package im.toss.util.concurrent.lock

import im.toss.util.repository.InMemoryRepository
import java.util.concurrent.TimeUnit
import kotlin.math.max

class InMemoryMutexLock(
    private val autoreleaseSeconds: Long,
    private val repository: InMemoryRepository
) : MutexLock() {
    override suspend fun acquire(key: String, timeout: Long, timeUnit: TimeUnit): Boolean {
        val value = System.currentTimeMillis().toString().toByteArray()

        val ttl = when {
            timeout == 0L -> 0L
            timeout < 0 -> autoreleaseSeconds
            else -> max(1L, timeUnit.toSeconds(timeout))
        }
        return repository.setnex(rawKey(key), ttl, TimeUnit.SECONDS, value)
    }

    override suspend fun release(key: String): Boolean {
        return repository.del(rawKey(key))
    }

    override suspend fun isAcquired(key: String): Boolean {
        return repository.exists(rawKey(key))
    }

    private fun rawKey(key: String) = "$key:lock"
}
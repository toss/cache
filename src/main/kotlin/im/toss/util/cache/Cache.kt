package im.toss.util.cache

import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.cache.metrics.CacheMeter
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

open class Cache(override val name: String) : CacheMeter {
    data class KeyFunction(
        val function: (name: String, version: String, key: Any) -> String
    )

    companion object {
        private val random = Random(System.currentTimeMillis())

        @JvmStatic
        protected suspend fun <T:Any> runOrRetry(fetch: (suspend () -> T)): T {
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
    }

    override val missCount: Long get() = _missCount.get()
    override val hitCount: Long get() = _hitCount.get()
    override val putCount: Long get() = _putCount.get()
    override val evictionCount: Long? get() = _evictionCount.get()
    override val size: Long? get() = null

    private val _missCount = AtomicLong()
    private val _hitCount = AtomicLong()
    private val _putCount = AtomicLong()
    private val _evictionCount = AtomicLong()

    protected fun incrementMissCount() { _missCount.incrementAndGet() }
    protected fun incrementHitCount() { _hitCount.incrementAndGet() }
    protected fun incrementPutCount() { _putCount.incrementAndGet() }
    protected fun incrementEvictCount() { _evictionCount.incrementAndGet() }
}
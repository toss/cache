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
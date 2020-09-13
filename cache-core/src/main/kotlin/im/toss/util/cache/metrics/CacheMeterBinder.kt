package im.toss.util.cache.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.atomic.AtomicLong

interface CacheMeter {
    val name: String
    val missCount: Long
    val hitCount: Long
    val putCount: Long
    val evictionCount: Long?
    val size: Long?
}

class CacheMetrics(override val name: String): CacheMeter {
    override val missCount: Long get() = _missCount.get()
    override val hitCount: Long get() = _hitCount.get()
    override val putCount: Long get() = _putCount.get()
    override val evictionCount: Long? get() = _evictionCount.get()
    override val size: Long? get() = null

    private val _missCount = AtomicLong()
    private val _hitCount = AtomicLong()
    private val _putCount = AtomicLong()
    private val _evictionCount = AtomicLong()

    fun incrementMissCount() { _missCount.incrementAndGet() }
    fun incrementHitCount() { _hitCount.incrementAndGet() }
    fun incrementPutCount() { _putCount.incrementAndGet() }
    fun incrementEvictCount() { _evictionCount.incrementAndGet() }
}

class CacheMeterBinder(private val cache: CacheMeter, tags: Iterable<Tag>) :
    io.micrometer.core.instrument.binder.cache.CacheMeterBinder(cache, cache.name, tags)
{
    override fun size(): Long? = cache.size
    override fun hitCount(): Long = cache.hitCount
    override fun missCount(): Long = cache.missCount
    override fun evictionCount(): Long? = cache.evictionCount
    override fun putCount(): Long = cache.putCount

    override fun bindImplementationSpecificMetrics(registry: MeterRegistry) {

    }
}

fun <T: CacheMeter> T.metrics(meterRegistry: MeterRegistry, tags: Iterable<Tag> = emptyList()): T {
    val cacheMeterBinder = CacheMeterBinder(this, tags)
    cacheMeterBinder.bindTo(meterRegistry)
    return this
}
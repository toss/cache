package im.toss.util.cache.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag

interface CacheMeter {
    val name: String
    val missCount: Long
    val hitCount: Long
    val putCount: Long
    val evictionCount: Long?
    val size: Long?
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
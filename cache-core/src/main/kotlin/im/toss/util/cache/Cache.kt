package im.toss.util.cache

import im.toss.util.cache.metrics.CacheMetrics
import io.micrometer.core.instrument.Tag

interface Cache {
    data class KeyFunction(
        val function: (name: String, key: Any) -> String
    )

    data class CustomTagsFunction(
        val function: (namespaceId: String, cacheMetrics: CacheMetrics) -> List<Tag>
    )
}

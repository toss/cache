package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.CacheManager
import im.toss.util.cache.CacheNamespace
import im.toss.util.cache.MultiFieldCache
import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.cache.spring.CacheGroupDefinition
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
@ConditionalOnBean(annotation = [EnableCacheSupport::class])
class CacheGroupManager(
    val cacheManager: CacheManager,
    cacheGroupDefinitions: List<CacheGroupDefinition>
) {
    val groups = cacheGroupDefinitions.associateBy { it.groupId }

    init {
        cacheManager.resources {
            cacheGroupDefinitions.forEach {
                namespace(it.groupId) {
                    CacheNamespace(
                        resourceId = it.resourceId ?: "default",
                        serializerId = "ByteArraySerializer"
                    )
                }
            }
        }
    }

    fun get(namespaceId: String): MultiFieldCache<String> = cacheManager.multiFieldCache(namespaceId, "ByteArraySerializer")

    fun getBlocking(namespaceId: String): BlockingMultiFieldCache<String> = get(namespaceId).blocking

    fun evict(groupId: String, key: String) = getBlocking(groupId).evict(key)

    fun cacheKey(context: Map<String, String>): String {
        return context.entries.sortedBy { it.key }.joinToString(",") { "${it.key}=${it.value}" }
    }
}

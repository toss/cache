package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.CacheManager
import im.toss.util.cache.MultiFieldCache
import im.toss.util.cache.blocking.BlockingMultiFieldCache
import im.toss.util.cache.cacheOptions
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
    private val cacheGroups = ConcurrentHashMap<String, MultiFieldCache<String>>()
    private val blockingCacheGroups = ConcurrentHashMap<String, BlockingMultiFieldCache<String>>()

    fun get(groupId: String): MultiFieldCache<String> =
        cacheGroups.computeIfAbsent(groupId) {
            val groupDefinition = groups[groupId] ?: CacheGroupDefinition(groupId)
            cacheManager.multiFieldCache(
                namespaceId = groupId,
                serializerId = "ByteArraySerializer",
                resourceId = groupDefinition.resourceId,
                options = groupDefinition.options ?: cacheOptions(ttl = 10)
            )
        }

    fun getBlocking(groupId: String): BlockingMultiFieldCache<String> =
        blockingCacheGroups.computeIfAbsent(groupId) {
            get(groupId).blocking()
        }

    fun evict(groupId: String, key: String) = getBlocking(groupId).evict(key)

    fun cacheKey(context: Map<String, String>): String {
        return context.entries.sortedBy { it.key }.joinToString(",") { "${it.key}=${it.value}" }
    }
}

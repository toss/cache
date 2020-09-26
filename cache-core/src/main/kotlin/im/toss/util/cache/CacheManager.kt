package im.toss.util.cache

import im.toss.util.cache.resources.RedisClusterCacheResources
import im.toss.util.data.serializer.Serializer
import im.toss.util.cache.metrics.metrics
import im.toss.util.cache.resources.InMemoryCacheResources
import im.toss.util.data.serializer.ByteArraySerializer
import io.lettuce.core.cluster.RedisClusterClient
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.ConcurrentHashMap

typealias CacheResourcesDsl = CacheManager.ResourcesDefinition.() -> Unit

class CacheManager(
    private val meterRegistry: MeterRegistry,
    resourcesDsl: CacheResourcesDsl? = null
) {
    fun resources(dsl: CacheResourcesDsl): CacheManager {
        dsl(ResourcesDefinition(this))
        return this
    }

    fun <TKey: Any> keyValueCache(
        name: String,
        serializerId: String? = null,
        lockAutoreleaseSeconds: Long = 10
    ): KeyValueCache<TKey> {
        val namespace = getNamespace(name)
        val resources = getResources(namespace.resourceId)
        return KeyValueCache<TKey>(
            name = name,
            keyFunction = keyFunction,
            lock = resources.lock(lockAutoreleaseSeconds),
            repository = resources.keyFieldValueRepository(),
            serializer = getSerializer(serializerId),
            options = namespace.options
        ).metrics(meterRegistry)
    }

    fun <TKey: Any> multiFieldCache(
        name: String,
        serializerId: String? = null,
        lockAutoreleaseSeconds: Long = 10
    ): MultiFieldCache<TKey> {
        val namespace = getNamespace(name)
        val resources = getResources(namespace.resourceId)
        return MultiFieldCache<TKey>(
            name = name,
            keyFunction = keyFunction,
            lock = resources.lock(lockAutoreleaseSeconds),
            repository = resources.keyFieldValueRepository(),
            serializer = getSerializer(serializerId),
            options = namespace.options
        ).metrics(meterRegistry)
    }

    private var keyFunction: Cache.KeyFunction = Cache.KeyFunction { name, key -> "cache:$name:$key" }

    private val resources = ConcurrentHashMap<String, CacheResources>()
    private val namespaces = ConcurrentHashMap<String, CacheNamespace>()
    private val serializers = ConcurrentHashMap<String, Serializer>(mapOf("ByteArraySerializer" to ByteArraySerializer))
    private fun setKeyFunction(keyFunction: Cache.KeyFunction) {
        this.keyFunction = keyFunction
    }

    private fun addResource(resourceId: String, resources: CacheResources) {
        if (this.resources.containsKey(resourceId)) {
            throw Exception("alread exists resource: $resourceId")
        }

        this.resources[resourceId] = resources
    }

    private fun getResources(resourceId: String? = null): CacheResources {
        return resources[resourceId ?: "default"]
            ?: throw NoSuchElementException("resource not exists: $resourceId")
    }

    private fun addNamespace(namespaceId: String, namespace: CacheNamespace) {
        if (namespaces.containsKey(namespaceId)) {
            throw Exception("alread exists namespace: $namespaceId")
        }

        namespaces[namespaceId] = namespace
    }

    private fun getNamespace(namespaceId: String? = null): CacheNamespace {
        return namespaces[namespaceId ?: "default"]
            ?: throw NoSuchElementException("namespace not exists: $namespaceId")
    }

    private fun addSerializer(serializerId: String, serializer: Serializer) {
        if (serializers.containsKey(serializerId)) {
            throw Exception("alread exists serializer: $serializerId")
        }

        serializers[serializerId] = serializer
    }

    private fun getSerializer(serializerId: String? = null): Serializer {
        return serializers[serializerId ?: "default"]
            ?: throw NoSuchElementException("serializer not exists: $serializerId")
    }

    init {
        if (resourcesDsl != null) {
            resources(resourcesDsl)
        }
    }

    class ResourcesDefinition(private val cacheManager: CacheManager) {
        fun keyFunction(block: (name: String, key: Any) -> String) {
            cacheManager.setKeyFunction(Cache.KeyFunction(block))
        }

        fun redisCluster(
            resourceId: String = "default",
            readTimeoutMillis: Long = 3000,
            block: () -> RedisClusterClient
        ) {
            cacheManager.addResource(resourceId,
                RedisClusterCacheResources(readTimeoutMillis = readTimeoutMillis, client = block())
            )
        }

        fun inMemory(
            resourceId: String = "default"
        ) {
            cacheManager.addResource(resourceId,
                InMemoryCacheResources()
            )
        }

        fun serializer(id: String = "default", block: () -> Serializer) {
            cacheManager.addSerializer(id, block())
        }

        fun namespace(id: String = "default", block: () -> CacheNamespace) {
            cacheManager.addNamespace(id, block())
        }
    }
}

fun cacheResources(dsl: CacheResourcesDsl): CacheResourcesDsl {
    return dsl
}

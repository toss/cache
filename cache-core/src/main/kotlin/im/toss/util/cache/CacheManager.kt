package im.toss.util.cache

import im.toss.util.cache.impl.KeyValueCacheImpl
import im.toss.util.cache.impl.MultiFieldCacheImpl
import im.toss.util.cache.metrics.CacheMetrics
import im.toss.util.cache.resources.RedisClusterCacheResources
import im.toss.util.data.serializer.Serializer
import im.toss.util.cache.metrics.metrics
import im.toss.util.cache.resources.InMemoryCacheResources
import im.toss.util.data.serializer.ByteArraySerializer
import im.toss.util.properties.InheritableProperties
import io.lettuce.core.cluster.RedisClusterClient
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap

typealias CacheResourcesDsl = CacheManager.ResourcesDefinition.() -> Unit

class CacheManager(
    private val meterRegistry: MeterRegistry,
    resourcesDsl: CacheResourcesDsl? = null
) {
    fun loadProperties(properties: Map<String, String>, prefix: String? = null) {
        val props = InheritableProperties(properties, prefix)
        val namespaces = props.instantiates<CacheNamespace>(
            path = "toss.cache.namespace",
            parentsPath = "toss.cache.preset.namespace",
            parentFieldName = "preset"
        )

        resources {
            namespaces.forEach { (id, namespace) ->
                namespace(id) { namespace }
            }
       }
    }

    fun resources(dsl: CacheResourcesDsl): CacheManager {
        dsl(ResourcesDefinition(this))
        return this
    }

    fun <TKey: Any> keyValueCache(
        namespaceId: String
    ): KeyValueCache<TKey> {
        @Suppress("UNCHECKED_CAST")
        return keyValueCaches.computeIfAbsent(namespaceId) {
            val namespace = getNamespace(namespaceId)
            val resources = getResources(namespace.resourceId)
            KeyValueCacheImpl<TKey>(
                name = namespaceId,
                keyFunction = keyFunction,
                lock = resources.lock(namespace.options.run { lockTimeout.seconds }),
                repository = resources.keyFieldValueRepository(),
                serializer = getSerializer(namespace.serializerId),
                options = namespace.options,
                metrics = getMetrics(namespaceId)
            )
        } as KeyValueCache<TKey>
    }

    fun <TKey: Any> keyValueCache(
        namespaceId: String,
        serializerId: String
    ): KeyValueCache<TKey> {
        @Suppress("UNCHECKED_CAST")
        return keyValueCacheWithSerializers.computeIfAbsent(namespaceId to serializerId) {
            val namespace = getNamespace(namespaceId)
            val resources = getResources(namespace.resourceId)
            KeyValueCacheImpl<TKey>(
                name = namespaceId,
                keyFunction = keyFunction,
                lock = resources.lock(namespace.options.run { lockTimeout.seconds }),
                repository = resources.keyFieldValueRepository(),
                serializer = getSerializer(serializerId),
                options = namespace.options,
                metrics = getMetrics(namespaceId)
            )
        } as KeyValueCache<TKey>
    }

    fun <TKey: Any> multiFieldCache(
        namespaceId: String
    ): MultiFieldCache<TKey> {
        @Suppress("UNCHECKED_CAST")
        return multiFieldCaches.computeIfAbsent(namespaceId) {
            val namespace = getNamespace(namespaceId)
            val resources = getResources(namespace.resourceId)
            MultiFieldCacheImpl<TKey>(
                name = namespaceId,
                keyFunction = keyFunction,
                lock = resources.lock(namespace.options.run { lockTimeout.seconds }),
                repository = resources.keyFieldValueRepository(),
                serializer = getSerializer(namespace.serializerId),
                options = namespace.options,
                metrics = getMetrics(namespaceId)
            )
        } as MultiFieldCache<TKey>
    }

    fun <TKey: Any> multiFieldCache(
        namespaceId: String,
        serializerId: String
    ): MultiFieldCache<TKey> {
        @Suppress("UNCHECKED_CAST")
        return multiFieldCacheWithSerializers.computeIfAbsent(namespaceId to serializerId) {
            val namespace = getNamespace(namespaceId)
            val resources = getResources(namespace.resourceId)
            MultiFieldCacheImpl<TKey>(
                name = namespaceId,
                keyFunction = keyFunction,
                lock = resources.lock(namespace.options.run { lockTimeout.seconds }),
                repository = resources.keyFieldValueRepository(),
                serializer = getSerializer(serializerId),
                options = namespace.options,
                metrics = getMetrics(namespaceId)
            )
        } as MultiFieldCache<TKey>
    }

    suspend fun <TKey: Any> evict(namespaceId: String, key: TKey) {
        multiFieldCache<TKey>(namespaceId).evict(key)
    }

    fun <TKey: Any> evictBlocking(namespaceId: String, key: TKey) {
        runBlocking { evict(namespaceId, key) }
    }


    private var keyFunction: Cache.KeyFunction = Cache.KeyFunction { name, key -> "cache:$name:$key" }
    private var customTagsFunction: Cache.CustomTagsFunction = Cache.CustomTagsFunction { namespaceId, cacheMetrics -> emptyList() }

    private val resources = ConcurrentHashMap<String, CacheResources>()
    private val namespaces = ConcurrentHashMap<String, CacheNamespace>()
    private val serializers = ConcurrentHashMap<String, Serializer>(mapOf("ByteArraySerializer" to ByteArraySerializer))
    private val metricises = ConcurrentHashMap<String, CacheMetrics>()
    private val multiFieldCaches = ConcurrentHashMap<String, MultiFieldCache<*>>()
    private val multiFieldCacheWithSerializers = ConcurrentHashMap<Pair<String, String>, MultiFieldCache<*>>()
    private val keyValueCaches = ConcurrentHashMap<String, KeyValueCache<*>>()
    private val keyValueCacheWithSerializers = ConcurrentHashMap<Pair<String, String>, KeyValueCache<*>>()

    private fun setKeyFunction(keyFunction: Cache.KeyFunction) {
        this.keyFunction = keyFunction
    }

    private fun setCustomTags(customTagsFunction: Cache.CustomTagsFunction) {
        this.customTagsFunction = customTagsFunction
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

        namespaces.computeIfAbsent(namespaceId) {
            metricises.computeIfAbsent(namespaceId) {
                CacheMetrics(namespaceId)
                    .apply {
                        metrics(meterRegistry, customTagsFunction.function(namespaceId, this))
                    }
            }
            namespace
        }
    }

    fun getNamespace(namespaceId: String? = null): CacheNamespace {
        return namespaces[namespaceId ?: "default"]
            ?: throw NoSuchElementException("namespace not exists: $namespaceId")
    }

    fun getMetrics(namespaceId: String? = null): CacheMetrics {
        return metricises[namespaceId ?: "default"]
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

        fun metricsCustomTags(block: (namespaceId: String, cacheMetrics: CacheMetrics) -> List<Tag>) {
            cacheManager.setCustomTags(Cache.CustomTagsFunction(block))
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

package im.toss.util.cache

import im.toss.util.cache.resources.RedisClusterCacheResources
import im.toss.util.data.serializer.Serializer
import im.toss.util.cache.metrics.metrics
import im.toss.util.cache.resources.InMemoryCacheResources
import io.lettuce.core.cluster.RedisClusterClient
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.ConcurrentHashMap

typealias CacheResourcesDsl = CacheManager.ResoucesDefinition.() -> Unit

class CacheManager(
    private val meterRegistry: MeterRegistry,
    resourcesDsl: CacheResourcesDsl? = null
) {
    fun resources(dsl: CacheResourcesDsl): CacheManager {
        dsl(ResoucesDefinition(this))
        return this
    }

    fun <TKey: Any> keyValueCache(
        name: String,
        options: CacheOptions,
        resourceId: String? = null,
        serializerId: String? = null,
        lockAutoreleaseSeconds: Long = 10
    ): KeyValueCache<TKey> {
        val resouces = getResources(resourceId)
        @Suppress("UNCHECKED_CAST")
        return cacheInstances.getOrPut(name) {
            KeyValueCache<TKey>(
                name = name,
                keyFunction = keyFunction,
                lock = resouces.lock(lockAutoreleaseSeconds),
                repository = resouces.keyValueRepository(),
                serializer = getSerializer(serializerId),
                options = options
            ).metrics(meterRegistry)
        } as KeyValueCache<TKey>
    }

    fun <TKey: Any> multiFieldCache(
        name: String,
        options: CacheOptions,
        resourceId: String? = null,
        serializerId: String? = null,
        lockAutoreleaseSeconds: Long = 10
    ): MultiFieldCache<TKey> {
        val resouces = getResources(resourceId)
        @Suppress("UNCHECKED_CAST")
        return cacheInstances.getOrPut(name) {
            MultiFieldCache<TKey>(
                name = name,
                keyFunction = keyFunction,
                lock = resouces.lock(lockAutoreleaseSeconds),
                repository = resouces.keyFieldValueRepository(),
                serializer = getSerializer(serializerId),
                options = options
            ).metrics(meterRegistry)
        } as MultiFieldCache<TKey>
    }

    private var keyFunction: Cache.KeyFunction = Cache.KeyFunction { name, version, key ->
        "cache:$name.$version:$key"
    }

    private val resources = ConcurrentHashMap<String, CacheResources>()
    private val serializers = ConcurrentHashMap<String, Serializer>()
    private val cacheInstances = ConcurrentHashMap<String, Cache>()
    private fun setKeyFunction(keyFunction: Cache.KeyFunction) {
        this.keyFunction = keyFunction
    }

    private fun addResource(resourceId: String, resouces: CacheResources) {
        if (resources.containsKey(resourceId)) {
            throw Exception("alread exists resource: $resourceId")
        }

        resources[resourceId] = resouces
    }

    private fun getResources(resourceId: String? = null): CacheResources {
        val id = resourceId ?: "default"
        if (!resources.containsKey(id)) {
            throw NoSuchElementException("resource not exists: $id")
        }
        return resources[id]!!
    }

    private fun addSerializer(serializerId: String, serializer: Serializer) {
        if (serializers.containsKey(serializerId)) {
            throw Exception("alread exists serializer: $serializerId")
        }

        serializers[serializerId] = serializer
    }

    private fun getSerializer(serializerId: String? = null): Serializer {
        val id = serializerId ?: "default"
        if (!serializers.containsKey(id)) {
            throw NoSuchElementException("serializer not exists: $id")
        }
        return serializers[id]!!
    }

    init {
        if (resourcesDsl != null) {
            resources(resourcesDsl)
        }
    }

    class ResoucesDefinition(private val cacheManager: CacheManager) {
        fun keyFunction(block: (name: String, version: String, key: Any) -> String) {
            cacheManager.setKeyFunction(Cache.KeyFunction(block))
        }

        fun redisCluster(resourceId: String = "default", block: () -> RedisClusterClient) {
            cacheManager.addResource(resourceId,
                RedisClusterCacheResources(block())
            )
        }

        fun inMemory(resourceId: String = "default") {
            cacheManager.addResource(resourceId, InMemoryCacheResources())
        }

        fun serializer(id: String = "default", block: () -> Serializer) {
            cacheManager.addSerializer(id, block())
        }
    }
}

fun cacheResources(dsl: CacheResourcesDsl): CacheResourcesDsl {
    return dsl
}

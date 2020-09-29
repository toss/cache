package im.toss.util.cache

import im.toss.test.equalsTo
import im.toss.util.data.serializer.StringSerializer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.time.Duration
import java.util.concurrent.TimeUnit

internal class CacheManagerTest {
    @Test
    fun `loadProperties test`() {
        val properties = mapOf(
            "preset.namespace.global.options.version" to "1",
            "preset.namespace.global.options.cache-mode" to "normal",
            "preset.namespace.global.options.cold-time" to "PT1S",
            "preset.namespace.global.options.lock-timeout" to "PT30S",
            "preset.namespace.global.options.cache-failure-policy" to "fallback",

            "preset.namespace.default.preset" to "global",
            "preset.namespace.default.options.version" to "2",
            "preset.namespace.default.options.ttl" to "PT10S",
            "preset.namespace.default.options.apply-ttl-if-hit" to "true",

            "namespace.first.resource-id" to "default",
            "namespace.first.preset" to "default",
            "namespace.first.serializer-id" to "default",
            "namespace.first.options.ttl" to "PT30S",

            "namespace.second.resource-id" to "default2",
            "namespace.second.preset" to "default",
            "namespace.second.options.ttl" to "PT10S"
        )

        val cacheManager = CacheManager(SimpleMeterRegistry())
        cacheManager.loadProperties(properties, "toss.cache")

        cacheManager.getNamespace("first") equalsTo
                CacheNamespace(
                    resourceId = "default",
                    serializerId = "default",
                    options = CacheOptions(
                        version = "2",
                        cacheMode = CacheMode.NORMAL,
                        ttl = Duration.ofSeconds(30L),
                        applyTtlIfHit = true,
                        coldTime = Duration.ofSeconds(1L),
                        cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin
                    )
                )

        cacheManager.getNamespace("second") equalsTo
                CacheNamespace(
                    resourceId = "default2",
                    serializerId = null,
                    options = CacheOptions(
                        version = "2",
                        cacheMode = CacheMode.NORMAL,
                        ttl = Duration.ofSeconds(10L),
                        applyTtlIfHit = true,
                        coldTime = Duration.ofSeconds(1L),
                        cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin
                    )
                )
    }

    @Test
    fun `inMemory keyValueCache`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory()
                serializer { StringSerializer }
                namespace("") {
                    CacheNamespace(
                        options = cacheOptions(
                            "1",
                            CacheMode.NORMAL,
                            1,
                            TimeUnit.SECONDS,
                            true,
                            1,
                            TimeUnit.SECONDS,
                            10,
                            TimeUnit.SECONDS,
                            cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin
                        )
                    )
                }
            }

            val cache = cacheManager.keyValueCache<String>("")

            cache.load("hello") { "world" }
            cache.get<String>("hello") equalsTo "world"
        }
    }

    @Test
    fun `inMemory multiFieldCache`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory()
                serializer { StringSerializer }
                namespace("") {
                    CacheNamespace(
                        options = cacheOptions(
                            "1",
                            CacheMode.NORMAL,
                            1,
                            TimeUnit.SECONDS,
                            true,
                            1,
                            TimeUnit.SECONDS,
                            10,
                            TimeUnit.SECONDS,
                            cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin
                        )
                    )

                }
            }
            val cache = cacheManager.multiFieldCache<String>("")

            cache.load("hello", "field") { "world" }
            cache.get<String>("hello", "field") equalsTo "world"
        }
    }

    @Test
    fun `namespace`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory("first")
                serializer { StringSerializer }
                namespace("hello") {
                    CacheNamespace(
                        resourceId = "first",
                        options = cacheOptions(
                            version = "1",
                            cacheMode = CacheMode.NORMAL,
                            ttl = 10
                        )
                    )
                }
            }

            val cache1 = cacheManager.keyValueCache<String>("hello")
            cache1.load("hello") { "world" }
            cache1.get<String>("hello") equalsTo "world"


            val cache2= cacheManager.multiFieldCache<String>("hello")
            cache2.load("hello", "field") { "world" }
            cache2.get<String>("hello", "field") equalsTo "world"
        }
    }

    @Test
    fun `metrics are shared between the same namespace`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory("first")
                serializer { StringSerializer }
                namespace("hello") {
                    CacheNamespace(
                        resourceId = "first",
                        options = cacheOptions(
                            version = "1",
                            cacheMode = CacheMode.NORMAL,
                            ttl = 10
                        )
                    )
                }
            }
            val metrics = cacheManager.getMetrics("hello")
            val cache1 = cacheManager.keyValueCache<String>("hello")
            val cache2 = cacheManager.multiFieldCache<String>("hello")
            cache1.getOrLoad("1") { "value" } // miss, put
            cache1.getOrLoad("1") { "value" } // hit
            cache1.getOrLoad("1") { "value" } // hit

            cache2.getOrLoad("1", "1") { "field1value" } // miss, put
            cache2.getOrLoad("1", "1") { "field1value" } // hit
            cache2.getOrLoad("1", "1") { "field1value" } // hit

            cache1.getOrLoad("1") { "value2"} equalsTo "value" // hit
            cache2.getOrLoad("1", "1") { "field1value2" } equalsTo "field1value" // hit

            metrics.run {
                missCount equalsTo 2L
                putCount equalsTo 2L
                hitCount equalsTo 6L
            }
        }
    }

    @Test
    fun `use a different serializer in the same namespace`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory("first")
                serializer { StringSerializer }
                namespace("hello") {
                    CacheNamespace(
                        resourceId = "first",
                        options = cacheOptions(
                            version = "1",
                            cacheMode = CacheMode.NORMAL,
                            ttl = 10
                        )
                    )
                }
            }

            val cache1 = cacheManager.keyValueCache<String>("hello")
            cache1.getOrLoad("key1") { "value1" }

            val cache2 = cacheManager.keyValueCache<String>("hello", "ByteArraySerializer")
            cache2.get<ByteArray>("key1") equalsTo "value1".toByteArray()
        }
    }
}
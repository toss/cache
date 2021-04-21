package im.toss.util.cache

import im.toss.test.equalsTo
import im.toss.util.properties.InheritableProperties
import org.junit.jupiter.api.Test
import java.time.Duration

internal class CacheNamespaceTest {
    @Test
    fun properties() {
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
            "namespace.second.options.ttl" to "PT10S",
            "namespace.second.options.run-with-isolated-thread" to "true"
        )
        val source = InheritableProperties(properties, "toss.cache")
        source.instantiates<CacheNamespace>(
            path = "toss.cache.namespace",
            parentsPath = "toss.cache.preset.namespace",
            parentFieldName = "preset"
        ) equalsTo mapOf(
            "first" to CacheNamespace(
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
            ),
            "second" to CacheNamespace(
                resourceId = "default2",
                serializerId = null,
                options = CacheOptions(
                    version = "2",
                    cacheMode = CacheMode.NORMAL,
                    ttl = Duration.ofSeconds(10L),
                    applyTtlIfHit = true,
                    coldTime = Duration.ofSeconds(1L),
                    cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin,
                    runWithIsolatedThread = true
                )
            )
        )
    }
}
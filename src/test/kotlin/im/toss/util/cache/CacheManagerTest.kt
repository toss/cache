package im.toss.util.cache

import im.toss.test.equalsTo
import im.toss.util.data.serializer.StringSerializer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.TimeUnit

internal class CacheManagerTest {

    @Test
    fun `inMemory keyValueCache`() {
        runBlocking {
            val cacheManager = CacheManager(SimpleMeterRegistry()) {
                keyFunction { name, key -> "$name:$key" }
                inMemory()
                serializer { StringSerializer }
            }

            val cache = cacheManager.keyValueCache<String>(
                "",
                cacheOptions(
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
            }

            val cache = cacheManager.multiFieldCache<String>(
                "",
                cacheOptions(
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

            cache.load("hello", "field") { "world" }
            cache.get<String>("hello", "field") equalsTo "world"
        }
    }
}
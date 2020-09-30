package im.toss.util.cache.spring.webmvc

import im.toss.SpringWebMvcTest
import im.toss.test.equalsTo
import im.toss.util.cache.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration

internal class CacheConfigurationTest: SpringWebMvcTest() {
    @Autowired
    lateinit var cacheManager: CacheManager

    @Test
    fun test() {
        val namespace = cacheManager.getNamespace("hello")
        namespace equalsTo CacheNamespace(
            resourceId = "default",
            serializerId = null,
            options = CacheOptions(
                version = "1",
                cacheMode = CacheMode.NORMAL,
                ttl = Duration.ofSeconds(10),
                applyTtlIfHit = true,
                coldTime = Duration.ofSeconds(1),
                cacheFailurePolicy = CacheFailurePolicy.FallbackToOrigin
            )
        )
    }
}
package im.toss.util.cache.spring

import im.toss.SpringWebMvcTest
import im.toss.test.equalsTo
import im.toss.util.cache.cacheOptions
import im.toss.util.cache.spring.webmvc.CacheGroupManager
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

class CacheGroupDefinitionTest: SpringWebMvcTest() {
    @Autowired
    lateinit var cacheGroupManager: CacheGroupManager

    @Test
    fun test() {
        cacheGroupManager.groups.keys equalsTo setOf("default", "group1", "group2")
        (cacheGroupManager.groups["group1"] ?: error("")).options!!.ttl equalsTo 10L
        (cacheGroupManager.groups["group2"] ?: error("")).options equalsTo null
    }
}

@Configuration
class CacheGroupDefinitionTestConfiguration {
    @Bean
    fun cacheGroup1() = CacheGroupDefinition("group1", options = cacheOptions(ttl = 10L))

    @Bean
    fun cacheGroup2() = CacheGroupDefinition("group2")
}
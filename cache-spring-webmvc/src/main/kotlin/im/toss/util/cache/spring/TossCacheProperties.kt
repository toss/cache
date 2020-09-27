package im.toss.util.cache.spring

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "toss")
class TossCacheProperties {
    lateinit var cache: Map<String, String>
}

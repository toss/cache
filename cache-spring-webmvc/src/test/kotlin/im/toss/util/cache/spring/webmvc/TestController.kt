package im.toss.util.cache.spring.webmvc

import org.springframework.web.bind.annotation.*
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

data class TestResponse(
    val index: Long,
    val lastUpdated: ZonedDateTime
)

@RestController
@RequestMapping("/test")
class TestController {
    @ResponseCacheSupport("default")
    @GetMapping("/get")
    fun get(
        @CacheKey("userNo") @RequestHeader("user-no") userNo: Long,
        @CacheField("idx") @RequestHeader("index") index: Long
    ): TestResponse {
        return TestResponse(index, lastUpdated = ZonedDateTime.now())
    }

    @EvictCache(["default"])
    @PostMapping("/evict")
    fun evict(
        @CacheKey("userNo") @RequestHeader("user-no") userNo: Long
    ): String {
        return "OK"
    }

    @ResponseCacheSupport("default")
    @GetMapping("/no-cache-key")
    fun noCacheKey(): String {
        return ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }
}

package im.toss.util.cache.spring.webmvc

import im.toss.SpringWebMvcTest
import im.toss.request
import im.toss.test.doesNotEqualTo
import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpMethod
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.ZonedDateTime

class ResponseCacheTest: SpringWebMvcTest() {
    @Autowired
    lateinit var cacheGroupManager: CacheGroupManager

    @Test
    fun `캐시에 응답을 적재하고, 다시 요청하면 캐싱된 응답을 한다`() {
        val headers = mapOf(
            "content-type" to "application/json",
            "user-no" to "1"
        )

        val result1a = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        val result2a = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "2"))
        Thread.sleep(1000)
        val result1b = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        val result2b = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "2"))

        fun ResponseEntity<TestResponse>.equalsTo(other:ResponseEntity<TestResponse>) {
            statusCode equalsTo other.statusCode
            body equalsTo other.body
        }

        result1a.equalsTo(result1b)
        result2a.equalsTo(result2b)
    }

    @Test
    fun `no-cache 헤더를 보내면 캐시가 갱신된다`() {
        val headers = mapOf(
            "content-type" to "application/json",
            "user-no" to "1"
        )

        val result1 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "10"))
        val result2 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "10", "cache-control" to "no-cache"))
        result1.statusCode equalsTo result2.statusCode
        result1.body doesNotEqualTo result2.body
    }

    @Test
    fun `evict`() {
        val headers = mapOf(
            "content-type" to "application/json",
            "user-no" to "1"
        )

        val result1 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        Thread.sleep(1000)
        client.request<String>(method = HttpMethod.POST, path = "/test/evict", headers = headers)
        val result2 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        result1.body doesNotEqualTo result2.body
    }

    @Test
    fun `CacheKey가 명시되어있지 않으면 캐시가 동작하지 않는다`() {
        val result1 = client.request<String>(path = "/test/no-cache-key")
        Thread.sleep(100)
        val result2 = client.request<String>(path = "/test/no-cache-key")
        result1.statusCode equalsTo result2.statusCode
        result1.body doesNotEqualTo result2.body
    }

    @Test
    fun `cacheGroupManager로 evict할 수 있다`() {
        val headers = mapOf(
            "content-type" to "application/json",
            "user-no" to "1"
        )

        val result1 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        Thread.sleep(1000)
        cacheGroupManager.evict("default", "userNo=1")
        val result2 = client.request<TestResponse>(path = "/test/get", headers = headers + mapOf("index" to "1"))
        result1.body doesNotEqualTo result2.body
    }

}

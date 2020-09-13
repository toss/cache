package im.toss

import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus

class TestApplicationTest: SpringWebMvcTest() {
    @Test
    fun test() {
        client.request<String>(HttpMethod.GET, "/health").run {
            statusCode equalsTo HttpStatus.OK
            body equalsTo "OK"
        }
    }
}
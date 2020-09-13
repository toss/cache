package im.toss

import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class SpringWebMvcTest {
    @Autowired lateinit var restTemplate: TestRestTemplate
    @LocalServerPort var port = 0

    val client: TestClient by lazy { TestClient(restTemplate, port) }
}

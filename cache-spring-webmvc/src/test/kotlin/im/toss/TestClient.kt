package im.toss

import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.test.web.client.exchange
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.RequestEntity
import org.springframework.http.ResponseEntity
import java.net.URI

//@Component
class TestClient(
    val restTemplate: TestRestTemplate,
    val port: Int
) {
    fun uri(path: String) = URI.create("http://localhost:$port$path")
}

inline fun <reified T : Any> TestClient.request(
    method: HttpMethod = HttpMethod.GET,
    path: String,
    headers: Map<String, String> = emptyMap()
): ResponseEntity<T> {
    val entity = RequestEntity<Any>(
        HttpHeaders().also {
            headers.entries.forEach { entry ->
                it.set(entry.key, entry.value)
            }
        },
        method,
        uri(path)
    )

    return restTemplate.exchange<T>(entity)
}

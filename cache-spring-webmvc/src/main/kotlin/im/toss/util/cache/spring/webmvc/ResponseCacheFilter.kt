package im.toss.util.cache.spring.webmvc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import im.toss.util.data.container.Pack
import im.toss.util.data.hash.base64
import im.toss.util.data.hash.sha1
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import java.time.ZonedDateTime
import javax.servlet.Filter
import javax.servlet.FilterChain
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.annotation.WebFilter
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

data class ResponseMetadata(
    val status: Int,
    val etag: String,
    val lastModified: ZonedDateTime,
    val headers: HttpHeaders
)

@Component
@ConditionalOnBean(annotation = [EnableCacheSupport::class])
@WebFilter(urlPatterns = ["/*"])
class ResponseCacheFilter: Filter {
    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val httpServletResponse = response as? HttpServletResponse ?: return chain.doFilter(request, response)
        val httpServletRequest = request as? HttpServletRequest ?: return chain.doFilter(request, response)

        val captureResponse = CaptureHttpServletResponse(httpServletResponse)

        chain.doFilter(request, captureResponse)

        val command = CacheCommand.getCurrent()
        command.loadToCacheIfNeeds {
            if (captureResponse.status == 200 ) {
                val responseBody = captureResponse.capturedResponseBody
                captureResponse.addHeader("Content-Length", responseBody.size.toString())
                val etagValue = responseBody.sha1().base64()
                val metadata = captureResponse.run {
                    ResponseMetadata(
                        status = status,
                        etag = etagValue,
                        lastModified = ZonedDateTime.now(),
                        headers = capturedHeaders
                    )
                }
                Pack.Writer()
                    .add("res/metadata", objectMapper.writeValueAsBytes(metadata))
                    .add("res/body", responseBody)
                    .finish()
            } else null
        }

        if (captureResponse.isCaptureMode) {
            val data = command.data
            if (data != null) {
                val metadata = data.getInputStream("res/metadata")!!.let {
                    objectMapper.readValue<ResponseMetadata>(it)
                }
                val responseBody = data.getInputStream("res/body")!!

                httpServletResponse.run {
                    if (httpServletRequest.ifNoneMatch(metadata.etag)) {
                        status = 304 // Not Modified
                    } else {
                        status = metadata.status
                        metadata.headers.forEach { entry ->
                            entry.value.forEach { addHeader(entry.key, it) }
                        }
                        addHeader("ETag", metadata.etag)
                        responseBody.copyTo(outputStream)
                        flushBuffer()
                    }
                }
            } else {
                val responseBody = captureResponse.capturedResponseBody
                val etagValue = responseBody.sha1().base64()
                httpServletResponse.run {
                    if (httpServletRequest.ifNoneMatch(etagValue)) {
                        status = 304 // Not Modified
                    } else {
                        setHeader("ETag", etagValue)
                        setContentLength(responseBody.size)
                        outputStream.write(responseBody)
                        flushBuffer()
                    }
                }
            }
        }
    }
}

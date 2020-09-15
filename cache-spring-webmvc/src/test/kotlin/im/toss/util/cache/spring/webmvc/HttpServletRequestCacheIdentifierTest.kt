package im.toss.util.cache.spring.webmvc

import im.toss.SpringWebMvcTest
import im.toss.test.equalsTo
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.servlet.HandlerMapping
import javax.servlet.ServletContext
import kotlin.reflect.jvm.javaMethod

class HttpServletRequestCacheIdentifierTest: SpringWebMvcTest() {
    @Autowired
    lateinit var servletContext: ServletContext

    @Autowired lateinit var cacheGroupManager: CacheGroupManager

    @Test
    fun parseDefinition() {
        CacheKey.getIdentifier(::get1.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(HttpServletRequestValue.RequestHeader("user-id", "x-user-id"))
        )

        CacheField.getIdentifier(::get1.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(
                HttpServletRequestValue.Method,
                HttpServletRequestValue.PathPattern,
                HttpServletRequestValue.RequestParam("idx1","index1"),
                HttpServletRequestValue.RequestParam("idx2", "index2"),
                HttpServletRequestValue.PathVariable("res", "resource")
            )
        )

        CacheKey.getIdentifier(::get2.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(HttpServletRequestValue.RequestHeader("user-id", "x-user-id"))
        )
        CacheField.getIdentifier(::get2.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(
                HttpServletRequestValue.Method,
                HttpServletRequestValue.PathPattern,
                HttpServletRequestValue.RequestParam("idx1", "index1"),
                HttpServletRequestValue.RequestParam("idx2", "index2"),
                HttpServletRequestValue.PathVariable("res", "resource")
            )
        )

        CacheKey.getIdentifier(::get3.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(HttpServletRequestValue.RequestHeader("user-id", "x-user-id"))
        )

        CacheField.getIdentifier(::get3.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(
                HttpServletRequestValue.Method,
                HttpServletRequestValue.PathPattern,
                HttpServletRequestValue.RequestParam("idx1", "index1"),
                HttpServletRequestValue.RequestParam("idx2", "index2"),
                HttpServletRequestValue.PathVariable("res", "resource")
            )
        )

        CacheKey.getIdentifier(::value1.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(HttpServletRequestValue.Value("key", "globalKey"))
        )

        CacheField.getIdentifier(::value1.javaMethod!!) equalsTo HttpServletRequestCacheIdentifier(
            sortedSetOf(HttpServletRequestValue.Method, HttpServletRequestValue.PathPattern, HttpServletRequestValue.Value("field", "globalField"))
        )
    }

    fun get1(
        @CacheKey("user-id") @RequestHeader("x-user-id") userId: Long,
        @CacheField("idx1") @RequestParam("index1") index1: Long,
        @CacheField("idx2") @RequestParam("index2") index2: Long,
        @CacheField("res") @PathVariable("resource") resource: Long
    ): String {
        return "OK"
    }

    @CacheKey.RequestHeader("user-id=x-user-id")
    @CacheField.PathVariable("res=resource")
    @CacheField.RequestParam("idx1=index1", "idx2=index2")
    fun get2(): String {
        return "OK"
    }

    @CacheKey.RequestHeader("user-id=x-user-id")
    @CacheField.RequestParam("idx1=index1", "idx2=index2")
    @CacheField.PathVariable("res=resource")
    fun get3(): String {
        return "OK"
    }

    @CacheKey.Value("key=globalKey")
    @CacheField.Value("field=globalField")
    fun value1(): String {
        return "OK"
    }

    @Test
    fun cacheIdentifierString() {
        val request = MockMvcRequestBuilders.get("/api/res/{resouce}", "hello")
            .header("x-user-id", "hello")
            .queryParam("index1", "first")
            .queryParam("index2", "second")
            .requestAttr(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE, "/api/res/{resource}")
            .requestAttr(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE, mapOf("resource" to "hello"))
            .buildRequest(servletContext)

        CacheKey.getIdentifier(::cacheKey1.javaMethod!!).get(request) equalsTo "user-id=hello"
        CacheField.getIdentifier(::cacheKey1.javaMethod!!).get(request) equalsTo "@M=GET,@P=/api/res/{resource},idx1=first,idx2=second,res=hello"
    }

    @CacheKey.RequestHeader("user-id=x-user-id")
    @CacheField.PathVariable("res=resource")
    @CacheField.RequestParam("idx1=index1", "idx2=index2")
    fun cacheKey1() {}

    @Test
    fun cacheIdentifierString2() {
        val request = MockMvcRequestBuilders.get("/api/res/{resouce}", "hello")
            .header("x-user-id", "hello")
            .queryParam("index1", "first")
            .queryParam("index2", "second")
            .requestAttr(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE, "/api/res/{resource}")
            .requestAttr(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE, mapOf("resource" to "hello"))
            .buildRequest(servletContext)

        CacheKey.getIdentifier(::cacheKey2.javaMethod!!).get(request) equalsTo "x-user-id=hello"
        CacheField.getIdentifier(::cacheKey2.javaMethod!!).get(request) equalsTo "@M=GET,@P=/api/res/{resource},index1=first,index2=second,resource=hello"
    }
    
    @CacheKey.RequestHeader("x-user-id")
    @CacheField.PathVariable("resource")
    @CacheField.RequestParam("index1", "index2")
    fun cacheKey2() {}

    @Test
    fun cacheKeyTest() {
        cacheGroupManager.cacheKey(mapOf("x-user-id" to "hello")) equalsTo "x-user-id=hello"
    }

    @TestFactory
    fun cacheIdentifiertest(): List<DynamicTest> {
        val request = MockMvcRequestBuilders.get("/api/res/{resouce}", "hello")
            .header("x-user-id", "hello")
            .queryParam("index1", "first")
            .queryParam("index2", "second")
            .requestAttr(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE, "/api/res/{resource}")
            .requestAttr(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE, mapOf("resource" to "hello"))
            .buildRequest(servletContext)


        return listOf(
            HttpServletRequestCacheIdentifier().header("user", "x-user-id")
                    to "user=hello",

            HttpServletRequestCacheIdentifier()
                .requestParam("idx1", "index1")
                .requestParam("idx2", "index2")
                    to "idx1=first,idx2=second",

            HttpServletRequestCacheIdentifier()
                .requestParam("idx1", "index1")
                    to "idx1=first",

            HttpServletRequestCacheIdentifier()
                .pathVariable("res", "resource")
                    to "res=hello",

            HttpServletRequestCacheIdentifier()
                .value("value", "constantValue")
                    to "value=constantValue"
        ).map { (builder, expected) ->
            dynamicTest("$builder -> $expected") {
                builder.get(request) equalsTo expected
            }
        }
    }
}


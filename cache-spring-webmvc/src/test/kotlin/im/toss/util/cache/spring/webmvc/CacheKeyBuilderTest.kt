package im.toss.util.cache.spring.webmvc

import im.toss.SpringWebMvcTest
import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestParam
import kotlin.reflect.jvm.javaMethod

class CacheKeyBuilderTest: SpringWebMvcTest() {
    @Autowired
    lateinit var testController: TestController

    @Test
    fun parseDefinition() {
        CacheKeyBuilder.from(::get1.javaMethod!!) equalsTo CacheKeyBuilder(
            keys = listOf(RequestValue.RequestHeader("x-user-id")),
            fields = listOf(RequestValue.PathVariable("resource"), RequestValue.RequestParam("index1"), RequestValue.RequestParam("index2"))
        )

        CacheKeyBuilder.from(::get2.javaMethod!!) equalsTo CacheKeyBuilder(
            keys = listOf(RequestValue.RequestHeader("x-user-id")),
            fields = listOf(RequestValue.PathVariable("resource"), RequestValue.RequestParam("index1"), RequestValue.RequestParam("index2"))
        )

        CacheKeyBuilder.from(::get3.javaMethod!!) equalsTo CacheKeyBuilder(
            keys = listOf(RequestValue.RequestHeader("x-user-id")),
            fields = listOf(RequestValue.PathVariable("resource"), RequestValue.RequestParam("index1"), RequestValue.RequestParam("index2"))
        )

        CacheKeyBuilder.from(::value1.javaMethod!!) equalsTo CacheKeyBuilder(
            keys = listOf(RequestValue.Value("globalKey")),
            fields = listOf(RequestValue.Value("globalField"))
        )
    }

    fun get1(
        @CacheKey @RequestHeader("x-user-id") userId: Long,
        @CacheField @RequestParam("index1") index1: Long,
        @CacheField @RequestParam("index2") index2: Long,
        @CacheField @PathVariable("resource") resource: Long
    ): String {
        return "OK"
    }

    @CacheKey.RequestHeader("x-user-id")
    @CacheField.PathVariable("resource")
    @CacheField.RequestParam("index1", "index2")
    fun get2(): String {
        return "OK"
    }

    @CacheKey.RequestHeader("x-user-id")
    @CacheField.RequestParam("index1", "index2")
    @CacheField.PathVariable("resource")
    fun get3(): String {
        return "OK"
    }

    @CacheKey.Value("globalKey")
    @CacheField.Value("globalField")
    fun value1(): String {
        return "OK"
    }
}


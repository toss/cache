package im.toss.util.cache.spring.webmvc

import org.springframework.web.servlet.HandlerMapping
import java.lang.reflect.Method
import java.lang.reflect.Parameter
import javax.servlet.http.HttpServletRequest

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class CacheKey {
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class Value(val value: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestHeader(vararg val name: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class PathVariable(vararg val name: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestParam(vararg val name: String)
}

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class CacheField {
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class Value(val value: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestHeader(vararg val name: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class PathVariable(vararg val name: String)

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestParam(vararg val name: String)
}


data class CacheKeyBuilder(
    val keys: List<RequestValue>,
    val fields: List<RequestValue>
) {
    fun buildKey(request: HttpServletRequest): String = keys.joinToString("|") { it.get(request) ?: "" }
    fun buildField(request: HttpServletRequest): String = fields.joinToString("|", "${request.method.toUpperCase()} ${request.getBestMatchingPattern() ?: "?"}|") { it.get(request) ?: "" }

    companion object {
        fun from(method: Method): CacheKeyBuilder =
            CacheKeyBuilder(
                getCacheKeyValues(method),
                getCacheFieldValues(method)
            )
    }
}

private fun getCacheKeyValues(method: Method): List<RequestValue> {
    val values = method.getAnnotation(CacheKey.Value::class.java)?.value?.let { listOf(RequestValue.Value(it)) } ?: emptyList()
    val headers = method.getAnnotation(CacheKey.RequestHeader::class.java)?.name?.map { RequestValue.RequestHeader(it) } ?: emptyList()
    val params = method.getAnnotation(CacheKey.RequestParam::class.java)?.name?.map { RequestValue.RequestParam(it) } ?: emptyList()
    val pathVariables = method.getAnnotation(CacheKey.PathVariable::class.java)?.name?.map { RequestValue.PathVariable(it) } ?: emptyList()
    fun Parameter.isCacheKey(): Boolean = getAnnotation(CacheKey::class.java) != null
    val paramValues = method.parameters.filter { it.isCacheKey() }.mapNotNull { RequestValue.from(it) }
    return (values + headers + params + pathVariables + paramValues).toSortedSet(compareBy({ it.order }, { it.name })).toList()
}

private fun getCacheFieldValues(method: Method): List<RequestValue> {
    val values = method.getAnnotation(CacheField.Value::class.java)?.value?.let { listOf(RequestValue.Value(it)) } ?: emptyList()
    val headers = method.getAnnotation(CacheField.RequestHeader::class.java)?.name?.map { RequestValue.RequestHeader(it) } ?: emptyList()
    val params = method.getAnnotation(CacheField.RequestParam::class.java)?.name?.map { RequestValue.RequestParam(it) } ?: emptyList()
    val pathVariables = method.getAnnotation(CacheField.PathVariable::class.java)?.name?.map { RequestValue.PathVariable(it) } ?: emptyList()
    fun Parameter.isCacheField(): Boolean = getAnnotation(CacheField::class.java) != null
    val paramValues = method.parameters.filter { it.isCacheField() }.mapNotNull { RequestValue.from(it) }
    return (values + headers + params + pathVariables + paramValues).toSortedSet(compareBy({ it.order }, { it.name })).toList()
}

interface RequestValue {
    val order: Int
    val name: String
    fun get(request: HttpServletRequest): String?

    companion object {
        fun from(param: Parameter): RequestValue? {
            val requestHeader = param.getAnnotation(org.springframework.web.bind.annotation.RequestHeader::class.java)
            if (requestHeader != null) {
                return RequestHeader(requestHeader.value)
            }

            val requestParam = param.getAnnotation(org.springframework.web.bind.annotation.RequestParam::class.java)
            if (requestParam != null) {
                return RequestParam(requestParam.value)
            }

            val pathVariable = param.getAnnotation(org.springframework.web.bind.annotation.PathVariable::class.java)
            if (pathVariable != null) {
                return PathVariable(pathVariable.value)
            }
            return null
        }
    }

    data class Value(
        override val name: String
    ): RequestValue {
        override val order: Int get() = 0
        override fun get(request: HttpServletRequest): String? {
            return name
        }
    }

    data class RequestHeader(
        override val name: String
    ): RequestValue {
        override val order: Int get() = 1
        override fun get(request: HttpServletRequest): String? {
            return request.getHeader(name)
        }
    }

    data class PathVariable(
        override val name: String
    ): RequestValue {
        override val order: Int get() = 2
        override fun get(request: HttpServletRequest): String? {
            @Suppress("UNCHECKED_CAST")
            val pathVariables = request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE) as Map<String, String>
            return pathVariables[name]
        }
    }

    data class RequestParam(
        override val name: String
    ): RequestValue {
        override val order: Int get() = 3
        override fun get(request: HttpServletRequest): String? {
            return request.getParameter(name)
        }
    }
}



package im.toss.util.cache.spring.webmvc

import org.springframework.web.servlet.HandlerMapping
import java.lang.reflect.Method
import java.lang.reflect.Parameter
import java.util.*
import javax.servlet.http.HttpServletRequest
import kotlin.text.StringBuilder

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class CacheKey(val keyName: String) {
    /**
     * Constant value
     * Format: {name}={value}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class Value(vararg val format: String)

    /**
     * Http Request Header Value
     * Format: {name}={headerName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestHeader(vararg val format: String)


    /**
     * Path Variable Value
     * Format: {name}={variableName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class PathVariable(vararg val format: String)


    /**
     * Request Param Value
     * Format: {name}={paramName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestParam(vararg val format: String)

    companion object {
        fun getIdentifier(method: Method): HttpServletRequestCacheIdentifier = HttpServletRequestCacheIdentifier().run {
            method.getAnnotation(Value::class.java)?.run { format.forEach { it.parseFormat { value(first, second) } } }
            method.getAnnotation(RequestHeader::class.java)?.run { format.forEach { it.parseFormat { header(first, second) } } }
            method.getAnnotation(RequestParam::class.java)?.run { format.forEach { it.parseFormat { requestParam(first, second) } } }
            method.getAnnotation(PathVariable::class.java)?.run { format.forEach { it.parseFormat { pathVariable(first, second) } } }
            method.parameters.forEach { it.getAnnotation(CacheKey::class.java)?.run { HttpServletRequestValue.from(keyName, it) }?.run { add(this) } }
            this
        }
    }
}

@Target(AnnotationTarget.VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
annotation class CacheField(val fieldName: String) {
    /**
     * Constant value
     * Format: {name}={value}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class Value(vararg val format: String)

    /**
     * Http Request Header Value
     * Format: {name}={headerName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestHeader(vararg val format: String)


    /**
     * Path Variable Value
     * Format: {name}={variableName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class PathVariable(vararg val format: String)


    /**
     * Request Param Value
     * Format: {name}={paramName}
     */
    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class RequestParam(vararg val format: String)

    companion object {
        fun getIdentifier(method: Method): HttpServletRequestCacheIdentifier = HttpServletRequestCacheIdentifier(TreeSet()).run {
            add(HttpServletRequestValue.Method)
            add(HttpServletRequestValue.PathPattern)
            method.getAnnotation(Value::class.java)?.run { format.forEach { it.parseFormat { value(first, second) } } }
            method.getAnnotation(RequestHeader::class.java)?.run { format.forEach { it.parseFormat { header(first, second) } } }
            method.getAnnotation(RequestParam::class.java)?.run { format.forEach { it.parseFormat { requestParam(first, second) } } }
            method.getAnnotation(PathVariable::class.java)?.run { format.forEach { it.parseFormat { pathVariable(first, second) } } }
            method.parameters.forEach { it.getAnnotation(CacheField::class.java)?.run { HttpServletRequestValue.from(fieldName, it) }?.run { add(this) } }
            this
        }
    }
}

private fun String.parseFormat(block: Pair<String, String>.() -> Unit) {
    val firstIndex = indexOf('=')
    return if (firstIndex < 0) {
        (this to this).block()
    } else {
        (substring(0, firstIndex) to substring(firstIndex + 1)).block()
    }
}

data class HttpServletRequestCacheIdentifier(val items: TreeSet<HttpServletRequestValue> = TreeSet()) {
    fun get(request: HttpServletRequest): String {
        val sb = StringBuilder()
        for (item in items) {
            if (sb.isNotEmpty()) sb.append(',')
            sb.append(item.name)
            sb.append('=')
            sb.append(item.get(request))
        }
        return sb.toString()
    }

    fun header(name: String, headerName: String): HttpServletRequestCacheIdentifier {
        items += HttpServletRequestValue.RequestHeader(name, headerName)
        return this
    }

    fun pathVariable(name: String, variableName: String): HttpServletRequestCacheIdentifier {
        items += HttpServletRequestValue.PathVariable(name, variableName)
        return this
    }

    fun requestParam(name: String, paramName: String): HttpServletRequestCacheIdentifier {
        items += HttpServletRequestValue.RequestParam(name, paramName)
        return this
    }

    fun value(name: String, value: String): HttpServletRequestCacheIdentifier {
        items += HttpServletRequestValue.Value(name, value)
        return this
    }

    fun add(value: HttpServletRequestValue) {
        items += value
    }

    fun isEmpty(): Boolean = items.isEmpty()

    override fun toString(): String {
        return "CacheIdentifier($items)"
    }
}

interface HttpServletRequestValue: Comparable<HttpServletRequestValue> {
    val name: String
    fun get(request: HttpServletRequest): String

    override fun compareTo(other: HttpServletRequestValue): Int {
        return name.compareTo(other.name)
    }

    companion object {
        fun from(name: String, param: Parameter): HttpServletRequestValue? {
            val requestHeader = param.getAnnotation(org.springframework.web.bind.annotation.RequestHeader::class.java)
            if (requestHeader != null) {
                return RequestHeader(name, requestHeader.value)
            }

            val requestParam = param.getAnnotation(org.springframework.web.bind.annotation.RequestParam::class.java)
            if (requestParam != null) {
                return RequestParam(name, requestParam.value)
            }

            val pathVariable = param.getAnnotation(org.springframework.web.bind.annotation.PathVariable::class.java)
            if (pathVariable != null) {
                return PathVariable(name, pathVariable.value)
            }
            return null
        }
    }

    object Method: HttpServletRequestValue {
        override val name: String get() = "@M"
        override fun get(request: HttpServletRequest): String {
            return request.method.toUpperCase()
        }
    }

    object PathPattern: HttpServletRequestValue {
        override val name: String get() = "@P"
        override fun get(request: HttpServletRequest): String {
            return request.getBestMatchingPattern() ?: request.pathInfo
        }
    }

    data class Value(
        override val name: String,
        val value: String
    ): HttpServletRequestValue {
        override fun get(request: HttpServletRequest): String {
            return value
        }
    }

    data class RequestHeader(
        override val name: String,
        val headerName: String
    ): HttpServletRequestValue {
        override fun get(request: HttpServletRequest): String {
            return request.getHeader(headerName) ?: ""
        }
    }

    data class PathVariable(
        override val name: String,
        val variableName: String
    ): HttpServletRequestValue {
        override fun get(request: HttpServletRequest): String {
            @Suppress("UNCHECKED_CAST")
            val pathVariables = request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE) as? Map<String, String>
                ?: return ""
            return pathVariables[variableName] ?: ""
        }
    }

    data class RequestParam(
        override val name: String,
        val paramName: String
    ): HttpServletRequestValue {
        override fun get(request: HttpServletRequest): String {
            return request.getParameter(paramName) ?: ""
        }
    }
}



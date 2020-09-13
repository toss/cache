package im.toss.util.cache.spring.webmvc

import org.springframework.web.servlet.HandlerMapping
import javax.servlet.http.HttpServletRequest

data class CacheControl(
    val isNoStore: Boolean,
    val isNeedsInvalidate: Boolean
)

fun HttpServletRequest.parseHeadersForCache(): CacheControl {
    val cacheControl = (getHeader("cache-control") ?: "").split(Regex(""",\s*""")).toSet()
    val noStore = "no-store" in cacheControl
    val mustRevalidate = "mustRevalidate" in cacheControl
    val noCache = "no-cache" in cacheControl
    return CacheControl(
        isNoStore = noStore,
        isNeedsInvalidate = mustRevalidate || noCache || noStore
    )
}

fun HttpServletRequest.getBestMatchingPattern(): String? {
    return getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE) as? String
}

data class IfMatch(
    val isValid: Boolean = false,
    val isWeak: Boolean = false,
    val value: String = ""
) {
    companion object {
        private val pattern = Regex("""^(?<weak>W/)?\s*"?(?<value>[^"]+)"?\s*$""")
        fun parse(headerValue: String): IfMatch {
            val matched = pattern.matchEntire(headerValue) ?: return IfMatch()
            val valueGroup = matched.groups["value"] ?: return IfMatch()
            return IfMatch(
                isValid = true,
                isWeak = matched.groups["weak"] != null,
                value = valueGroup.value
            )
        }
    }
}

fun HttpServletRequest.ifNoneMatch(etag: String): Boolean {
    val data = IfMatch.parse(getHeader("If-None-Match") ?: "")
    return data.isValid && data.value == etag
}
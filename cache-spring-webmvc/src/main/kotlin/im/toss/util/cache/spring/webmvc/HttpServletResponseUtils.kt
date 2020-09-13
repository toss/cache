package im.toss.util.cache.spring.webmvc

import javax.servlet.http.HttpServletResponse

fun HttpServletResponse.getHeaders(): Map<String, Collection<String>> {
    return headerNames.associateWith { name -> getHeaders(name) }
}
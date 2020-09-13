package im.toss.util.cache.spring

import im.toss.util.cache.CacheOptions

data class CacheGroupDefinition(
    val groupId: String,
    val resourceId: String? = null,
    val options: CacheOptions? = null
)
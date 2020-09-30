package im.toss.util.cache

import com.fasterxml.jackson.annotation.JsonProperty

data class CacheNamespace(
    @field:JsonProperty("resource-id")
    val resourceId: String? = null,

    @field:JsonProperty("serializer-id")
    val serializerId: String? = null,

    @field:JsonProperty("options")
    val options: CacheOptions = cacheOptions()
)

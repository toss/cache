package im.toss.util.cache

data class CacheNamespace(
    val resourceId: String = "default",
    val serializerId: String = "default",
    val options: CacheOptions = cacheOptions()
)

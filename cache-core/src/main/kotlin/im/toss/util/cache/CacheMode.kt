package im.toss.util.cache

import com.fasterxml.jackson.annotation.JsonProperty

enum class CacheMode {
    @JsonProperty("normal")
    NORMAL,
    @JsonProperty("eviction-only")
    EVICTION_ONLY
}
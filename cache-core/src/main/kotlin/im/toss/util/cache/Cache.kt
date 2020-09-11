package im.toss.util.cache

interface Cache {
    data class KeyFunction(
        val function: (name: String, key: Any) -> String
    )
}

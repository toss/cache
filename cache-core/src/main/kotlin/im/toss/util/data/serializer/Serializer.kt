package im.toss.util.data.serializer

interface Serializer {
    val name: String
    fun <T> serialize(o: T): ByteArray
    fun <T> deserialize(value: ByteArray): T
}
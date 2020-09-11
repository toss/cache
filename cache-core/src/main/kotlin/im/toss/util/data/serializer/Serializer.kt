package im.toss.util.data.serializer

interface Serializer {
    fun <T> serialize(o: T): ByteArray
    fun <T> deserialize(value: ByteArray): T
}
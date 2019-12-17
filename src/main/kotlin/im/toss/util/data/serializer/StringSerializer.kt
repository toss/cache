package im.toss.util.data.serializer

object StringSerializer : Serializer {
    override fun <T> deserialize(value: ByteArray): T {
        @Suppress("UNCHECKED_CAST")
        return String(value) as T
    }

    override fun <T> serialize(o: T): ByteArray {
        return o.toString().toByteArray()
    }
}

package im.toss.util.data.serializer

object ByteArraySerializer: Serializer {
    override fun <T> deserialize(value: ByteArray): T {
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    override fun <T> serialize(o: T): ByteArray {
        return if (o is ByteArray) {
            o
        } else {
            throw Error("Not support serialize")
        }
    }
}

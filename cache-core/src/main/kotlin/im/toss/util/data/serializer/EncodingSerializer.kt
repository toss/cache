package im.toss.util.data.serializer

import im.toss.util.data.encoding.Codec
import java.io.IOException

class EncodingSerializer(
    private val serializer: Serializer,
    private vararg val codecs: Codec
) : Serializer {
    override val name: String
        get() = "encoding(${serializer.name}, [${codecs.joinToString(",") { it.name }}])"

    override fun <T> serialize(o: T): ByteArray {
        try {
            var data = serializer.serialize(o)
            for (codec in codecs) {
                data = codec.encode(data)
            }
            return data
        } catch (e: IOException) {
            throw RuntimeException("Exception occured in encoding", e)
        }
    }

    override fun <T> deserialize(value: ByteArray): T {
        try {
            var data = value
            for (codec in codecs) {
                data = codec.decode(data, 0, data.size)
            }
            return serializer.deserialize(data)
        } catch (e: IOException) {
            throw RuntimeException("Exception occured in decoding", e)
        }
    }
}
package im.toss.util.serializer

import im.toss.test.equalsTo
import im.toss.util.data.encoding.GZipCompressionCodec
import im.toss.util.data.serializer.EncodingSerializer
import im.toss.util.data.serializer.KryoSerializer
import org.junit.jupiter.api.Test

class SerializerTest {
    @Test
    fun serializerName() {
        EncodingSerializer(
            KryoSerializer(),
            GZipCompressionCodec()
        ).name equalsTo "encoding(kryo-4, [gzip])"
    }
}
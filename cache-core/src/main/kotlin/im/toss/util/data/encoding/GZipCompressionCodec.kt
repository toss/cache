package im.toss.util.data.encoding

import org.apache.commons.io.IOUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class GZipCompressionCodec : Codec {
    override fun encode(bytes: ByteArray): ByteArray {
        val output = ByteArrayOutputStream()
        GZIPOutputStream(output).use { out -> out.write(bytes) }
        return output.toByteArray()
    }

    override fun decode(bytes: ByteArray, offset: Int, length: Int): ByteArray {
        GZIPInputStream(ByteArrayInputStream(bytes, offset, length)).use { inputStream -> return IOUtils.toByteArray(inputStream) }
    }
}
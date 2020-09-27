package im.toss.util.data.bits

import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

class BitsUtilsTest {
    @Test
    fun `read int4L`() {
        val bytes = ByteArray(8).apply {
            ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN).putInt(0x12345678).putInt(0x76543210)
        }
        val inputStream = ByteArrayInputStream(bytes)
        inputStream.int4L() equalsTo 0x12345678
        inputStream.int4L() equalsTo 0x76543210
        bytes.int4L(0) equalsTo 0x12345678
        bytes.int4L(4) equalsTo 0x76543210
    }

    @Test
    fun `write int4L`() {
        val bytes = ByteArray(8)
        bytes.int4L(0, 0x12345678)
        bytes.int4L(4, 0x76543210)
        bytes equalsTo byteArrayOf(0x78.toByte(), 0x56.toByte(), 0x34.toByte(), 0x12.toByte(), 0x10.toByte(), 0x32.toByte(), 0x54.toByte(), 0x76.toByte())
    }

    @Test
    fun `read short2L`() {
        val bytes = ByteArray(4).apply {
            ByteBuffer.wrap(this).order(ByteOrder.LITTLE_ENDIAN).putShort(0x1234).putShort(0x7654)
        }

        bytes.short2L(0) equalsTo 0x1234.toShort()
        bytes.short2L(2) equalsTo 0x7654.toShort()
    }

    @Test
    fun `write short2L`() {
        val bytes = ByteArray(4)
        bytes.short2L(0, 0x1234)
        bytes.short2L(2, 0x7654)
        bytes equalsTo byteArrayOf(0x34.toByte(), 0x12.toByte(), 0x54.toByte(), 0x76.toByte())
    }
}
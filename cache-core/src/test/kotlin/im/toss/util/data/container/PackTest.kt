package im.toss.util.data.container

import com.fasterxml.jackson.databind.ObjectMapper
import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

class PackTest {
    @Test
    fun entryWriteAndReadTest() {
        val entry1 = Pack.Entry("hello", 100, 256, true)
        val entry2 = Pack.Entry("world", 100, 256, false)
        val entry3 = Pack.Entry("test", 65535, 256, true)

        val bytes = ByteArrayOutputStream().also { output ->
            entry1.write(output)
            entry2.write(output)
            entry3.write(output)
            output.close()
        }.toByteArray()

        bytes.size equalsTo entry1.sizeOf() + entry2.sizeOf() + entry3.sizeOf()

        println(bytes.size)

        ByteArrayInputStream(bytes).also { input ->
            Pack.Entry.read(input, 100).also { it equalsTo entry1 }
            Pack.Entry.read(input, 100).also { it equalsTo entry2 }
            Pack.Entry.read(input, 65535).also { it equalsTo entry3 }
        }
    }

    @Test
    fun entryFormatTest() {
        val name = "abc"
        val bytes = ByteArrayOutputStream().also {
            Pack.Entry(name, 100, 1024, true).write(it)
        }.toByteArray()
        bytes[0] equalsTo name.toByteArray().size.toByte()
        String(bytes, 1, 3) equalsTo "abc"
        ByteBuffer.wrap(bytes, 4, 4).order(ByteOrder.LITTLE_ENDIAN).int equalsTo -1024
        bytes equalsTo Base64.getDecoder().decode("A2FiYwD8//8=")
    }

    @Test
    fun packFormatTestNoCompress() {
        val bytes = Pack.Writer()
            .add("first", "hello".toByteArray())
            .add("second", "world".toByteArray())
            .finish()

        bytes[0] equalsTo 0x54.toByte()
        bytes[1] equalsTo 0x5A.toByte()
        String(bytes, 2, 5) equalsTo "hello"
        String(bytes, 7, 5) equalsTo "world"
        bytes equalsTo Base64.getDecoder().decode("VFpoZWxsb3dvcmxkY2JgYGBNyywqLmEFstiKU5Pz81JATADp////IzA=")
        String(bytes).also { println("bytes -> $it") }
    }

    @Test
    fun packFormatTestCompress() {
        val bytes = Pack.Writer()
            .add("first", "000000000000000000000000000000000000".toByteArray())
            .add("second", "111111111111111111111111111111111111".toByteArray())
            .finish()

        bytes[0] equalsTo 0x54.toByte()
        bytes[1] equalsTo 0x5A.toByte()

        bytes equalsTo Base64.getDecoder().decode("VFozMCAMADM0JAwAAgAAAAVmaXJzdPv///8Gc2Vjb25k+////xkAAAC4QQ==")
    }

    @Test
    fun packFormatReadTestNoCompress() {
        val bytes = Base64.getDecoder().decode("VFpoZWxsb3dvcmxkY2JgYGBNyywqLmEFstiKU5Pz81JATADp////IzA=")
        val pack = Pack(bytes)
        pack.getInputStream("first")!!.readBytes().toString(Charsets.UTF_8) equalsTo "hello"
        pack.getInputStream("second")!!.readBytes().toString(Charsets.UTF_8) equalsTo "world"
    }

    @Test
    fun writerTest() {
       val body = "{\"success\":[1351234, 123512354, 1234123, 123414]}".toByteArray()
        val headers = ObjectMapper().writeValueAsBytes(mapOf("Content-Type" to listOf("application/json"), "Content-Size" to listOf(body.size.toString())))

        val output = Pack.Writer()
            .add("headers", headers)
            .add("body", body)
            .finish()

        println("origin.size = ${headers.size + body.size}")
        println("headers.size = ${headers.size}")
        println("output.size = ${output.size}")
        val pack = Pack(output)
        pack.getInputStream("headers")!!.readBytes() equalsTo headers
        pack.getInputStream("body")!!.readBytes() equalsTo body
    }
}
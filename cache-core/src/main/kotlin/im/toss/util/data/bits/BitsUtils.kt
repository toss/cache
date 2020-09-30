package im.toss.util.data.bits

import java.io.InputStream
import java.io.OutputStream

fun OutputStream.byte1(value: Int): OutputStream = also { write(value) }

fun OutputStream.int4L(value: Int): OutputStream = also {
    write(int0(value))
    write(int1(value))
    write(int2(value))
    write(int3(value))
}

fun OutputStream.short2L(value: Short): OutputStream = also {
    write(int0(value.toInt()))
    write(int1(value.toInt()))
}

fun OutputStream.bytes(bytes: ByteArray) = also { write(bytes) }
fun OutputStream.bytes(bytes: ByteArray, offset: Int, count: Int) = also { write(bytes, offset, count) }

fun InputStream.int4L(): Int = buildIntL(read(), read(), read(), read())
fun InputStream.byte1(): Int = read()
fun InputStream.bytes(length: Int): ByteArray = ByteArray(length).also { read(it) }
fun InputStream.utf8(length: Int): String = bytes(length).toString(Charsets.UTF_8)

fun ByteArray.int4L(offset: Int = 0): Int = buildIntL(this[offset].toInt(), this[offset + 1].toInt(), this[offset + 2].toInt(), this[offset + 3].toInt())
fun ByteArray.int4L(offset: Int, value: Int): ByteArray = apply {
    this[offset] = int0(value).toByte()
    this[offset + 1] = int1(value).toByte()
    this[offset + 2] = int2(value).toByte()
    this[offset + 3] = int3(value).toByte()
}

fun ByteArray.short2L(offset: Int = 0): Short = buildShortL(this[offset].toInt(), this[offset + 1].toInt())
fun ByteArray.short2L(offset: Int = 0, value: Short): ByteArray = apply {
    this[offset] = int0(value.toInt()).toByte()
    this[offset + 1] = int1(value.toInt()).toByte()
}

private fun buildIntL(v0: Int, v1: Int, v2: Int, v3: Int): Int = v3 shl 24 or (v2 and 255 shl 16) or (v1 and 255 shl 8) or (v0 and 255)
private fun buildShortL(v0: Int, v1: Int): Short = ((v1 and 255 shl 8) or (v0 and 255)).toShort()
private fun int3(v: Int): Int = v shr 24
private fun int2(v: Int): Int = v shr 16
private fun int1(v: Int): Int = v shr 8
private fun int0(v: Int): Int = v


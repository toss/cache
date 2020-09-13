package im.toss.util.data.container

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.*
import kotlin.math.absoluteValue

class Pack(
    private val bytes: ByteArray
) {
    companion object {
        fun Int.toBytes(): ByteArray = ByteArray(4).also { ByteBuffer.wrap(it).order(ByteOrder.LITTLE_ENDIAN).putInt(this) }
        fun ByteArray.getInt(offset:Int): Int = ByteBuffer.wrap(this, offset, 4).order(ByteOrder.LITTLE_ENDIAN).int
        fun ByteArray.getShort(offset:Int): Short = ByteBuffer.wrap(this, offset, 2).order(ByteOrder.LITTLE_ENDIAN).short
        fun InputStream.readInt(): Int = ByteArray(4).also { read(it) }.getInt(0)
    }

    fun validate(): Pack {
        check(bytes[0] == 0x54.toByte() && bytes[1] == 0x5A.toByte()) { "invalid magic" }
        val crc = bytes.getShort(bytes.size - 2)
        val crc2 = CRC32().also { it.update(bytes, 0, bytes.size - 2) }.value.toShort()
        check(crc == crc2) { "invalid crc" }
        return this
    }

    val entries: Map<String, Entry> by lazy {
        validate()
        val lengthPart = bytes.getInt(bytes.size - 6)
        val length = lengthPart.absoluteValue
        val offset = bytes.size - 6 - length
        val compressed = lengthPart < 0
        val input = getInputStream(offset, length, compressed)
        val entriesCount = input.readInt()
        var entryOffset = 2
        (1..entriesCount).map {
            Entry.read(input, entryOffset).also { entryOffset += it.length }
        }.associateBy { it.name }
    }

    fun getInputStream(name: String): InputStream? {
        val entry = entries[name] ?: return null
        return getInputStream(entry.offset, entry.length, entry.compressed)
    }

    private fun getInputStream(offset: Int, length: Int, compressed: Boolean): InputStream {
        val inputStream = ByteArrayInputStream(bytes, offset, length)
        return if (compressed) {
            InflaterInputStream(inputStream, Inflater(true))
        } else {
            inputStream
        }
    }

    class Writer {
        companion object {
            private val ZERO4 = ByteArray(4) { 0 }

            fun compress(content:ByteArray): ByteArray {
                return ByteArrayOutputStream().also { output ->
                    DeflaterOutputStream(output, Deflater(Deflater.DEFAULT_COMPRESSION, true)).also { it.write(content) }.close()
                }.toByteArray()
            }
        }
        private val output = ByteArrayOutputStream().also {
            it.write(0x54); it.write(0x5A) // MAGIC, 2 byte
        }
        private val names = mutableSetOf<String>()
        private val entries = mutableListOf<Entry>()

        fun add(name: String, content: ByteArray): Writer {
            check(name !in names) { "already exists entry: name=$name" }

            val offset = output.size()
            val compressedContent = compress(content)
            val compressed = content.size > compressedContent.size
            output.write(if (compressed) compressedContent else content)
            val length = output.size() - offset
            names += name
            entries += Entry(name, offset, length, compressed)
            return this
        }

        fun finish(): ByteArray {
            val entries = ByteArrayOutputStream().also { out ->
                out.write(entries.size.toBytes())
                entries.forEach { it.write(out) }
            }.toByteArray()
            val compressedEntries = compress(entries)
            val compressed = entries.size > compressedEntries.size
            val data = if(compressed) compressedEntries else entries

            output.write(data)
            output.write((if (compressed) -data.size else data.size).toBytes()) // 4 bytes
            output.write(ZERO4, 0, 2) // CRC 2 bytes

            val result = output.toByteArray()
            val crc = CRC32().also { it.update(result, 0, result.size - 2) }.value.toShort()
            ByteBuffer.wrap(result, result.size - 2, 2).order(ByteOrder.LITTLE_ENDIAN).putShort(crc)
            return result
        }
    }

    data class Entry(
        val name: String,
        val offset: Int,
        val length: Int,
        val compressed: Boolean
    ) {
        fun sizeOf(): Int = nameBytes.size + 5 // 1(name) + 4(length)
        private val nameBytes: ByteArray by lazy { name.toByteArray(Charsets.UTF_8) }

        fun write(output: OutputStream) {
            check(nameBytes.size < 256) { "entry name must be less than 256 bytes." }

            output.write(nameBytes.size)
            output.write(nameBytes, 0, nameBytes.size)
            output.write((if (compressed) -length else length).toBytes(), 0, 4)
        }

        companion object {
            fun read(input: InputStream, offset: Int): Entry {
                val nameLength = input.read()
                val name = ByteArray(nameLength).also { input.read(it) }.toString(Charsets.UTF_8)
                val intBytes = ByteArray(4)
                val buffer = ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN)
                val length = buffer.also { input.read(intBytes) }.getInt(0)
                val compressed = length < 0
                return Entry(name, offset, length.absoluteValue, compressed)
            }
        }
    }
}
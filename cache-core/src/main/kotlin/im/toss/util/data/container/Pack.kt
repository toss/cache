package im.toss.util.data.container

import im.toss.util.data.bits.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.zip.*
import kotlin.math.absoluteValue

class Pack(
    private val bytes: ByteArray
) {
    fun validate(): Pack {
        check(bytes.short2L() == 0x5A54.toShort()) { "invalid magic" }
        val crc = bytes.short2L(bytes.size - 2)
        val crc2 = CRC32().also { it.update(bytes, 0, bytes.size - 2) }.value.toShort()
        check(crc == crc2) { "invalid crc" }
        return this
    }

    val entries: Map<String, Entry> by lazy {
        validate()
        val length = bytes.int4L(bytes.size - 6)
        val input = getInputStream(offset = bytes.size - 6 - length.absoluteValue, length = length.absoluteValue, compressed = length < 0)
        var entryOffset = 2
        val entriesCount = input.int4L()
        (1..entriesCount).associate {
            Entry.read(input, entryOffset)
                .also { entryOffset += it.length }
                .let { it.name to it }
        }
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
            fun compress(content:ByteArray): ByteArray {
                return ByteArrayOutputStream().apply {
                    DeflaterOutputStream(this, Deflater(Deflater.DEFAULT_COMPRESSION, true)).apply { bytes(content) }.close()
                }.toByteArray()
            }
        }
        private val output = ByteArrayOutputStream().apply { short2L(0x5A54) } // MAGIC, 2 byte
        private val names = mutableSetOf<String>()
        private val entries = mutableListOf<Entry>()

        fun add(name: String, content: ByteArray): Writer {
            check(name !in names) { "already exists entry: name=$name" }

            val offset = output.size()
            val compressedContent = compress(content)
            val compressed = content.size > compressedContent.size
            output.bytes(if (compressed) compressedContent else content)
            val length = output.size() - offset
            names += name
            entries += Entry(name, offset, length, compressed)
            return this
        }

        fun finish(): ByteArray {
            val entries = ByteArrayOutputStream().apply {
                int4L(entries.size)
                entries.forEach { it.write(this) }
            }.toByteArray()
            val compressedEntries = compress(entries)
            val compressed = entries.size > compressedEntries.size
            val data = if(compressed) compressedEntries else entries

            val result = output.apply {
                bytes(data)
                int4L((if (compressed) -data.size else data.size)) // 4 bytes
                short2L(0) // CRC 2 bytes
            }.toByteArray()
            val crc = CRC32().apply { update(result, 0, result.size - 2) }.value.toShort()
            result.short2L(result.size - 2, crc)
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

        fun write(output: OutputStream) = output.run {
            check(nameBytes.size < 256) { "entry name must be less than 256 bytes." }
            byte1(nameBytes.size)
            bytes(nameBytes)
            int4L((if (compressed) -length else length))
        }

        companion object {
            fun read(input: InputStream, offset: Int): Entry = input.run {
                val nameLength = byte1()
                val name = utf8(nameLength)
                val length = int4L()
                Entry(name, offset, length.absoluteValue, length < 0)
            }
        }
    }
}
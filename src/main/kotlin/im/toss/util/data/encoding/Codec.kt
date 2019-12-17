package im.toss.util.data.encoding

interface Codec {
    fun encode(bytes: ByteArray): ByteArray
    fun decode(bytes: ByteArray, offset: Int, length: Int): ByteArray
}
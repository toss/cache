package im.toss.util.data.encoding

interface Codec {
    val name: String
    fun encode(bytes: ByteArray): ByteArray
    fun decode(bytes: ByteArray, offset: Int, length: Int): ByteArray
}
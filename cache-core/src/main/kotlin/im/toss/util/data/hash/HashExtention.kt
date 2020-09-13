package im.toss.util.data.hash

import java.security.MessageDigest
import java.util.*

fun ByteArray.sha1(): ByteArray = MessageDigest.getInstance("SHA1").digest(this)
fun ByteArray.base64(): String = Base64.getEncoder().encodeToString(this)
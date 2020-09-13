package im.toss.util.repository

import java.util.concurrent.TimeUnit

interface KeyValueRepository {
    suspend fun get(key: String): ByteArray?
    suspend fun set(key: String, value: ByteArray, ttl: Long, unit: TimeUnit)
    suspend fun expire(key: String, ttl: Long, unit: TimeUnit)
    suspend fun delete(key: String): Boolean
}
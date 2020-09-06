package im.toss.util.repository

import java.util.concurrent.TimeUnit

interface KeyFieldValueRepository {
    suspend fun get(key: String, field: String): ByteArray?
    suspend fun set(key: String, field: String, value: ByteArray, ttl: Long, unit: TimeUnit)
    suspend fun incrBy(key: String, field: String, amount: Long, ttl: Long, unit: TimeUnit): Long
    suspend fun expire(key: String, ttl: Long, unit: TimeUnit)
    suspend fun delete(key: String): Boolean
    suspend fun delete(key: String, field: String): Boolean
}
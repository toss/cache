package im.toss.util.repository

import java.util.concurrent.TimeUnit

class InMemoryKeyFieldValueRepository(
    private val repository: InMemoryRepository
) : KeyFieldValueRepository {
    override suspend fun get(key: String, field: String): ByteArray? {
        return repository.hget(key, field)
    }

    override suspend fun set(key: String, field: String, value: ByteArray, ttl: Long, unit: TimeUnit) {
        repository.hset(key, field, value)
        if (ttl > 0L) {
            repository.expire(key, ttl, unit)
        }
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        repository.expire(key, ttl, unit)
    }

    override suspend fun delete(key: String): Boolean {
        return repository.del(key)
    }

    override suspend fun delete(key: String, field: String): Boolean {
        return repository.hdel(key, field)
    }
}


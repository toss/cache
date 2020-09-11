package im.toss.util.repository

import java.util.concurrent.*

class InMemoryKeyValueRepository(
    private val repository: InMemoryRepository
) : KeyValueRepository {
    override suspend fun get(key: String): ByteArray? {
        return repository.get(key)
    }

    override suspend fun set(key: String, value: ByteArray, ttl: Long, unit: TimeUnit) {
        repository.setex(key, ttl, unit, value)
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        repository.expire(key, ttl, unit)
    }

    override suspend fun delete(key: String): Boolean {
        return repository.del(key)
    }
}


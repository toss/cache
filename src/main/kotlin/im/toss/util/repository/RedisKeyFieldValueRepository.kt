package im.toss.util.repository

import im.toss.util.coroutine.runWithTimeout
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import java.util.concurrent.TimeUnit

class RedisKeyFieldValueRepository(
    private val readTimeoutMillis: Long,
    private val commands: RedisAdvancedClusterReactiveCommands<ByteArray, ByteArray>
) :
    KeyFieldValueRepository {
    override suspend fun get(key: String, field: String): ByteArray? {
        return runWithTimeout(readTimeoutMillis) {
            commands.hget(key.toByteArray(), field.toByteArray()).awaitFirstOrNull()
        }
    }

    override suspend fun set(key: String, field:String, value: ByteArray, ttl: Long, unit: TimeUnit) {
        runWithTimeout(readTimeoutMillis) {
            commands.hset(key.toByteArray(), field.toByteArray(), value).awaitSingle()
            if (ttl > 0L) {
                commands.expire(key.toByteArray(), unit.toSeconds(ttl)).awaitSingle()
            }
        }
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        runWithTimeout(readTimeoutMillis) {
            commands.expire(key.toByteArray(), unit.toSeconds(ttl)).awaitSingle()
        }
    }

    override suspend fun delete(key: String): Boolean {
        return runWithTimeout(readTimeoutMillis) {
            commands.del(key.toByteArray()).awaitSingle() == 1L
        }
    }

    override suspend fun delete(key: String, field: String): Boolean {
        return runWithTimeout(readTimeoutMillis) {
            commands.hdel(key.toByteArray(), field.toByteArray()).awaitSingle() == 1L
        }
    }
}
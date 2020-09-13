package im.toss.util.repository

import im.toss.util.coroutine.runWithTimeout
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import java.util.concurrent.TimeUnit

class RedisKeyValueRepository(
    private val readTimeoutMillis: Long,
    private val commands: RedisAdvancedClusterReactiveCommands<ByteArray, ByteArray>
) :
    KeyValueRepository {
    override suspend fun get(key: String): ByteArray? {
        return runWithTimeout(readTimeoutMillis) {
            commands.get(key.toByteArray()).awaitFirstOrNull()
        }
    }

    override suspend fun set(key: String, value: ByteArray, ttl: Long, unit: TimeUnit) {
        runWithTimeout(readTimeoutMillis) {
            commands.setex(key.toByteArray(), unit.toSeconds(ttl), value).awaitSingle()
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
}
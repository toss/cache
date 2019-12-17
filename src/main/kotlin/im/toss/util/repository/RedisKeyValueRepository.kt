package im.toss.util.repository

import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import java.util.concurrent.TimeUnit

class RedisKeyValueRepository(private val commands: RedisAdvancedClusterReactiveCommands<ByteArray, ByteArray>) :
    KeyValueRepository {
    override suspend fun get(key: String): ByteArray? {
        return commands.get(key.toByteArray()).awaitFirstOrNull()
    }

    override suspend fun set(key: String, value: ByteArray, ttl: Long, unit: TimeUnit) {
        commands.setex(key.toByteArray(), unit.toSeconds(ttl), value).awaitSingle()
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        commands.expire(key.toByteArray(), unit.toSeconds(ttl)).awaitSingle()
    }

    override suspend fun delete(key: String): Boolean {
        return commands.del(key.toByteArray()).awaitSingle() == 1L
    }
}
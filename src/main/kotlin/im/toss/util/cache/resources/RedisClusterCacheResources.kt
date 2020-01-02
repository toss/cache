package im.toss.util.cache.resources

import im.toss.util.cache.CacheResources
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.concurrent.lock.RedisMutexLock
import im.toss.util.repository.KeyFieldValueRepository
import im.toss.util.repository.KeyValueRepository
import im.toss.util.repository.RedisKeyFieldValueRepository
import im.toss.util.repository.RedisKeyValueRepository
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands
import io.lettuce.core.codec.ByteArrayCodec

class RedisClusterCacheResources(
    private val readTimeoutMillis: Long,
    private val client: RedisClusterClient
) : CacheResources {
    private val connection by lazy {
        client.connect(ByteArrayCodec())
    }

    private fun commands(): RedisAdvancedClusterReactiveCommands<ByteArray, ByteArray> {
        return connection.reactive()
    }

    override fun keyValueRepository(): KeyValueRepository {
        return RedisKeyValueRepository(readTimeoutMillis, commands())
    }

    override fun keyFieldValueRepository(): KeyFieldValueRepository {
        return RedisKeyFieldValueRepository(readTimeoutMillis, commands())
    }

    override fun lock(autoReleaseSeconds: Long): MutexLock {
        return RedisMutexLock(autoReleaseSeconds, readTimeoutMillis, commands())
    }
}
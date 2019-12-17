package im.toss.util.concurrent.lock

import im.toss.util.redis.SetNEX
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands
import kotlinx.coroutines.reactive.awaitSingle
import java.util.concurrent.TimeUnit
import kotlin.math.max

class RedisMutexLock(
    private val autoreleaseSeconds: Long,
    private val commands: RedisAdvancedClusterReactiveCommands<ByteArray, ByteArray>
) : MutexLock() {
    private val setNEX = SetNEX(commands)

    override suspend fun acquire(key: String, timeout: Long, timeUnit: TimeUnit): Boolean {
        val rkey = rawKey(key)
        val value = System.currentTimeMillis().toString().toByteArray()

        val ttl = when {
            timeout == 0L -> 0L
            timeout < 0 -> autoreleaseSeconds
            else -> max(1L, timeUnit.toSeconds(timeout))
        }

        return if (ttl > 0) {
            setNEX.exec(rkey, ttl, value)
        } else {
            commands.setnx(rkey, value).awaitSingle()
        }
    }

    override suspend fun release(key: String): Boolean {
        return commands.del(rawKey(key)).awaitSingle() == 1L
    }

    override suspend fun isAcquired(key: String): Boolean {
        return commands.exists(rawKey(key)).awaitSingle() == 1L
    }

    private fun rawKey(key: String) = "$key:lock".toByteArray()
}
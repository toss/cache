package im.toss.util.redis

import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.reactive.RedisScriptingReactiveCommands

class SetNEX(
    commands: RedisScriptingReactiveCommands<ByteArray, ByteArray>
) : RedisFunction<ByteArray, ByteArray>(commands, ScriptOutputType.INTEGER, script) {
    companion object {
        private val script = """
            local success = redis.call("SETNX", KEYS[1], ARGV[2])
            if success == 1 then
                redis.call("EXPIRE", KEYS[1], ARGV[1])
                return 1
            else
                return 0
            end
            """.trimIndent().toByteArray()
    }

    suspend fun exec(key: ByteArray, expire: Long, value: ByteArray): Boolean {
        return super.eval<Int>(arrayOf(key), arrayOf(expire.toString().toByteArray(), value)) == 1
    }
}

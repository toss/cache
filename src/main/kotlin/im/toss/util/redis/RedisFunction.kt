package im.toss.util.redis

import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.reactive.RedisScriptingReactiveCommands
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import kotlinx.coroutines.reactive.awaitSingle

open class RedisFunction<K, V>(
    private val commands: RedisScriptingReactiveCommands<K, V>,
    private val scriptOutputType: ScriptOutputType,
    private val script: V
) {
    private val digest by lazy {
        commands.digest(script)
    }

    suspend fun <R> eval(keys: Array<K>, values: Array<V>): R {
        return try {
            commands.evalsha<R>(digest, scriptOutputType, keys, *values).awaitSingle()
        } catch (e: RedisNoScriptException) {
            val digest = commands.scriptLoad(script).awaitSingle()
            commands.evalsha<R>(digest, scriptOutputType, keys, *values).awaitSingle()
        }
    }

    suspend fun <R> evalOrDefault(keys: Array<K>, values: Array<V>, defaultValue: R): R {
        return try {
            commands.evalsha<R>(digest, scriptOutputType, keys, *values).awaitFirstOrDefault(defaultValue)
        } catch (e: RedisNoScriptException) {
            val digest = commands.scriptLoad(script).awaitSingle()
            commands.evalsha<R>(digest, scriptOutputType, keys, *values).awaitFirstOrDefault(defaultValue)
        }
    }
}
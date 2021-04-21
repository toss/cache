package im.toss.util.cache

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import im.toss.util.repository.KeyFieldValueRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.math.max

open class TTLValue<T>(initValue: T) {
    private var expireTime = -1L
    var value:T = initValue

    fun isExpired(): Boolean {
        return ttl() < 0L
    }

    fun expire(ttlMillis: Long) {
        expireTime = System.currentTimeMillis() + ttlMillis
    }

    fun ttl(): Long {
        return if (expireTime == -1L) {
            Long.MAX_VALUE
        } else {
            max(expireTime - System.currentTimeMillis(), -1L)
        }
    }
}

class TestKeyFieldValueRepository(
    val context: CoroutineContext? = null,
    val onGet:(key: String, field: String, result: ByteArray?) -> Unit = { _, _, _ -> },
    val onSet:(key: String, field: String, value: ByteArray?, ttl: Long, unit: TimeUnit) -> Unit = { _, _, _, _, _ -> },
) : KeyFieldValueRepository {
    class Item : TTLValue<MutableMap<String, ByteArray>>(ConcurrentHashMap())

    private suspend inline fun <R> with(crossinline final: R.() -> Unit = {}, crossinline block: () -> R): R {
        val run = {
            synchronized(this) {
                block().apply {
                    final()
                }
            }
        }

        return if (context == null) {
            run()
        } else {
            withContext(context) {
                run()
            }
        }
    }

    fun get(key:String): Item? {
        val item = items[key] ?: return null
        return if (item.isExpired()) {
            items.remove(key)
            return null
        } else item
    }

    private val items = ConcurrentHashMap<String, Item>()
    override suspend fun get(key: String, field: String): ByteArray? =
        with(final = { onGet(key, field, this) }) {
            val item = get(key) ?: return@with null
            return@with item.value[field]
        }

    override suspend fun set(key: String, field: String, value: ByteArray, ttl: Long, unit: TimeUnit) =
        with(final = { onSet(key, field, value, ttl, unit) }) {
            val item = items.getOrPut(key) { Item() }
            item.value[field] = value
            if (ttl > 0L) {
                item.expire(unit.toMillis(ttl))
            }
        }

    override suspend fun incrBy(key: String, field: String, amount: Long, ttl: Long, unit: TimeUnit): Long = with {
        val item = items.getOrPut(key) { Item() }
        val currentValue = item.value.computeIfAbsent(field) { "0".toByteArray(Charsets.UTF_8) }.toString(Charsets.UTF_8).toLong()
        (currentValue + amount).also {
            item.value[field] = it.toString().toByteArray(Charsets.UTF_8)
            if (ttl > 0L) {
                item.expire(unit.toMillis(ttl))
            }
        }
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        items[key]?.expire(unit.toMillis(ttl))
    }

    override suspend fun delete(key: String): Boolean = with {
        val item = items.remove(key)
        return@with if (item == null) false else !item.isExpired()
    }

    override suspend fun delete(key: String, field: String) = with {
        val item = get(key) ?: return@with false
        val result = item.value.remove(field) != null
        if (item.value.isEmpty()) {
            items.remove(key)
        }
        result
    }

    fun toMap() = items.mapNotNull { entry ->
        if (entry.value.isExpired()) {
            null
        } else {
            entry.key to entry.value.value.map { it.key to String(it.value) }.associate { it }
        }
    }.associate { it }

    fun toJson() = objectMapper.writeValueAsString(toMap())
}

val objectMapper = ObjectMapper().registerKotlinModule()
package im.toss.util.cache

import im.toss.util.repository.KeyValueRepository
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class TestKeyValueRepository : KeyValueRepository {
    class Item : TTLValue<ByteArray?>(null)

    private fun internalGet(key:String): Item? {
        val item = items[key] ?: return null
        return if (item.isExpired()) {
            items.remove(key)
            return null
        } else item
    }

    private val items = ConcurrentHashMap<String, Item>()
    override suspend fun get(key: String): ByteArray? = synchronized(this) {
        val item = internalGet(key) ?: return null
        return item.value
    }

    override suspend fun set(key: String, value: ByteArray, ttl: Long, unit: TimeUnit) = synchronized(this) {
        val item = items.getOrPut(key) { Item() }
        item.value = value
        if (ttl > 0L) {
            item.expire(unit.toMillis(ttl))
        }
    }

    override suspend fun expire(key: String, ttl: Long, unit: TimeUnit) {
        items[key]?.expire(unit.toMillis(ttl))
    }

    override suspend fun delete(key: String): Boolean = synchronized(this) {
        val item = items.remove(key)
        return if (item == null) false else !item.isExpired()
    }

    fun toMap() = items.mapNotNull { entry ->
        if (entry.value.isExpired()) {
            null
        } else {
            entry.key to entry.value.value?.let { String(it) }
        }
    }.associate { it }

    fun toJson() = objectMapper.writeValueAsString(toMap())
}
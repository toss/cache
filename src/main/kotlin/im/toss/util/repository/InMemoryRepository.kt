package im.toss.util.repository


import kotlinx.coroutines.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.LinkedHashMap
import kotlin.math.max

class InMemoryRepository {
    val items = ConcurrentSkipListMap<String, Item>()
    val queue = ConcurrentSkipListSet<Expire>(compareBy(Expire::time, Expire::key))
    private var processing = AtomicBoolean()

    fun shutdown() {
        queue.clear()
        items.clear()
    }

    fun get(key: String): ByteArray? = synchronized(this) {
        val item = getItem(key) ?: return null
        return item.value as ByteArray
    }

    fun setnex(key: String, ttl: Long, timeUnit: TimeUnit, value: ByteArray): Boolean = synchronized(this) {
        val newItem = Item(value, ttl, timeUnit)
        val oldItem = items.putIfAbsent(key, newItem)
        return if (oldItem == null) {
            updateSchedule(key, -1L, newItem.expire)
            true
        } else {
            if (oldItem.isExpired()) {
                items[key] = newItem
                updateSchedule(key, oldItem.expire, newItem.expire)
                true
            } else {
                false
            }
        }
    }

    fun setex(key: String, ttl: Long, timeUnit: TimeUnit, value: ByteArray) = synchronized(this) {
        items.compute(key) { _, prevItem ->
            if (prevItem == null) {
                Item(value, ttl, timeUnit) { next -> updateSchedule(key, -1L, next) }
            } else {
                prevItem.value = value
                prevItem.expire(ttl, timeUnit) { prev, next -> updateSchedule(key, prev, next) }
                prevItem
            }
        }
    }

    fun expire(key: String, ttl: Long, timeUnit: TimeUnit) = synchronized(this) {
        items.computeIfPresent(key) { _, item ->
            item.expire(ttl, timeUnit) { prev, next -> updateSchedule(key, prev, next) }
            item
        }
    }

    fun del(key: String): Boolean = synchronized(this) {
        val item = items.remove(key)
        return if (item == null) {
            false
        } else {
            updateSchedule(key, item.expire, -1L)
            true
        }
    }

    fun hget(key: String, field: String): ByteArray? = synchronized(this) {
        val item = getItem(key) ?: return null
        @Suppress("UNCHECKED_CAST")
        val map = item.value as LinkedHashMap<String, ByteArray>
        return map[field]
    }

    fun hset(key: String, field: String, value: ByteArray) = synchronized(this) {
        val item = items.computeIfAbsent(key) {
            Item(LinkedHashMap<String, ByteArray>())
        }

        @Suppress("UNCHECKED_CAST")
        val map = item.value as LinkedHashMap<String, ByteArray>
        map[field] = value
    }

    fun hincrby(key: String, field: String, value: Long): Long = synchronized(this) {
        val item = items.computeIfAbsent(key) {
            Item(LinkedHashMap<String, ByteArray>())
        }

        @Suppress("UNCHECKED_CAST")
        val map = item.value as LinkedHashMap<String, ByteArray>

        val currentValue = map.computeIfAbsent(field) { "0".toByteArray(Charsets.UTF_8) }.toString(Charsets.UTF_8).toLong()
        (currentValue + value).also { map[field] = it.toString().toByteArray(Charsets.UTF_8) }
    }


    fun hdel(key: String, field: String): Boolean = synchronized(this) {
        val item = getItem(key) ?: return false
        @Suppress("UNCHECKED_CAST")
        val map = item.value as MutableMap<String, ByteArray>
        return map.remove(field) != null
    }

    fun exists(key: String): Boolean = synchronized(this) {
        return getItem(key) != null
    }

    private fun getItem(key:String): Item? {
        val item = items[key] ?: return null
        return if (item.isExpired()) {
            items.remove(key)
            return null
        } else item
    }

    data class Expire(val key: String, val time: Long)

    private fun updateSchedule(key: String, prevTime: Long, nextTime: Long) {
        if (prevTime > 0L) {
            queue.remove(Expire(key, prevTime))
        }
        if (nextTime > 0L) {
            queue.add(Expire(key, nextTime))
            processExpires()
        }
    }

    private fun processExpires() {
        if (processing.compareAndSet(false, true)) {
            GlobalScope.launch {
                try {
                    var idle = true
                    while (queue.size > 0) {
                        when (idle) {
                            true -> delay(50)
                            false -> delay(2)
                        }
                        val limit = 100

                        val removed = removeExpiredItems(100)
                        idle = removed < limit
                    }
                } finally {
                    processing.set(false)
                }
            }
        }
    }

    private fun removeExpiredItems(limit: Int): Int = synchronized(this) {
        var removed = 0
        val now = System.currentTimeMillis()
        val iter = queue.iterator()
        while(iter.hasNext() && removed < limit) {
            val item = iter.next()
            if (item.time < now) {
                iter.remove()
                del(item.key)
                removed++
            }
        }
        return removed
    }

   class Item(initValue: Any, ttl: Long = -1L, timeUnit: TimeUnit = TimeUnit.MILLISECONDS, changed: ((next: Long) -> Unit)? = null) {
        var value: Any = initValue
        var expire = time(ttl, timeUnit)

        init {
            if (changed != null) {
                changed(expire)
            }
        }

        private fun time(ttl: Long, timeUnit: TimeUnit): Long {
            return if (ttl < 0L) {
                -1L
            } else {
                System.currentTimeMillis() + timeUnit.toMillis(ttl)
            }
        }

        fun isExpired(): Boolean {
            return ttl() < 0L
        }

        fun expire(ttl: Long, timeUnit: TimeUnit, changed: (prev: Long, next: Long) -> Unit) {
            val prev = expire
            val next = time(ttl, timeUnit)
            expire = next
            changed(prev, next)
        }

        fun ttl(): Long {
            return if (expire == -1L) {
                Long.MAX_VALUE
            } else {
                max(expire - System.currentTimeMillis(), -1L)
            }
        }
    }
}

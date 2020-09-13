package im.toss.util.repository

import im.toss.test.equalsTo
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors

import java.util.concurrent.TimeUnit

class InMemoryKeyValueRepositoryTest {
    @Test
    fun `get test`() {
        runBlocking {
            val repository = InMemoryKeyValueRepository(InMemoryRepository())

            repository.set("1", "hello".toByteArray(), 1, TimeUnit.SECONDS)

            repository.get("1") equalsTo "hello".toByteArray()
        }
    }

    @Test
    fun `multi threaded set test`() {
        val threadCount = 8
        val valueCount = 2000
        val sources = (1..threadCount).map { threadIndex ->
            (1..valueCount).map { value ->
                "$threadIndex-$value" to "$value"
            }
        }

        val threadPool = Executors.newFixedThreadPool(threadCount).asCoroutineDispatcher()
        runBlocking {
            val r = InMemoryRepository()
            val repository = InMemoryKeyValueRepository(r)
            val startTime = System.currentTimeMillis()
            withContext(threadPool) {
                sources.map { data ->
                    async {
                        data.forEach { (key, value) ->
                            repository.set(key, value.toByteArray(), 2, TimeUnit.SECONDS)
                        }
                    }
                }.awaitAll()
            }
            val elapsed = System.currentTimeMillis() - startTime
            println("put all $elapsed ms")

            val startTime2 = System.currentTimeMillis()
            withContext(threadPool) {
                sources.map { data ->
                    async {
                        data.forEach { (key, value) ->
                            val stored = repository.get(key)
                            stored equalsTo value.toByteArray()
                        }
                    }
                }.awaitAll()
            }
            val elapsed2 = System.currentTimeMillis() - startTime2
            println("verify all $elapsed2 ms")

            while(r.queue.isNotEmpty()) {
                delay(100)
            }
            println("done")
        }
    }

    @Test
    fun `expire test`() {
        runBlocking {
            val r = InMemoryRepository()
            val repository = InMemoryKeyValueRepository(r)
            (1..10).forEach {
                repository.set(it.toString(), it.toString().toByteArray(), it.toLong() * 100, TimeUnit.MILLISECONDS)
            }

            delay(400)

            println(r.queue)
            println(r.items)
        }
    }
}
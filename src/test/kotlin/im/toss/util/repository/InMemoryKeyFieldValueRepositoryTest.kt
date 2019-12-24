package im.toss.util.repository

import im.toss.test.equalsTo
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.Executors

import java.util.concurrent.TimeUnit

class InMemoryKeyFieldValueRepositoryTest {
    @Test
    fun `get test`() {
        runBlocking {
            val repository = InMemoryKeyFieldValueRepository(InMemoryRepository())

            repository.set("1", "field", "hello".toByteArray(), 1, TimeUnit.SECONDS)

            repository.get("1", "field") equalsTo "hello".toByteArray()
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
            val repository = InMemoryKeyFieldValueRepository(InMemoryRepository())
            withContext(threadPool) {
                sources.map { data ->
                    async {
                        data.forEach { (key, value) ->
                            repository.set(key, "field", value.toByteArray(), -1, TimeUnit.SECONDS)
                        }
                    }
                }.awaitAll()
            }

            sources.map { data ->
                data.forEach { (key, value) ->
                    val stored = repository.get(key, "field")
                    stored equalsTo value.toByteArray()
                }
            }
        }
    }
}
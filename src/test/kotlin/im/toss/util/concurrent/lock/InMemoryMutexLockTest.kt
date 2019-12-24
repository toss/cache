package im.toss.util.concurrent.lock

import im.toss.test.equalsTo
import im.toss.util.repository.InMemoryRepository
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.Executors

internal class InMemoryMutexLockTest {

    @Test
    fun acquire() {
        runBlocking {
            val lock = InMemoryMutexLock(2, InMemoryRepository())
            lock.acquire("hello") equalsTo true
            lock.acquire("hello") equalsTo false
        }
    }


    @Test
    fun release() {
        runBlocking {
            val lock = InMemoryMutexLock(2, InMemoryRepository())
            lock.acquire("hello") equalsTo true
            lock.release("hello") equalsTo true
            lock.acquire("hello") equalsTo true
        }
    }

    @Test
    fun autorelease() {
        runBlocking {
            val lock = InMemoryMutexLock(1, InMemoryRepository())
            lock.acquire("hello") equalsTo true
            delay(1000L)
            lock.acquire("hello") equalsTo true
        }
    }

    @Test
    fun isAcquired() {
        runBlocking {
            val lock = InMemoryMutexLock(2, InMemoryRepository())
            lock.acquire("hello") equalsTo true
            lock.isAcquired("hello") equalsTo true
        }
    }

    @Test
    fun multiThreadLock() {
        val threadCount = 8
        val threadPool = Executors.newFixedThreadPool(threadCount).asCoroutineDispatcher()
        val lock = InMemoryMutexLock(2, InMemoryRepository())
        runBlocking(threadPool) {
            var nonAtomicLong = 0L
            (1..10).map {
                async {
                    (1..100).forEach {
                        runOrRetry {
                            lock.run("hello", 1000) {
                                val value = ++nonAtomicLong
                                println("${Thread.currentThread().id} => $value")
                            }
                        }
                    }
                }
            }.awaitAll()
        }
    }
}
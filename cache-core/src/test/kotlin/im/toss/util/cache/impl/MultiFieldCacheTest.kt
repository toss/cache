package im.toss.util.cache.impl

import im.toss.test.equalsTo
import im.toss.util.cache.*
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.coroutine.runWithTimeout
import im.toss.util.data.serializer.StringSerializer
import im.toss.util.repository.KeyFieldValueRepository
import im.toss.util.thread.newThreadPoolExecutor
import im.toss.util.time.NanoClock
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.*
import kotlinx.coroutines.reactive.awaitSingle
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.system.measureTimeMillis

class MultiFieldCacheTest {
    private fun testCache(
        repository: KeyFieldValueRepository? = null,
        version: String = "0001",
        ttl: Long = 100L,
        coldTime: Long = 0L,
        applyTtlIfHit: Boolean = true,
        context: CoroutineContext? = null,
        mutexLock: MutexLock = LocalMutexLock(5000),
        failurePolicy: CacheFailurePolicy = CacheFailurePolicy.ThrowException,
        pessimisticLock: Boolean = true,
        optimisticLock: Boolean = true,
    ) = MultiFieldCacheImpl<String>(
        name = "dict_cache",
        keyFunction = Cache.KeyFunction { name, key -> "$name:{$key}" },
        lock = mutexLock,
        repository = repository ?: TestKeyFieldValueRepository(),
        serializer = StringSerializer,
        context = context,
        options = cacheOptions(
            version = version,
            ttl = ttl,
            ttlTimeUnit = TimeUnit.MILLISECONDS,
            coldTime = coldTime,
            coldTimeUnit = TimeUnit.MILLISECONDS,
            applyTtlIfHit = applyTtlIfHit,
            cacheFailurePolicy = failurePolicy,
            lockTimeout = 10,
            enablePessimisticLock = pessimisticLock,
            enableOptimisticLock = optimisticLock,
        )
    )

    // timeout 테스트
    @TestFactory
    fun `Cache의 TTL에 도래하면, key가 제거된다`(): List<DynamicTest> {
        data class Given(
            val ttl: Long, val delayTime: Long, val value: String, val expected: String?
        )
        return listOf(
            Given(ttl = 100, delayTime = 10, value = "value", expected = "value"),
            Given(ttl = 100, delayTime = 110, value = "value", expected = null)
        )
            .map { given ->
                dynamicTest("TTL이 ${given.ttl}ms인 ${given.value}값이,  ${given.delayTime}ms 이후에 값이 ${given.expected}이 된다.") {
                    runBlocking {
                        // given
                        val cache = testCache(ttl = given.ttl)
                        cache.getOrLoad("key", "field") { given.value }

                        // when
                        delay(given.delayTime)

                        // then
                        cache.get<String>("key", "field").equalsTo(given.expected)
                    }
                }
            }
    }

    @TestFactory
    fun `Cache의 TTL에 도래하기전 hit되면, TTL이 연장된다`(): List<DynamicTest> {
        data class Given(
            val ttl: Long, val hitTime: Long, val delayTime: Long, val value: String, val expected: String?
        )
        return listOf(
            Given(ttl = 100, hitTime = 50, delayTime = 10, value = "value", expected = "value"),
            Given(ttl = 100, hitTime = 50, delayTime = 80, value = "value", expected = "value"),
            Given(ttl = 100, hitTime = 50, delayTime = 110, value = "value", expected = null)
        )
            .map { given ->
                dynamicTest("TTL이 ${given.ttl}ms인 ${given.value}값이,  ${given.delayTime}ms 이후에 값이 ${given.expected}이 된다.") {
                    runBlocking {
                        // given
                        val cache = testCache(ttl = given.ttl)
                        cache.getOrLoad("key", "field") { given.value }

                        // when
                        delay(given.hitTime)
                        cache.get<String>("key", "field").equalsTo(given.value)
                        delay(given.delayTime)

                        // then
                        cache.get<String>("key", "field").equalsTo(given.expected)
                    }
                }
            }
    }

    @Test
    fun `getOrLoad의 applyTtlIfHit가 false이면, hit시 ttl이 연장되지 않는다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 100, applyTtlIfHit = false)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            delay(50)
            cache.getOrLoad<String>("key", "field") { "reloaded" }
            delay(55)

            // then
            cache.get<String>("key", "field").equalsTo(null)
        }
    }

    // manual evict 테스트
    @Test
    fun `키를 evict하면 키의 모든 데이터가 제거된다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 1000)
            cache.getOrLoad("key", "field1") { "value1" }
            cache.getOrLoad("key", "field2") { "value2" }

            // when
            cache.evict("key")

            // then
            cache.get<String>("key", "field1").equalsTo(null)
            cache.get<String>("key", "field2").equalsTo(null)
        }
    }

    @Test
    fun `evict하면 같은 키의 여러 버전의 캐시 데이터가 일괄 제거 된다`() {
        runBlocking {
            val repository = TestKeyFieldValueRepository()
            val cacheCount = 10
            (1..cacheCount).forEach { cacheVersion ->
                val caches = (1..cacheCount).associateWith { version ->
                    testCache(
                        repository = repository,
                        version = "$version",
                        ttl = 1000
                    )
                }

                caches.forEach { (version, cache) -> cache.getOrLoad("key", "field") { "value:$version" } }
                (caches[cacheVersion] ?: error("")).evict("key")
                caches.values.forEach { it.get<String>("key", "field").equalsTo(null) }
            }
        }
    }

    @Test
    fun `같은 키의 캐시라도 버전이 다르면 격리되어 저장되고 읽을 수 있다`() {
        runBlocking {
            // given
            val repository = TestKeyFieldValueRepository()
            val cacheV1 = testCache(repository = repository, version = "v1", ttl = 1000)
            val cacheV2 = testCache(repository = repository, version = "v2", ttl = 1000)

            // when
            cacheV1.getOrLoad("key", "field") { "value:v1" }
            cacheV2.getOrLoad("key", "field") { "value:v2" }

            // then
            cacheV1.get<String>("key", "field").equalsTo("value:v1")
            cacheV2.get<String>("key", "field").equalsTo("value:v2")
        }
    }

    // evict 이후 cold time 테스트
    @TestFactory
    fun `evict이후 설정된 coldtime동안 cache에 적재 되지 않는다`(): List<DynamicTest> {
        data class Given(
            val ttl: Long, val coldTime: Long, val delayTime: Long,
            val initValue: String, val updateValue: String, val expected: String?
        )
        return listOf(
            Given(
                ttl = 200,
                coldTime = 100,
                delayTime = 20,
                initValue = "value",
                updateValue = "updated",
                expected = null
            ),
            Given(
                ttl = 200,
                coldTime = 200,
                delayTime = 150,
                initValue = "value",
                updateValue = "updated",
                expected = null
            ),
            Given(
                ttl = 200,
                coldTime = 100,
                delayTime = 120,
                initValue = "value",
                updateValue = "updated",
                expected = "updated"
            ),
            Given(
                ttl = 200,
                coldTime = 200,
                delayTime = 300,
                initValue = "value",
                updateValue = "updated",
                expected = "updated"
            )
        )
            .map { given ->
                dynamicTest("coldTime이 ${given.coldTime}ms일때, evict하고 ${given.delayTime}ms 이후에 ${given.updateValue} 값을 적재하면, ${given.expected}이 된다.") {
                    runBlocking {
                        // given
                        val repository = TestKeyFieldValueRepository()
                        val cache = testCache(repository = repository, ttl = given.ttl, coldTime = given.coldTime)
                        println("coldTime: ${given.coldTime} ms")
                        cache.getOrLoad("key", "field") { given.initValue }
                        println("given: ${repository.toJson()}")

                        // when
                        cache.evict("key")
                        println("evicted: ${repository.toJson()}")
                        delay(given.delayTime)
                        println("delay ${given.delayTime}ms")
                        cache.getOrLoad("key", "field") { given.updateValue }
                        println("loaded: ${repository.toJson()}")

                        // then
                        cache.get<String>("key", "field").equalsTo(given.expected)
                    }
                }
            }
    }

    abstract class DataSource {
        abstract suspend fun get(key: String, field: String, value: String): String
    }

    @Test
    fun `getOrLoad로 같은 키를 가지는 데이터 소스의 읽기를 병렬로 수행하면, 하나만 실행된다 - Collapsed Forwarding`() {
        runBlocking {
            var lastCaptured = "NOT_CAPTURED"
            val slot = slot<String>()
            val dataSource = mockk<DataSource>()
            coEvery {
                dataSource.get(any(), any(), capture(slot))
            } coAnswers {
                delay(200)
                lastCaptured = slot.captured
                slot.captured
            }

            // given
            val cache = testCache(ttl = 1000)

            // when
            listOf(
                "first",
                "second",
                "third"
            )
                .map { fetched ->
                    async {
                        cache.getOrLoad("key", "field") {
                            dataSource.get("key", "field", fetched)
                        }
                    }
                }.awaitAll()

            // then
            cache.get<String>("key", "field").equalsTo(lastCaptured)
            coVerify(exactly = 1) { dataSource.get(any(), any(), any()) }
        }
    }

    @Test
    fun `load로 같은 키를 가지는 데이터 소스의 읽기를 병렬로 수행하면, 모두 실행된다 - Collapsed Forwarding`() {
        runBlocking {
            var lastCaptured = "NOT_CAPTURED"
            val slot = slot<String>()
            val dataSource = mockk<DataSource>()
            coEvery {
                dataSource.get(any(), any(), capture(slot))
            } coAnswers {
                delay(200)
                lastCaptured = slot.captured
                slot.captured
            }

            // given
            val cache = testCache(ttl = 1000)

            // when
            listOf(
                "first",
                "second",
                "third"
            )
                .map { fetched ->
                    async {
                        cache.load("key", "field") {
                            dataSource.get("key", "field", fetched)
                        }
                    }
                }.awaitAll()

            // then
            cache.get<String>("key", "field").equalsTo(lastCaptured)
            coVerify(exactly = 3) { dataSource.get(any(), any(), any()) }
        }
    }

    @Test
    fun `같은 키의 다른 필드를 가지는 데이터 소스의 읽기는 동시에 실행된다 - Collapsed Forwarding`() {
        val fetchDelay = 100L
        val slot = slot<String>()
        val dataSource = mockk<DataSource>()
        coEvery {
            dataSource.get(any(), any(), capture(slot))
        } coAnswers {
            delay(fetchDelay)
            slot.captured
        }

        // given
        val given = listOf(
            "field1" to "value1",
            "field2" to "value2",
            "field3" to "value3",
            "field4" to "value4",
            "field5" to "value5"
        )

        val cache = testCache(ttl = 1000)

        // when
        val elapsed = measureTimeMillis {
            runBlocking {
                given
                    .map {
                        async {
                            cache.getOrLoad("key", it.first) {
                                dataSource.get("key", it.first, it.second)
                            }
                        }
                    }.awaitAll()
            }
        }

        println(elapsed)

        // then
        assertThat(elapsed).isCloseTo(fetchDelay, Offset.offset(100L))
        given.forEach {
            coVerify(exactly = 1) { dataSource.get("key", it.first, it.second) }
        }
    }

    // 2번의 읽기 완료 순서 섞임 테스트
    @Test
    fun `데이터 소스의 읽기의 지연시간에 차이가 발생해도, 먼저 읽은 값이 덮어쓰지 않는다`() {
        // Collapsed Forwarding을 사용하면, 언제나 먼저 읽은 값이 기록된다.
    }

    // 읽기와 evict 섞임 테스트
    @Test
    fun `데이터 소스로부터 값을 읽고, 적재가 되기 전, evict 발생 시, 값이 적재되지 않아야한다`() {
        runBlocking {
            val repository = TestKeyFieldValueRepository()
            val cache = testCache(repository = repository, ttl = 200)

            // given
            println("given: ${repository.toJson()}")

            // when
            listOf(
                async {
                    println("write begin")
                    val fetched = cache.getOrLoad("key", "field") {
                        delay(500)
                        "delayValue"
                    }
                    println("write end: $fetched")
                },
                async {
                    println("evict begin")
                    delay(200)
                    cache.evict("key")
                    println("evict end")
                }
            ).awaitAll()

            println("then: ${repository.toJson()}")
            cache.get<String>("key", "field").equalsTo(null)
        }
    }


    @Test
    fun `여러 필드에서 데이터 소스로부터 값을 읽고, 적재가 되기 전, evict 발생 시, 값이 적재되지 않아야한다`() {
        runBlocking(Executors.newFixedThreadPool(4).asCoroutineDispatcher()) {
            val repository = TestKeyFieldValueRepository()
            val cache = testCache(repository = repository, ttl = 5000)

            // given
            println("given: ${repository.toJson()}")
            val startTime = System.currentTimeMillis()
            fun now() = (System.currentTimeMillis() - startTime).toString().padStart(5, ' ')
            fun log(value: String) = println(" ${now()} $value")

            // when
            // |------------------------- load(key, "slow") ------------------------->| FAILED
            // |--- load(key, "1") --->| LOADED
            //                            |- evict(key)->|
            //                                             |--- load(key, "2") --->| LOADED
            // ==> cache == { key: {"2":"value"} }
            listOf(
                async {
                    log("load1 begin")
                    val fetched = cache.getOrLoad("key", "short1") {
                        delay(100)
                        "shortDelayValue1"
                    }
                    log("load1 end: $fetched  => ${repository.toJson()}")
                },
                async {
                    delay(200)
                    log("load2 begin")
                    val fetched = cache.getOrLoad("key", "short2") {
                        delay(200)
                        "shortDelayValue2"
                    }
                    log("load2 end: $fetched  => ${repository.toJson()}")
                },
                async {
                    log("load3 begin")
                    val fetched = cache.getOrLoad("key", "long1") {
                        delay(1500)
                        "longDelayValue"
                    }
                    log("load3 end: $fetched  => ${repository.toJson()}")
                },
                async {
                    delay(150)
                    cache.evict("key")
                    log("evicted  => ${repository.toJson()}")
                }
            ).awaitAll()

            log("end  => ${repository.toJson()}")
            cache.get<String>("key", "long1").equalsTo(null)
        }
    }

    @Test
    fun `캐시에 이미 로딩된 값이 있어도, load()로 강제 update 할 수 있다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 100)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.load("key", "field") { "reloaded" }

            // then
            cache.get<String>("key", "field").equalsTo("reloaded")
        }
    }


    @Test
    fun `키가 coldTime이면, load()로 로딩되지 않는다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 100, coldTime = 1000L)
            cache.getOrLoad("key", "field") { "preset" }
            cache.evict("key")

            // when
            cache.load("key", "field") { "reloaded" }

            // then
            cache.get<String>("key", "field").equalsTo(null)
        }
    }

    @Test
    fun `키가 coldTime중이라도 load(forceLoad=true)로 강제로딩된다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 100, coldTime = 1000L)
            cache.getOrLoad("key", "field") { "preset" }
            cache.evict("key")

            // when
            cache.load("key", "field", forceLoad = true) { "reloaded" }

            // then
            cache.get<String>("key", "field").equalsTo("reloaded")
        }
    }

    @Test
    fun `coldTime을 사용하지 않는 경우, coldTime이 적용되지 않는다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 1000, coldTime = -1L)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.evict("key")
            cache.load("key", "field") { "reloaded" }

            // then
            cache.get<String>("key", "field").equalsTo("reloaded")
        }
    }


    @Test
    fun `evictionOnly 모드일때도 evict이 작동한다`() {
        runBlocking {
            // given
            val cache = testCache(null, ttl = 1000)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.options.cacheMode = CacheMode.EVICTION_ONLY
            cache.evict("key")

            // then
            cache.get<String>("key", "field") equalsTo null
        }
    }


    @Test
    fun `evictionOnly 모드일때 load는 eviction이 된다`() {
        runBlocking {
            // given
            val repository = TestKeyFieldValueRepository()
            val cache = testCache(repository, ttl = 1000)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.options.cacheMode = CacheMode.EVICTION_ONLY
            cache.load("key", "field") { "loaded" }

            // then
            cache.get<String>("key", "field") equalsTo null
            repository.toMap().size equalsTo 0
        }
    }

    @Test
    fun `evictionOnly 모드일때 load는 fetch를 수행하지 않는다`() {
        runBlocking {
            val mock = mockk<DataSource>()
            coEvery { mock.get(any(), any(), any()) } coAnswers { "loaded" }

            // given
            val repository = TestKeyFieldValueRepository()
            val cache = testCache(repository, ttl = 1000)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.options.cacheMode = CacheMode.EVICTION_ONLY
            cache.load("key", "field") { mock.get("", "", "") }

            // then
            cache.get<String>("key", "field") equalsTo null
            repository.toMap().size equalsTo 0
            coVerify(exactly = 0) { mock.get(any(), any(), any()) }
        }
    }

    @Test
    fun `evictionOnly 모드일때 get은 항상 null을 반환한다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 1000)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.options.cacheMode = CacheMode.EVICTION_ONLY

            // then
            cache.get<String>("key", "field") equalsTo null
        }
    }

    @Test
    fun `evictionOnly 모드일때 getOrLoad시 항상 fetch하고 적재 되지 않는다`() {
        runBlocking {
            // given
            val repository = TestKeyFieldValueRepository()
            val cache = testCache(repository, ttl = 1000)
            cache.getOrLoad("key", "field") { "preset" }

            // when
            cache.options.cacheMode = CacheMode.EVICTION_ONLY

            // then
            cache.getOrLoad("key", "field") { "loaded" } equalsTo "loaded"
            cache.get<String>("key", "field") equalsTo null
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, load시 Mutex가 acquire에서 timeout이 발생하는경우, 캐시 로딩이 무시된다`() {
        runBlocking {
            val mutexLock = mockk<MutexLock>(relaxed = true)
            coEvery { mutexLock.acquire(any(), any(), any()) } coAnswers {
                runWithTimeout(50) {
                    delay(10000) // long delay
                    true
                }
            }

            val cache = testCache(mutexLock = mutexLock, failurePolicy = CacheFailurePolicy.FallbackToOrigin)

            withTimeout(1000) {
                cache.load("key", "field") { "hello" }
            }

            val result = cache.get<String>("key", "field")
            println(result)
            result equalsTo null
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, load시 Mutex에서 acquire중 예외가 발생하면, 캐시 로딩이 무시된다`() {
        runBlocking {
            val mutexLock = mockk<MutexLock>(relaxed = true)
            coEvery { mutexLock.acquire(any(), any(), any()) } coAnswers {
                throw Exception("테스트 예외")
            }

            val cache = testCache(mutexLock = mutexLock, failurePolicy = CacheFailurePolicy.FallbackToOrigin)
            cache.load("key", "field") { "hello" }

            val result = cache.get<String>("key", "field")
            println(result)
            result equalsTo null
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, load시 repository에서 timeout이 발생하는경우, 캐시 로딩이 무시된다`() {
        runBlocking {
            val mutexLock = mockk<MutexLock>(relaxed = true)
            coEvery { mutexLock.acquire(any(), any(), any()) } coAnswers {
                runWithTimeout(50) {
                    delay(10000) // long delay
                    true
                }
            }

            val cache = testCache(mutexLock = mutexLock, failurePolicy = CacheFailurePolicy.FallbackToOrigin)

            withTimeout(1000) {
                cache.load("key", "field") { "hello" }
            }

            val result = cache.get<String>("key", "field")
            println(result)
            result equalsTo null
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, load시 repository에서 예외가 발생하면, 캐시 로딩이 무시된다`() {
        runBlocking {
            val repository = mockk<KeyFieldValueRepository>(relaxed = true)
            coEvery { repository.get(any(), any()) } coAnswers {
                throw Exception("테스트 get 예외")
            }
            coEvery { repository.set(any(), any(), any(), any(), any()) } coAnswers {
                throw Exception("테스트 set 예외")
            }
            coEvery { repository.expire(any(), any(), any()) } coAnswers {
                throw Exception("테스트 expire 예외")
            }

            val cache = testCache(repository = repository, failurePolicy = CacheFailurePolicy.FallbackToOrigin)
            cache.load("key", "field") { "hello" }
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, get시 repository에서 예외가 발생하면, null이 반환된다`() {
        runBlocking {
            val repository = mockk<KeyFieldValueRepository>(relaxed = true)
            coEvery { repository.get(any(), any()) } coAnswers {
                throw Exception("테스트 get 예외")
            }
            coEvery { repository.set(any(), any(), any(), any(), any()) } coAnswers {
                throw Exception("테스트 set 예외")
            }
            coEvery { repository.expire(any(), any(), any()) } coAnswers {
                throw Exception("테스트 expire 예외")
            }

            val cache = testCache(repository = repository, failurePolicy = CacheFailurePolicy.FallbackToOrigin)

            val result = cache.get<String>("key", "field")
            println(result)
            result equalsTo null
        }
    }

    @Test
    fun `cacheFailurePolicy가 fallbackToOrigin일때, get시 repository에서 timeout이 발생하면, null이 반환된다`() {
        runBlocking {
            val repository = mockk<KeyFieldValueRepository>(relaxed = true)
            coEvery { repository.get(any(), any()) } coAnswers {
                runWithTimeout(50) {
                    delay(10000)
                    "delayValue".toByteArray()
                }
            }
            coEvery { repository.set(any(), any(), any(), any(), any()) } coAnswers {
                runWithTimeout(50) {
                    delay(10000)
                }
            }
            coEvery { repository.expire(any(), any(), any()) } coAnswers {
                runWithTimeout(50) {
                    delay(10000)
                }
            }

            val cache = testCache(repository = repository, failurePolicy = CacheFailurePolicy.FallbackToOrigin)

            val result = cache.get<String>("key", "field")
            println(result)
            result equalsTo null
        }
    }

    @Test
    fun `특정 키를 lockForLoad로 잠금을 하면 값을 쓸때까지 잠긴다`() {
        runBlocking {
            val cache = testCache()
            val loader = cache.lockForLoad<String>("key", "field")
            cache.get<String>("key", "field") equalsTo null
            println("${ZonedDateTime.now()}> lock")

            val job = async {
                delay(1000)
                println("${ZonedDateTime.now()}> load")
                loader.load("HELLO")

                assertThrows<AlreadyLoadedException> {
                    runBlocking { loader.load("WORLD") }
                }
            }

            cache.getOrLoad("key", "field") { "NEW" } equalsTo "HELLO"
            println("${ZonedDateTime.now()}> after get")
            job.await()
        }
    }

    @Test
    fun `getOrLock에서 데이터 로딩 중 예외가 발생하면 락이 즉시 풀려야한다`() {
        val cache = testCache()
        val before = System.nanoTime()
        assertThrows<Exception> {
            runBlocking {
                cache.getOrLoad<String>("key", "field") { throw Exception() }
            }
        }
        runBlocking {
            val cached = cache.getOrLoad("key", "field") { "HELLO" }
            val elapsed = TimeUnit.MILLISECONDS.convert(System.nanoTime() - before, TimeUnit.NANOSECONDS)
            cached equalsTo "HELLO"
            assertThat(elapsed).isLessThan(200)
        }
    }

    @Test
    fun `특정 키를 getOrLockForLoad로 잠금을 하면 값을 쓸때까지 잠긴다`() {
        runBlocking {
            val cache = testCache()
            val result = cache.getOrLockForLoad<String>("key", "field")
            result.value equalsTo null
            println("${ZonedDateTime.now()}> lock")

            val job = async {
                delay(1000)
                println("${ZonedDateTime.now()}> load")
                result.loader!!.load("HELLO")

                assertThrows<AlreadyLoadedException> {
                    runBlocking { result.loader!!.load("WORLD") }
                }
            }

            cache.getOrLoad("key", "field") { "NEW" } equalsTo "HELLO"
            println("${ZonedDateTime.now()}> after get")
            job.await()
        }
    }

    @Test
    fun `특정 키를 getOrLockForLoad로 잠금을 하고 release를 할때까지 잠긴다`() {
        runBlocking {
            val cache = testCache()
            cache.getOrLoad("key", "field") { "FIRST" }
            val loader = cache.lockForLoad<String>("key", "field")
            println("${ZonedDateTime.now()}> lock")

            val job = async {
                delay(1000)
                println("${ZonedDateTime.now()}> load")
                loader.release()

                assertThrows<AlreadyLoadedException> {
                    runBlocking { loader.load("WORLD") }
                }
            }

            cache.getOrLoad("key", "field") { "NEW" } equalsTo "FIRST"
            println("${ZonedDateTime.now()}> after get")
            job.await()
        }
    }

    @Test
    fun `락 옵션을 끄면, 락 실행 시 예외가 발생된다`() {
        val key = "key"
        val field = "field"

        assertThrows<NotSupportOptimisticLockException> {
            runBlocking {
                val cache = testCache(ttl = 10000, optimisticLock = false)
                cache.optimisticLockForLoad<String>(key, field)
            }
        }

        assertThrows<NotSupportPessimisticLockException> {
            runBlocking {
                val cache = testCache(ttl = 10000, pessimisticLock = false)
                cache.pessimisticLockForLoad<String>(key, field)
            }
        }
    }

    @Test
    fun `pessimistic lock 옵션을 끄면, lockForLoad시 optimistic lock으로 동작한다`() {
        val key = "key"
        val field = "field"

        runBlocking {
            val cache = testCache(ttl = 10000, pessimisticLock = false)
            val lock = cache.lockForLoad<String>(key, field)
            cache.load(key, field) { "VER0" }
            cache.get<String>(key, field) equalsTo "VER0"
            lock.load("VER1")
            cache.get<String>(key, field) equalsTo null
        }
    }

    @Test
    fun `모든 lock 옵션을 끄면, lockForLoad를 사용할 수 있지만, lock이 적용되지 않는다`() {
        val key = "key"
        val field = "field"

        runBlocking {
            val cache = testCache(ttl = 10000, pessimisticLock = false, optimisticLock = false)
            val lock = cache.lockForLoad<String>(key, field)
            cache.load(key, field) { "VER0" }
            cache.get<String>(key, field) equalsTo "VER0"
            lock.load("VER1")
            cache.get<String>(key, field) equalsTo "VER1"
        }
    }

    @Test
    fun `낙관적 락으로 캐시에 값을 기록한다`() {
        runBlocking {
            val key = "key"
            val field = "field"
            val cache = testCache(ttl = 10000)
            cache.load(key, field) { "VER0" }

            val lock = cache.optimisticLockForLoad<String>(key, field)
            lock.load("VER1")

            cache.get<String>(key, field) equalsTo "VER1"
        }
    }


    @Test
    fun `낙관적 락 중 키가 evict되면 기록하지 않는다`() {
        runBlocking {
            val key = "key";
            val field = "field"
            val cache = testCache(ttl = 10000)
            cache.load(key, field) { "VER0" }

            val lock = cache.optimisticLockForLoad<String>(key, field)
            cache.evict(key)
            lock.load("VER1")

            cache.get<String>(key, field) equalsTo null
        }
    }

    @Test
    fun `한 키 필드에 낙관적 락으로 동시에 기록이 되면 캐시에서 값을 제거한다`() {
        val startTime = System.currentTimeMillis()
        fun now() = (System.currentTimeMillis() - startTime).toString().padStart(5, ' ')
        fun log(value: String) = println(" ${now()} $value")

        /*
        |-- load() { "VER0" } -------------------------------------------------|
        |------------------- optimisticLockForLoad() ------ load("VER1") ------|
        |--------------------- optimisticLockForLoad() ------ load("VER2") ----| get() -> null
        */

        runBlocking {
            val key = "key";
            val field = "field"
            val cache = testCache(ttl = 10000)
            cache.load(key, field) { "VER0" }
            cache.get<String>(key, field) equalsTo "VER0"
            log("init")
            val jobs = listOf(
                async {
                    log("[1] lock for VER1")
                    val lock = cache.optimisticLockForLoad<String>(key, field)
                    log("[1] lock version: ${lock.version}")
                    delay(200)
                    lock.load("VER1")
                    log("[1] loaded VER1")
                    cache.get<String>(key, field) equalsTo null
                },
                async {
                    log("[2] lock for VER2")
                    val lock = cache.optimisticLockForLoad<String>(key, field)
                    log("[2] lock version: ${lock.version}")
                    delay(400)
                    lock.load("VER2")
                    log("[2] loaded VER2")
                    cache.get<String>(key, field) equalsTo null
                }
            )
            log("get expect VER0")
            cache.get<String>(key, field) equalsTo "VER0"
            delay(300)
            jobs.awaitAll()
            cache.get<String>(key, field) equalsTo null
        }
    }

    @Test
    fun `한 키 필드에 낙관적 락으로 두 값이 동시에 기록될때, 먼저 실행된 락이 늦게 기록되면 캐시에서 값을 제거한다`() {
        val startTime = System.currentTimeMillis()
        fun now() = (System.currentTimeMillis() - startTime).toString().padStart(5, ' ')
        fun log(value: String) = println(" ${now()} $value")

        /*
        |<-- load() { "VER0" } -->| get() -> "VER0"
        |-------------------<optimisticLockForLoad()> | <----------- delay -----------> | <- load("VER1") ->| get() -> null
        |--------------------------------<optimisticLockForLoad()> | <- load("VER2") -> |  get() -> "VER2"
        */

        runBlocking {
            val key = "key";
            val field = "field"
            val cache = testCache(ttl = 10000)
            cache.load(key, field) { "VER0" }
            cache.get<String>(key, field) equalsTo "VER0"
            log("init")
            val jobs = listOf(
                async {
                    log("[1] lock for VER1")
                    val lock = cache.optimisticLockForLoad<String>(key, field)
                    log("[1] lock version: ${lock.version}")
                    delay(500)
                    lock.load("VER1")
                    log("[1] loaded VER1")
                    // 먼저 시작했고, 늦게 로딩된 경우, 어떤 값이 더 최신 데이터인지 알기 어렵기때문에 캐시를 비운다.
                    cache.get<String>(key, field) equalsTo null
                },
                async {
                    delay(100)
                    log("[2] lock for VER2")
                    val lock = cache.optimisticLockForLoad<String>(key, field)
                    log("[2] lock version: ${lock.version}")
                    delay(100)
                    lock.load("VER2")
                    log("[2] loaded VER2")
                    // 뒤에 시작했고, 빨리 로딩된 경우 캐시에 데이터가 로딩 된다.
                    cache.get<String>(key, field) equalsTo "VER2"
                }
            )
            jobs.awaitAll()
            // 결국 캐시에 값은 없어진다
            cache.get<String>(key, field) equalsTo null
        }
    }


    @Test
    fun `비관적락과 낙관적락으로 동시에 값을 기록하면 캐시에서 값을 제거한다`() {
        val startTime = System.currentTimeMillis()
        fun now() = (System.currentTimeMillis() - startTime).toString().padStart(5, ' ')
        fun log(value: String) = println(" ${now()} $value")

        /*
        |-- load() { "VER0" } -------------------------------------------------|
        |------------------- optimisticLockForLoad() ------ load("VER1") ------|
        |------------------------------- lockForLoad() ------ load("VER2") ----| get() -> null
        */

        runBlocking {
            val key = "key";
            val field = "field"
            val cache = testCache(ttl = 10000)
            cache.load(key, field) { "VER0" }
            cache.get<String>(key, field) equalsTo "VER0"
            log("init")
            val jobs = listOf(
                async {
                    log("[1] optimistic lock for VER1")
                    val lock = cache.optimisticLockForLoad<String>(key, field)
                    log("[1] lock version: ${lock.version}")
                    delay(200)
                    lock.load("VER1")
                    log("[1] loaded VER1")
                    cache.get<String>(key, field) equalsTo null
                },
                async {
                    log("[2] pessimistic lock for VER2")
                    val lock = cache.lockForLoad<String>(key, field)
                    log("[2] lock version: ${lock.version}")
                    delay(400)
                    lock.load("VER2")
                    log("[2] loaded VER2")
                    cache.get<String>(key, field) equalsTo null
                }
            )
            log("get expect VER0")
            cache.get<String>(key, field) equalsTo "VER0"
            delay(300)
            jobs.awaitAll()
            cache.get<String>(key, field) equalsTo null
        }
    }

    @Test
    fun `cache 사용 현황을 확인 할 수 있다`() {
        runBlocking {
            // given
            val cache = testCache(ttl = 1000)

            // when
            cache.getOrLoad("key", "field") { "1" }
            cache.getOrLoad("key", "field") { "1" }
            cache.getOrLoad("key", "field") { "1" }
            cache.getOrLoad("key", "field") { "1" }
            cache.evict("key")
            cache.getOrLoad("key", "field") { "1" }

            // then
            cache.missCount equalsTo 2L
            cache.hitCount equalsTo 3L
            cache.putCount equalsTo 2L
            cache.evictionCount equalsTo 1L
        }
    }


    @TestFactory
    fun `cache의 내부 로직이 지정한 스레드에서 수행된다`(): List<DynamicTest> {
        fun shouldThread(namePrefix: String) {
            assertThat(Thread.currentThread().name).startsWith(namePrefix)
        }

        return listOf(
            true to "cache",
            false to "main"
        ).map { (isolated, expected) ->
            val repositoryContext = newThreadPoolExecutor(2, "repo").asCoroutineDispatcher()
            val cacheContext = newThreadPoolExecutor(2, "cache").asCoroutineDispatcher()
            dynamicTest("isolated가 ${isolated}이면 $expected 스레드에서 실행한다") {
                val logBuffer = StringBuilder()
                runBlocking {
                    (1..3).forEach { i ->
                        val startTime = LocalDateTime.now(NanoClock.DEFAULT)
                        fun log(msg: String) {
                            val duration = Duration.between(startTime, LocalDateTime.now(NanoClock.DEFAULT)).toNanos()
                            val output = String.format(
                                "%-20s | %.6fs | %s",
                                Thread.currentThread().name,
                                duration / 1_000_000_000.0,
                                msg
                            )
                            logBuffer.appendLine(output)
//                            println(output)
                        }

                        suspend fun fetch(value: String): String {
                            return Mono.fromCallable {
                                log("$i: in fetch")
                                shouldThread("parallel")
                                value
                            }
                                .publishOn(Schedulers.parallel())
                                .awaitSingle()
                        }

                        val cache = testCache(
                            ttl = 1000,
                            context = if (isolated) cacheContext else null,
                            repository = TestKeyFieldValueRepository(
                                repositoryContext,
                                onGet = { key, field, result ->
                                    log("$i: onGet(key=$key, field=$field) -> $result")
                                    shouldThread("repo")
                                },
                                onSet = { key, field, value, ttl, unit ->
                                    log("$i: onSet(key=$key, field=$field, value=$value, ttl=$ttl, unit=$unit)")
                                    shouldThread("repo")
                                }
                            ))
                        log("---------------------------------")
                        log("$i: init")
                        shouldThread("main")
                        cache.getOrLoad("key", "field") {
                            log("$i: before load")
                            shouldThread(expected)
                            fetch("100").apply {
                                log("$i: after load")
                                shouldThread(expected)
                            }
                        }
                        log("$i: final")
                        shouldThread("main")
                    }
                }

                println(logBuffer.toString())
            }
        }
    }
}


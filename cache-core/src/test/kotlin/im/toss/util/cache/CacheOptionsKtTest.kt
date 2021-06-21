package im.toss.util.cache

import org.junit.jupiter.api.Test
import kotlin.math.abs

internal class CacheOptionsKtTest {
    @Test
    fun isProbabilityTest() {
        fun runTest(probability: Float, sampleCount: Int, allowErrorCount: Int) {
            val hitCount = (0..sampleCount).count { isProbability(probability) }
            val expectedCount = sampleCount * probability
            val error = hitCount - expectedCount
            println("probability: $probability - sample: $sampleCount expectedCount: $expectedCount, hitCount: $hitCount -> error: $error")

            assert(abs(hitCount - expectedCount) < allowErrorCount)
        }
        runTest(0.001f, 10000, 200)
        runTest(0.01f, 10000, 200)
        runTest(0.05f, 10000, 200)
        runTest(0.1f, 10000, 200)
        runTest(0.2f, 10000, 200)
        runTest(0.3f, 10000, 200)
        runTest(0.4f, 10000, 200)
        runTest(0.5f, 10000, 200)
        runTest(0.6f, 10000, 200)
        runTest(0.7f, 10000, 200)
        runTest(0.8f, 10000, 200)
        runTest(0.9f, 10000, 200)
        runTest(1f, 10000, 200)
    }
}
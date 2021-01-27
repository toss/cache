package im.toss.util.reflection

import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

internal class TypeTest {
    @Test
    fun getTypeTest() {
        getType<Long>()!!.typeName equalsTo "java.lang.Long"
        getType<String>()!!.typeName equalsTo "java.lang.String"
    }

    @Test
    fun getTypeGenericArgumentTest() {
        getType<List<Int>>()!!.typeName equalsTo "java.util.List<? extends java.lang.Integer>"
        getType<Map<String, Long>>()!!.typeName equalsTo "java.util.Map<java.lang.String, ? extends java.lang.Long>"
        getType<Map<String, List<Long>>>()!!.typeName equalsTo "java.util.Map<java.lang.String, ? extends java.util.List<? extends java.lang.Long>>"
    }

    @Test
    fun getTypePerformance() {
        getType<List<Int>>()
        val elapsed = measureNanoTime {
            for(i in 1..10_000_000) {
                getType<List<Int>>()
            }
        }
        println(TimeUnit.MILLISECONDS.convert(elapsed, TimeUnit.NANOSECONDS))
    }
}




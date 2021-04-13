package im.toss.util.data.serializer

import im.toss.test.equalsTo
import im.toss.util.data.hash.base64
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.util.*

internal class Kryo5SerializerTest {

    data class TestMember(
        val name: String,
        val age: Long
    )

    data class TestClass(
        val members: List<TestMember>
    )

    @TestFactory
    fun serializeTest(): List<DynamicTest> = listOf(
        null to "AA==",
        "" to "AwGB",
        1 to "AgI=",
        1L to "CQI=",
        true to "BQE=",
        LocalTime.parse("12:11:34.123") to "DgEMCyLAqdM6",
        LocalDate.parse("2020-01-15") to "DQHkDwEP",
        ZonedDateTime.parse("2021-04-13T11:12:15.001+09:00") to "FAHlDwQNCwwPwIQ9KzA5OjCw",
        emptyList<String>() to "LwEB",
        emptyList<Int>() to "LwEB",
        emptySet<String>() to "MAEB",
        emptySet<Long>() to "MAEB",
        emptyArray<String>() to "KAEB",
        emptyArray<Int>() to "LAEB",
        emptyArray<Boolean>() to "LgEB",
        emptyMap<String, String>() to "MQEB",
        arrayOf(1) to "LAECAQI=",
        listOf(1) to "MwECAg==",
        setOf("") to "NAEDAYE=",
        mapOf("" to "") to "NQEDAYEDAw==",
        linkedMapOf("" to "") to "IAECAwGBAwM=",
        hashMapOf("" to "") to "HwECAwGBAwM=",
        (1..10).toList() to "GQGLAgACBAYICgwOEBIU",
        (1..10).toMutableList() to "GQGLAgACBAYICgwOEBIU",
        (1..10).toSet() to "HAGLAgACBAYICgwOEBIU",
        (1..10).toMutableSet() to "HAGLAgACBAYICgwOEBIU",
        TestMember("hello", 11) to "NgEWaGVsbO8=",
        TestClass(members = emptyList()) to "NwEvAQE=",
        TestClass(members = listOf(TestMember("a", 1), TestMember("b", 2))) to "NwEyAQMBAoJhAQSCYg==",
    ).map { (obj, expected) ->
        dynamicTest("serialize (${obj?.javaClass?.canonicalName})$obj -> $expected") {
            val kryoFactory = Kryo5Serializer.factoryBuilder()
                .register(TestMember::class.java)
                .register(TestClass::class.java)
                .build()
            val serializer = Kryo5Serializer(kryoFactory)
            val serialized = serializer.serialize(obj)
            println("[${serializer.name}] (${obj?.javaClass?.canonicalName})$obj -> (size=${serialized.size})${serialized.base64()}")
            serialized.base64() equalsTo expected
            serializer.deserialize<Any>(serialized) equalsTo obj
        }
    }
}
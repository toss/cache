package im.toss.util.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import im.toss.test.equalsTo
import im.toss.util.data.serializer.KryoSerializer
import im.toss.util.reflection.getFieldValue
import im.toss.util.reflection.newClass
import im.toss.util.reflection.putFieldValue
import org.junit.jupiter.api.Test


class KryoSerializerTest {
    @Test
    fun `새 필드가 추가되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java)
        val person = personClass.newInstance()
        person.putFieldValue("name", "hello")
        person.putFieldValue("age", 16)

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java, "extraLong" to Long::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.getFieldValue("name") equalsTo person.getFieldValue("name")
        deserialized.getFieldValue("age") equalsTo person.getFieldValue("age")
    }

    @Test
    fun `기존 필드가 제거되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java)
        val person = personClass.newInstance()
        person.putFieldValue("name", "hello")
        person.putFieldValue("age", 16)
        person.putFieldValue("extra", "extraValue")

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.getFieldValue("name") equalsTo person.getFieldValue("name")
        deserialized.getFieldValue("age") equalsTo person.getFieldValue("age")
    }

    @Test
    fun `기존 필드의 이름이 변경되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java)
        val person = personClass.newInstance()
        person.putFieldValue("name", "hello")
        person.putFieldValue("age", 16)
        person.putFieldValue("extra", "extraValue")

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra2" to String::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.getFieldValue("name") equalsTo person.getFieldValue("name")
        deserialized.getFieldValue("age") equalsTo person.getFieldValue("age")
    }

    private fun kryoSerializer(type: Class<*>) = KryoSerializer(KryoSerializer.factoryBuilder().register(type).build())


    private fun Any.toJson(): String = objectMapper.writeValueAsString(this)

    private val objectMapper = ObjectMapper().registerKotlinModule()


    private fun ByteArray.toHexString() = joinToString("") { Integer.toUnsignedString(java.lang.Byte.toUnsignedInt(it), 16).padStart(2, '0') }
}
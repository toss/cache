package im.toss.util.data.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import im.toss.test.equalsTo
import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.DynamicType
import org.junit.jupiter.api.Test
import java.lang.reflect.Modifier


class KryoSerializerTest {
    @Test
    fun `새 필드가 추가되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java)
        val person = personClass.newInstance()
        person.put("name", "hello")
        person.put("age", 16)

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java, "extraLong" to Long::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.get("name") equalsTo person.get("name")
        deserialized.get("age") equalsTo person.get("age")
    }

    @Test
    fun `기존 필드가 제거되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java)
        val person = personClass.newInstance()
        person.put("name", "hello")
        person.put("age", 16)
        person.put("extra", "extraValue")

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.get("name") equalsTo person.get("name")
        deserialized.get("age") equalsTo person.get("age")
    }

    @Test
    fun `기존 필드의 이름이 변경되어도 예외없이 deserialize된다`() {
        // given
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java)
        val person = personClass.newInstance()
        person.put("name", "hello")
        person.put("age", 16)
        person.put("extra", "extraValue")

        val serialized = kryoSerializer(personClass).serialize(person)
        println("original: ${person.toJson()}")
        println("serialized: ${serialized.size}bytes -> \"${serialized.toHexString()}\"")

        // when
        val modifiedPersonClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra2" to String::class.java)
        val deserialized = kryoSerializer(modifiedPersonClass).deserialize<Any>(serialized)
        println("deserialized: ${deserialized.toJson()}")

        // then
        deserialized.get("name") equalsTo person.get("name")
        deserialized.get("age") equalsTo person.get("age")
    }

    private fun kryoSerializer(type: Class<*>) = KryoSerializer(KryoSerializer.factoryBuilder().register(type).build())

    private fun Any.put(name:String, value: Any?) = this::class.java.getField(name).set(this, value)

    private fun Any.get(name:String): Any? = this::class.java.getField(name).get(this)
    private fun Any.toJson(): String = objectMapper.writeValueAsString(this)

    private val objectMapper = ObjectMapper().registerKotlinModule()

    private fun newClass(className: String, vararg fields: Pair<String, Class<*>>): Class<*> {
        val typeBuilder = ByteBuddy()
            .subclass(Any::class.java)
            .name(className)

        var fieldBuilder: DynamicType.Builder.FieldDefinition.Optional.Valuable<*>? = null
        fields.forEach { (fieldName, fieldType) ->
            fieldBuilder = if (fieldBuilder == null) {
                typeBuilder.defineField(fieldName, fieldType, Modifier.PUBLIC)
            } else {
                fieldBuilder!!.defineField(fieldName, fieldType, Modifier.PUBLIC)
            }
        }

        return fieldBuilder!!
            .make()
            .load(javaClass.classLoader)
            .loaded
    }
    private fun ByteArray.toHexString() = joinToString("") { Integer.toUnsignedString(java.lang.Byte.toUnsignedInt(it), 16).padStart(2, '0') }
}
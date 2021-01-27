package im.toss.util.reflection

import im.toss.test.doesNotEqualTo
import im.toss.test.equalsTo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.lang.reflect.Type

internal class TypeDigestTest {
    @Test
    fun fieldTypeInfoTest() {
        val personClass = newClass("Person", "name" to String::class.java, "age" to Int::class.java, "extra" to String::class.java)
        val personType1 = TypeDigest().getOrAdd(personClass)
        val personClass2 = newClass("Person", "name" to String::class.java, "age" to Long::class.java, "extra" to String::class.java)
        val personType2 = TypeDigest().getOrAdd(personClass2)

        personType1 doesNotEqualTo personType2
    }

    @Test
    fun digestTest() {
        DigestMode.values().forEach { mode ->
            val digests = listOf(
                TypeDigest(mode).digest(newClass("person", "Age" to Int::class.java)),
                TypeDigest(mode).digest(newClass("person", "age" to Int::class.java)),
                TypeDigest(mode).digest(newClass("Person", "age" to Int::class.java)),
                TypeDigest(mode).digest(newClass("Person", "age" to Long::class.java)),
                TypeDigest(mode).digest(newClass("Person", "age" to Double::class.java)),
                TypeDigest(mode).digest(newClass("Person", "Age" to Int::class.java)),
                TypeDigest(mode).digest(newClass("Person", "age" to Int::class.java, "name" to String::class.java)),
                TypeDigest(mode).digest<Int>(),
                TypeDigest(mode).digest<Long>(),
                TypeDigest(mode).digest<String>()
            )

            println("$mode => $digests")
            digests.size equalsTo digests.toSet().size
        }
    }

    @Test
    fun environmentValueTest() {
        val javaVersion = System.getProperty("java.version")
        TypeDigest(
            DigestMode.SHORT, mapOf(
                "serializer.version" to "hello:1.0.0",
                "custom.value" to "cache"
            )
        ).environmentsValue equalsTo """
            custom.value=cache
            java.version=$javaVersion
            serializer.version=hello:1.0.0
        """.trimIndent()
    }

    @Test
    fun specificationWriteTest() {
        val javaVersion = System.getProperty("java.version")
        val personType = newClass("Person", "age" to Long::class.java)
        TypeDigest().getSpecification(personType).toString() equalsTo """
            [DEFINITION]
            class Person: java.lang.Object^1 {
            field age:long^1
            }
            [ENVIRONMENTS]
            java.version=$javaVersion
            [DEPENDENCIES]
            internal class java.lang.Object
            primitive long
            
        """.trimIndent()
    }

    @Test
    fun specificationWriteTest2() {
        val javaVersion = System.getProperty("java.version")
        TypeDigest(ignoreFieldNames = setOf("\$jacocoData")).getSpecification<Hello>().toString() equalsTo """
            [DEFINITION]
            class im.toss.util.reflection.Hello: java.lang.Object^H {
            field item:im.toss.util.reflection.HelloItem<java.lang.String>^I
            field list:java.util.List<java.lang.String>^I
            }
            [ENVIRONMENTS]
            java.version=$javaVersion
            [DEPENDENCIES]
            class im.toss.util.reflection.HelloItem: java.lang.Object^H {
            field<T> value:T^I
            }
            generic class im.toss.util.reflection.HelloItem<java.lang.String>
            internal class java.lang.Object
            internal class java.lang.String
            internal class java.util.List
            generic class java.util.List<java.lang.String>
            
        """.trimIndent()
    }


    @Test
    fun enumClassTest() {
        val typeDigest = TypeDigest()
        val type = typeDigest.getOrAdd<EnumClass>() as ClassInfo
        assertThat(type.fields.map { it.name }).contains("A", "B")
    }

    @Test
    fun simpleClassTest() {
        val typeDigest = TypeDigest()
        val type = typeDigest.getOrAdd<SimpleClass>() as ClassInfo
        assertThat(type.fields.map { it.name }).contains("a")
    }

    @Test
    fun classModifierTest() {
        val typeDigest = TypeDigest()
        val type1 = typeDigest.getOrAdd<SimpleClass>() as ClassInfo
        val type2 = typeDigest.getOrAdd<PrivateSimpleClass>() as ClassInfo
        val type3 = typeDigest.getOrAdd<AbstractSimpleClass>() as ClassInfo
        val type4 = typeDigest.getOrAdd<OpenSimpleClass>() as ClassInfo

        type1.modifiers.doesNotEqualTo(type2.modifiers)
        type1.modifiers.doesNotEqualTo(type3.modifiers)
        type1.modifiers.doesNotEqualTo(type4.modifiers)
        type2.modifiers.doesNotEqualTo(type3.modifiers)
        type2.modifiers.doesNotEqualTo(type4.modifiers)
        type3.modifiers.doesNotEqualTo(type4.modifiers)
    }

    @Test
    fun classFieldTest() {
        val typeDigest = TypeDigest()
        val type1 = typeDigest.getOrAdd<SimpleClass>() as ClassInfo
        val type2 = typeDigest.getOrAdd<VarClass>() as ClassInfo
        val type3 = typeDigest.getOrAdd<LateInitVarClass>() as ClassInfo

        val field1 = type1.fields.first()
        val field2 = type2.fields.first()
        val field3 = type3.fields.first()

        field1.doesNotEqualTo(field2)
        field1.doesNotEqualTo(field3)
        field2.doesNotEqualTo(field3)
    }

    @Test
    fun genericClassDependenciesTest() {
        val typeDigest = TypeDigest()
        val type = typeDigest.getOrAdd<List<Long>>() as GenericClassInfo
        type.rawType.equalsTo("java.util.List")
        type.typeArguments.equalsTo(listOf("? extends java.lang.Long"))

        typeDigest.getDependencies<List<Long>>() equalsTo setOf(
            "java.util.List<? extends java.lang.Long>",
            "? extends java.lang.Long",
            "java.util.List",
            "java.lang.Long"
        )
    }

    @TestFactory
    fun arrayTest(): List<DynamicTest> {
        data class Given(
            val type: Type,
            val expectedType: String,
            val expectedComponentType: String
        )
        return listOf(
            Given(BooleanArray::class.java, "boolean[]", "boolean"),
            Given(ByteArray::class.java, "byte[]", "byte"),
            Given(CharArray::class.java, "char[]", "char"),
            Given(ShortArray::class.java, "short[]", "short"),
            Given(IntArray::class.java, "int[]", "int"),
            Given(LongArray::class.java, "long[]", "long"),
            Given(FloatArray::class.java, "float[]", "float"),
            Given(DoubleArray::class.java, "double[]", "double"),
            Given(Array<Any>::class.java, "java.lang.Object[]", "java.lang.Object"),
            Given(Array<Int>::class.java, "java.lang.Integer[]", "java.lang.Integer"),
            Given(Array<Long>::class.java, "java.lang.Long[]", "java.lang.Long"),
            Given(
                Array<SimpleDataClass>::class.java,
                "im.toss.util.reflection.TypeDigestTest\$SimpleDataClass[]",
                "im.toss.util.reflection.TypeDigestTest\$SimpleDataClass"
            ),
            Given(
                Array<SimpleClass>::class.java,
                "im.toss.util.reflection.TypeDigestTest\$SimpleClass[]",
                "im.toss.util.reflection.TypeDigestTest\$SimpleClass"
            )
        ).map {
            it.run {
                dynamicTest("${type.typeName} -> type=$expectedType, component=$expectedComponentType") {
                    val typeDigest = TypeDigest()
                    val type = typeDigest.getOrAdd(type) as ArrayTypeInfo
                    type.type.equalsTo(expectedType)
                    type.componentType.equalsTo(expectedComponentType)
                }
            }
        }
    }

    enum class EnumClass {
        A, B
    }

    class SimpleClass {
        val a: String = ""
    }

    class VarClass {
        var a: String = ""
    }

    class LateInitVarClass {
        lateinit var a: String
    }

    private class PrivateSimpleClass {
        val a: String = ""
    }

    open class OpenSimpleClass {
        val a: String = ""
    }

    abstract class AbstractSimpleClass {
        val a: String = ""
    }

    data class SimpleDataClass(
        val name: String
    )

    class SimpleGenericClass<T>(
        val value: Long
    )
}

data class Hello(
    val item: HelloItem<String>,
    val list: List<String>
)

data class HelloItem<T>(
    val value: T
)
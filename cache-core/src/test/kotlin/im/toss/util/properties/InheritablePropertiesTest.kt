package im.toss.util.properties

import com.fasterxml.jackson.annotation.JsonProperty
import im.toss.test.equalsTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

internal class InheritablePropertiesTest {
    data class MyDto(
        @field:JsonProperty("required-property")
        val requiredProperty: String,
        @field:JsonProperty("name")
        val name: String?,
        @field:JsonProperty("duration")
        val duration: Duration? = null
    )

    @Test
    fun `instantiate simple properties`() {
        val properties = mapOf(
            "items.item1.required-property" to "value1",
            "items.item1.name" to "name1",
            "items.item1.duration" to "PT21S",
            "items.item2.required-property" to "value2",
            "items.item2.name" to "name2"
        )
        InheritableProperties(properties, "toss").run {
            instantiates<MyDto>("toss.items") equalsTo mapOf(
                "item1" to MyDto(requiredProperty = "value1", name = "name1", duration = Duration.ofSeconds(21L)),
                "item2" to MyDto(requiredProperty = "value2", name = "name2")
            )
        }
    }


    @Test
    fun `instantiate inherit properties same parent`() {
        val properties = mapOf(
            "items.item1.required-property" to "value1",
            "items.item1.name" to "name1",
            "items.item2.parent" to "item1",
            "items.item2.name" to "name2"
        )
        InheritableProperties(properties, "toss").run {
            instantiates<MyDto>("toss.items", "toss.items", "parent") equalsTo mapOf(
                "item1" to MyDto(requiredProperty = "value1", name = "name1"),
                "item2" to MyDto(requiredProperty = "value1", name = "name2")
            )
        }
    }

    @Test
    fun `instantiate inherit properties other parent`() {
        val properties = mapOf(
            "parents.item1.required-property" to "value1",
            "parents.item1.name" to "name1",
            "items.item2.parent" to "item1",
            "items.item2.name" to "name2"
        )
        InheritableProperties(properties, "toss").run {
            instantiates<MyDto>("toss.items", "toss.parents", "parent") equalsTo mapOf(
                "item2" to MyDto(requiredProperty = "value1", name = "name2")
            )
        }
    }


    @Test
    fun `circular reference detected when instantiate inherit properties` () {
        val properties = mapOf(
            "parents.p1.name" to "name1",
            "parents.p1.parent" to "p2",
            "parents.p2.name" to "name2",
            "parents.p2.parent" to "p3",
            "parents.p3.name" to "name3",
            "parents.p3.parent" to "p1",
            "parents.p3.required-property" to "req3",

            "items.item1.parent" to "p2"
        )
        InheritableProperties(properties, "toss").run {
            assertThrows<CircularReferenceException> {
                instantiates<MyDto>("toss.items", "toss.parents", "parent")
            }
        }
    }

    @Test
    fun `throw an exception when a required value is missing`() {
        val properties = mapOf(
            "items.item1.required-property" to "value1",
            "items.item1.name" to "name1",
            "items.item2.name" to "name2"
        )
        InheritableProperties(properties, "toss").run {
            assertThrows<MissingRequiredPropertyException> {
                instantiates<MyDto>("toss.items")
            }
        }
    }

    @Test
    fun `throw an exception when a invalid format`() {
        val properties = mapOf(
            "items.item1.required-property" to "value1",
            "items.item1.name" to "name1",
            "items.item1.duration" to "name1"
        )
        InheritableProperties(properties, "toss").run {
            assertThrows<InvalidPropertyFormatException> {
                instantiates<MyDto>("toss.items")
            }
        }
    }
}

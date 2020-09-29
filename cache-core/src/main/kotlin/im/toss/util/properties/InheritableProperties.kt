package im.toss.util.properties

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import java.lang.Exception

class InheritableProperties(
    properties: Map<String, String> = emptyMap(),
    prefix: String ?= null
) {
    private val root = mutableMapOf<String, Any>()

    init { putAll(properties, prefix) }

    fun putAll(properties: Map<String, String>, prefix: String? = null) = properties.entries.forEach { put(it.key, it.value, prefix) }
    fun put(propertyName: String, value: String, prefix: String? = null) = root.putByPath(if (prefix == null) propertyName else "$prefix.$propertyName", value)

    inline fun <reified T> instantiates(
        path: String,
        parentsPath: String = "",
        parentFieldName: String = ""
    ): Map<String, T> =
        inherit(path, parentsPath, parentFieldName)
            .associate { (key, value) ->
                key to value.instantiate<T>("$path.$key")
            }

    inline fun <reified T> Map<String, Any>.instantiate(key: String): T = try {
        inheritablePropertiesObjectMapper.convertValue(this)
    } catch (e: IllegalArgumentException) {
        e.cause.let { cause ->
            when (cause) {
                is InvalidFormatException -> throw InvalidPropertyFormatException(cause.propertyName(key), cause.value, e)
                is MissingKotlinParameterException -> throw MissingRequiredPropertyException(cause.propertyName(key), e)
                else -> throw e
            }
        }
    }

    fun JsonMappingException.propertyName(objectName: String) = path.joinToString(".", "$objectName.") { it.fieldName }

    fun inherit(
        path: String,
        parentsPath: String,
        parentFieldName: String
    ): List<Pair<String, Map<String, Any>>> {
        val objects = root.getMapByPath(path)
        val parents = root.getMapByPath(parentsPath)

        return objects
            ?.keys
            ?.mapNotNull { key ->
                objects.getMapByPath(key)
                    ?.inherit(parentFieldName, parents, mutableMapOf(), linkedSetOf(key))
                    ?.let { instance -> key to instance }
            }
            ?: emptyList()
    }

    private fun Map<String, Any>.inherit(
        parentFieldName: String,
        parents: Map<String, Any>?,
        output: MutableMap<String, Any>,
        inheritedParents: MutableSet<String>
    ): Map<String, Any> {
        getValueByPath(parentFieldName)?.let { parentName ->
            if (!inheritedParents.add(parentName)) {
                throw CircularReferenceException(inheritedParents.toList() + listOf(parentName))
            }
            parents?.getMapByPath(parentName)?.inherit(parentFieldName, parents, output, inheritedParents)
                ?: UnknownParentException(parentFieldName, parentName)
        }
        output.putAllFromMap(this)
        return output
    }
}

val inheritablePropertiesObjectMapper = ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(JavaTimeModule())
    .registerModule(ParameterNamesModule())
    .registerModule(Jdk8Module())
    .registerKotlinModule()

class UnknownParentException(parentFieldName: String, parentName: String): Exception("$parentFieldName: $parentName")
class InvalidPropertyFormatException(property: String, val value: Any, override val cause: Throwable): Exception("$value, $property", cause)
class MissingRequiredPropertyException(property: String, override val cause: Throwable): Exception(property, cause)
class CircularReferenceException(references: List<String>): Exception(references.joinToString(" -|> "))

private fun MutableMap<String, Any>.getOrNewMap(key: String): MutableMap<String, Any> {
    @Suppress("UNCHECKED_CAST")
    return computeIfAbsent(key) { mutableMapOf<String, Any>() } as MutableMap<String, Any>
}

private fun MutableMap<String, Any>.putByPath(path: String, value: String) {
    path.indexOf('.').let { sepIndex ->
        if (sepIndex < 0) this[path] = value
        else getOrNewMap(path.substring(0, sepIndex)).putByPath(
            path.substring(sepIndex + 1),
            value
        )
    }
}

private fun Map<String, Any>.getMapOrNull(key: String): Map<String, Any>? {
    @Suppress("UNCHECKED_CAST")
    return get(key) as? Map<String, Any>?
}

private fun Map<String, Any>.getValueByPath(path: String): String? =
    path.indexOf('.').let { sepIndex ->
        if (sepIndex < 0) get(path) as? String
        else getMapOrNull(path.substring(0, sepIndex))
            ?.getValueByPath(path.substring(sepIndex + 1))
    }

private fun Map<String, Any>.getMapByPath(path: String): Map<String, Any>? =
    path.indexOf('.').let { sepIndex ->
        if (sepIndex < 0) getMapOrNull(path)
        else getMapOrNull(path.substring(0, sepIndex))
            ?.getMapByPath(path.substring(sepIndex + 1))
    }

private fun MutableMap<String, Any>.putAllFromMap(map: Map<String, Any>) {
    map.forEach { (key, value) ->
        when (value) {
            is String -> this[key] = value
            is Map<*, *> -> @Suppress("UNCHECKED_CAST") (value as Map<String, Any>).let {
                    subMap -> getOrNewMap(key).putAllFromMap(subMap)
            }
        }
    }
}
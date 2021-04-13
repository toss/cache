package im.toss.util.reflection

import im.toss.util.data.encoding.i62.toI62
import im.toss.util.data.hash.sha1
import java.lang.Appendable
import java.lang.StringBuilder
import java.lang.reflect.*
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

class TypeDigest(
    val digestMode: DigestMode = DigestMode.SHORT,
    val environments: Map<String, String> = emptyMap(),
    private val ignoreFieldNames: Set<String> = emptySet() // don't use for production field
) {
    private val infoByType = ConcurrentHashMap<String, TypeInfo>()
    private val digestByTypeName = ConcurrentHashMap<String, String>()

    inline fun <reified T> digest(): String = digest(getType<T>())
    inline fun <reified T> getOrAdd(): TypeInfo = getOrAdd(getType<T>())
    inline fun <reified T> getDependencies(): Set<String> = getDependencies(getType<T>())
    inline fun <reified T> getSpecification() = getSpecification(getType<T>())


    fun digest(type: Type?): String {
        val typeInfo = getOrAdd(type)
        return digestByTypeName.computeIfAbsent(typeInfo.type) {
            val spec = getSpecification(type).toString()
            val hash = spec.toByteArray(Charsets.UTF_8).sha1()
            val buffer = ByteBuffer.wrap(hash)
            when (digestMode) {
                DigestMode.SHORT -> buffer.getInt(0).toI62()
                DigestMode.HALF -> buffer.getLong(12).toI62()
                DigestMode.FULL -> buffer.run { "${getLong(0).toI62()}${getLong(8).toI62()}${getInt(16).toI62()}" }
            }
        }
    }

    val environmentsValue: String by lazy {
        (environments + mapOf("java.version" to System.getProperty("java.version")))
            .entries
            .sortedBy { it.key }
            .joinToString("\n") { "${it.key}=${it.value}" }
    }

    fun getSpecification(type: Type?, appendable: Appendable = StringBuilder()): Appendable {
        val typeInfo = getOrAdd(type)
        return appendable.apply {
            appendLine("[DEFINITION]")
            typeInfo.write(appendable).appendln()
            appendLine("[ENVIRONMENTS]")
            appendLine(environmentsValue)
            appendLine("[DEPENDENCIES]")
            getDependencies(typeInfo.type)
                .sortedBy { it }
                .forEach {
                    if (it != typeInfo.type) {
                        get(it).write(appendable).appendln()
                    }
                }
        }
    }

    operator fun get(type: String?): TypeInfo {
        return type?.let { infoByType[it] } ?: NullTypeInfo
    }

    fun getOrAdd(type: Type?): TypeInfo {
        if (type == null) {
            return NullTypeInfo
        }

        val typeName = type.typeName
        val typeInfo = infoByType[typeName]
        if (typeInfo != null) {
            return typeInfo
        }

        if (type is TypeVariable<*>) {
            return GenericTypeVariableInfo(typeName)
        }

        val result = build(type).apply { add(this) }
        val depTypes = mutableSetOf<Type>().apply { deps(type, this) }
        depTypes.forEach { getOrAdd(it) }
        return result
    }

    fun getDependencies(type: Type?): Set<String> {
        return getDependencies(getOrAdd(type).type)
    }

    private fun getDependencies(typeName: String, result: MutableSet<String> = mutableSetOf()): Set<String> {
        val typeInfo = infoByType[typeName] ?: return result
        typeInfo.dependencies.forEach {
            if (result.add(it)) {
                getDependencies(it, result)
            }
        }
        return result
    }

    private fun add(typeInfo: TypeInfo): TypeInfo {
        infoByType[typeInfo.type] = typeInfo
        return typeInfo
    }

    private fun build(type: Type): TypeInfo {
        return when (type) {
            is GenericArrayType -> buildFromGenericArrayType(type)
            is ParameterizedType -> buildFromParameterizedType(type)
            is WildcardType -> buildFromWildcardType(type)
            is Class<*> -> buildFromClass(type)
            else -> UnknownTypeInfo(type.typeName, type.javaClass.typeName)
        }
    }

    private fun deps(type: Type, result: MutableSet<Type>) {
        when (type) {
            is GenericArrayType -> depsFromGenericArrayType(type, result)
            is ParameterizedType -> depsFromParameterizedType(type, result)
            is WildcardType -> depsFromWildcardType(type, result)
            is Class<*> -> depsFromClass(type, result)
        }
    }

    private fun buildFromGenericArrayType(type: GenericArrayType) = type.run {
        ArrayTypeInfo(typeName, genericComponentType.typeName)
    }

    private fun depsFromGenericArrayType(type: GenericArrayType, result: MutableSet<Type>) = type.run {
        result.add(genericComponentType)
    }

    private fun buildFromParameterizedType(type: ParameterizedType): TypeInfo = type.run {
        GenericClassInfo(
            typeName,
            rawType.typeName,
            actualTypeArguments.map { it.typeName }
        )
    }

    private fun depsFromParameterizedType(type: ParameterizedType, result: MutableSet<Type>) = type.run {
        result += rawType
        actualTypeArguments.forEach {
            result += it
        }
    }

    private fun buildFromWildcardType(type: WildcardType): TypeInfo = type.run {
        WildcardTypeInfo(
            typeName,
            upperBounds.map { it.typeName },
            lowerBounds.map { it.typeName }
        )
    }

    private fun depsFromWildcardType(type: WildcardType, result: MutableSet<Type>) = type.run {
        upperBounds.forEach {
            result += it
        }
        lowerBounds.forEach {
            result += it
        }
    }


    private fun buildFromClass(type: Class<*>): TypeInfo = type.run {
        if (isArray) {
            ArrayTypeInfo(typeName, componentType.typeName)
        } else {
            if (internalPackages.any { typeName.startsWith(it) }) {
                InternalClassInfo(typeName)
            } else {
                ClassInfo(
                    typeName,
                    modifiers,
                    declaredFields.mapNotNull {
                        it.run {
                            if (name in ignoreFieldNames) {
                                null
                            } else {
                                FieldInfo(name, modifiers, genericType.typeName, genericType is TypeVariable<*>)
                            }
                        }
                    },
                    superclass?.typeName ?: "null"
                )
            }
        }
    }

    private fun depsFromClass(type: Class<*>, result: MutableSet<Type>) = type.run {
        if (isArray) {
            result += componentType
        } else {
            if (internalPackages.any { typeName.startsWith(it) }) {
                // no op
            } else {
                result += superclass
                declaredFields.forEach {
                    result += it.genericType
                }
            }
        }
    }

    private val internalPackages = listOf(
        "java.lang.",
        "java.util.",
        "java.io.",
        "java.security.",
        "sun."
    )

    init {
        add(NullTypeInfo)
        listOf("boolean", "char", "short", "int", "long", "float", "double").forEach {
            add(PrimitiveTypeInfo(it))
        }
    }
}

interface TypeInfo {
    val type: String
    val isInternalType: Boolean get() = true
    val dependencies: Set<String>
    fun write(appendable: Appendable): Appendable = appendable.append(type)
}

data class PrimitiveTypeInfo(
    override val type: String
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type)
    override fun write(appendable: Appendable): Appendable = appendable.apply {
        append("primitive $type")
    }
}

data class ArrayTypeInfo(
    override val type: String,
    val componentType: String
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type, componentType)
}

data class InternalClassInfo(
    override val type: String
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type)

    override fun write(appendable: Appendable): Appendable = appendable.apply {
        append("internal class $type")
    }
}

object NullTypeInfo : TypeInfo {
    override val type: String = "null"
    override val dependencies: Set<String> = setOf(type)
}

data class UnknownTypeInfo(
    override val type: String,
    val classType: String
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type)
}

data class ClassInfo(
    override val type: String,
    val modifiers: Int,
    val fields: List<FieldInfo>,
    val superType: String
) : TypeInfo {
    override val isInternalType: Boolean get() = false

    override val dependencies: Set<String> = sequence {
        yield(superType)
        yield(type)
        fields.forEach {
            if (!it.isGenericType) {
                yield(it.type)
            }
        }
    }.toSet()

    override fun write(appendable: Appendable): Appendable = appendable.apply {
        appendLine("class $type: $superType^${modifiers.toI62().trimStart('0')} {")
        fields.forEach { field ->
            field.write(appendable)
            appendable.appendln()
        }
        append("}")
    }
}

data class FieldInfo(
    val name: String,
    val modifiers: Int,
    val type: String,
    val isGenericType: Boolean = false
) {
    fun write(appendable: Appendable): Appendable {
        return if (isGenericType) {
            appendable.append("field<$type> $name:$type^${modifiers.toI62().trimStart('0')}")
        } else {
            appendable.append("field $name:$type^${modifiers.toI62().trimStart('0')}")
        }
    }
}

data class GenericClassInfo(
    override val type: String,
    val rawType: String,
    val typeArguments: List<String>
) : TypeInfo {
    override val isInternalType: Boolean get() = false
    override val dependencies: Set<String> = setOf(type, rawType) + typeArguments

    override fun write(appendable: Appendable): Appendable = appendable.apply {
        append("generic class $type")
    }
}

data class WildcardTypeInfo(
    override val type: String,
    val upperBounds: List<String>,
    val lowerBounds: List<String>
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type) + upperBounds + lowerBounds
}

data class GenericTypeVariableInfo(
    override val type: String
) : TypeInfo {
    override val dependencies: Set<String> = emptySet()
}

enum class DigestMode {
    SHORT, HALF, FULL
}

private fun MessageDigest.update(value: String) {
    update(value.toByteArray(Charsets.UTF_8))
}
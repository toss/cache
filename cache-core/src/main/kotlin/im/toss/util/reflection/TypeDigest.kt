package im.toss.util.reflection

import im.toss.util.data.encoding.i62.toI62
import java.lang.reflect.*
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

class TypeDigest(
    val digestMode: DigestMode = DigestMode.SHORT
) {
    private val javaVersion = System.getProperty("java.version")
    private val infoByType = ConcurrentHashMap<String, TypeInfo>()
    private val digestByTypeName = ConcurrentHashMap<String, String>()

    inline fun <reified T> digest(): String {
        return digest((object : TypeReference<T>() {}).type)
    }

    inline fun <reified T> getOrAdd(): TypeInfo {
        return getOrAdd((object : TypeReference<T>() {}).type)
    }

    inline fun <reified T> getDependencies(): Set<String> {
        return getDependencies((object : TypeReference<T>() {}).type)
    }


    fun digest(type: Type?): String {
        val typeInfo = getOrAdd(type)
        return digestByTypeName.computeIfAbsent(typeInfo.type) {
            MessageDigest.getInstance("SHA1").run {
                update("<SYSTEM>")
                update("java=$javaVersion")

                update("<TYPE-DEFINITION>")
                typeInfo.digest(this)

                update("<DEPENDENCIES>")
                getDependencies(typeInfo.type)
                    .forEach {
                        get(it).digest(this)
                    }
                ByteBuffer.wrap(digest()).run {
                    when (digestMode) {
                        DigestMode.SHORT -> "${getInt(0).toI62()}"
                        DigestMode.HALF -> "${getLong(12).toI62()}"
                        DigestMode.FULL -> "${getLong(0).toI62()}${getLong(8).toI62()}${getInt(16).toI62()}"
                    }
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
                    declaredFields.map {
                        it.run {
                            FieldInfo(name, modifiers, genericType.typeName, genericType is TypeVariable<*>)
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
    fun digest(digest: MessageDigest) = digest.update(type)
}

data class PrimitiveTypeInfo(
    override val type: String
) : TypeInfo {
    override val dependencies: Set<String> = setOf(type)
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

    override fun digest(digest: MessageDigest) {
        digest.update("class($type, $superType, $modifiers)")
        fields.forEach { it.digest(digest) }
    }
}

data class FieldInfo(
    val name: String,
    val modifiers: Int,
    val type: String,
    val isGenericType: Boolean = false
) {
    fun digest(digest: MessageDigest) {
        if (isGenericType) {
            digest.update(".field($name,$modifiers,$type)")
        } else {
            digest.update(".field($name,$modifiers,$type,generic)")
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

abstract class TypeReference<T> {
    var type: Type? = (javaClass.genericSuperclass as ParameterizedType).actualTypeArguments[0]
}

enum class DigestMode {
    SHORT, HALF, FULL
}

private fun MessageDigest.update(value: String) {
    update(value.toByteArray(Charsets.UTF_8))
}
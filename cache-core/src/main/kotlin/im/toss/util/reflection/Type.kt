package im.toss.util.reflection

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

inline fun <reified T> getType(): Type? = (object : TypeReference<T>() {}).getType()

abstract class TypeReference<T> {
    fun getType(): Type? = (javaClass.genericSuperclass as ParameterizedType).actualTypeArguments[0]
}


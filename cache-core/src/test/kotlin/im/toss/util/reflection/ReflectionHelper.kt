package im.toss.util.reflection

import net.bytebuddy.ByteBuddy
import net.bytebuddy.dynamic.DynamicType
import java.lang.reflect.Modifier

object ReflectionHelper

fun newClass(className: String, vararg fields: Pair<String, Class<*>>): Class<*> {
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
        .load(ReflectionHelper.javaClass.classLoader)
        .loaded
}

fun Any.putFieldValue(name:String, value: Any?) = this::class.java.getField(name).set(this, value)
fun Any.getFieldValue(name:String): Any? = this::class.java.getField(name).get(this)
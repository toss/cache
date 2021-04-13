package im.toss.util.data.serializer

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.io.Input
import com.esotericsoftware.kryo.kryo5.io.Output
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy
import com.esotericsoftware.kryo.kryo5.serializers.CollectionSerializer
import com.esotericsoftware.kryo.kryo5.serializers.EnumNameSerializer
import com.esotericsoftware.kryo.kryo5.serializers.MapSerializer
import com.esotericsoftware.kryo.kryo5.serializers.TimeSerializers
import com.esotericsoftware.kryo.kryo5.util.DefaultInstantiatorStrategy
import com.esotericsoftware.kryo.kryo5.util.Pool
import im.toss.util.data.encoding.i62.toI62
import im.toss.util.data.hash.sha1
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.nio.ByteBuffer
import java.time.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap

class Kryo5Serializer(kryoFactory: KryoFactory) : Serializer {
    private val signature = kryoFactory.signature
    private val kryoPool = object : Pool<Kryo>(true, false, 16) {
        override fun create(): Kryo = kryoFactory()
    }
    private val outputPool = object : Pool<Output>(true, false, 16) {
        override fun create(): Output = Output(1024, -1)
    }
    private val inputPool = object : Pool<Input>(true, false, 16) {
        override fun create(): Input = Input(1024)
    }

    override val name: String get() = "kryo-5-${signature}"

    override fun <T> serialize(o: T): ByteArray {
        val kryo = kryoPool.obtain()
        val output = outputPool.obtain()
        val outputStream = ByteArrayOutputStream()
        output.outputStream = outputStream
        return try {
            kryo.writeClassAndObject(output, o)
            output.close()
            outputStream.toByteArray()
        } finally {
            outputPool.free(output.apply { reset() })
            kryoPool.free(kryo)
        }
    }

    override fun <T> deserialize(value: ByteArray): T {
        val kryo = kryoPool.obtain()
        val input = inputPool.obtain()
        return try {
            input.buffer = value
            @Suppress("UNCHECKED_CAST")
            kryo.readClassAndObject(input) as T
        } finally {
            inputPool.free(input.apply { reset() })
            kryoPool.free(kryo)
        }
    }

    companion object {
        fun factoryBuilder(): KryoFactoryBuilder {
            return KryoFactoryBuilder()
        }
    }

    interface KryoFactory {
        val signature: String
        operator fun invoke(): Kryo
    }

    class KryoFactoryBuilder {
        private val registeredSerializers = linkedMapOf<Class<*>, () -> com.esotericsoftware.kryo.kryo5.Serializer<*>>()
        private val registeredTypes = mutableListOf<Class<*>>()
        private val registeredEnumTypes = mutableListOf<Class<out Enum<*>>>()
        private val registeredCollectionTypes = mutableListOf<Class<*>>()
        private val registeredMapTypes = mutableListOf<Class<*>>()
        private val hashStream = ByteArrayOutputStream()
        private val hashWriter = OutputStreamWriter(hashStream)

        private fun write(description: String) {
            hashWriter.write("$description\n")
            hashWriter.flush()
        }

        fun register(type: Class<*>) = apply {
            registeredTypes.add(type)
            write("register: ${type.canonicalName}")
        }

        fun register(type: Class<*>, serializer: () -> com.esotericsoftware.kryo.kryo5.Serializer<*>) = apply {
            registeredSerializers[type] = serializer
            write("registerSerializer: ${type.name}, ${serializer().javaClass.name}")
        }

        fun registerEnum(type: Class<out Enum<*>>) = apply {
            registeredEnumTypes.add(type)
            write("registerEnum: ${type.name}")
        }

        fun registerCollection(type: Class<out Collection<*>>) = apply {
            registeredCollectionTypes.add(type)
            write("registerCollection: ${type.name}")
        }

        fun registerMap(type: Class<out Map<*, *>>) = apply {
            registeredMapTypes.add(type)
            write("registerMap: ${type.name}")
        }

        fun build(): KryoFactory {
            val bytes = hashStream.toByteArray()
            val hash = bytes.sha1()
            val buffer = ByteBuffer.wrap(hash)
            val signature = buffer.getInt(0).toI62()

            return object : KryoFactory {
                override val signature: String = signature

                override fun invoke(): Kryo =
                    Kryo().apply {
                        registeredSerializers.forEach { (type, serializer) -> register(type, serializer.invoke()) }
                        registeredEnumTypes.forEach { register(it, EnumNameSerializer(it)) }
                        registeredCollectionTypes.forEach { register(it, CollectionSerializer<Collection<*>>()) }
                        registeredMapTypes.forEach { register(it, MapSerializer<Map<*, *>>()) }
                        registeredTypes.forEach { register(it) }

                        instantiatorStrategy = DefaultInstantiatorStrategy(StdInstantiatorStrategy())
                        references = true
                    }
            }
        }

        init {
            fun registerDefaultArrayTypes() {
                register(Array::class.java)
                register(CharArray::class.java)
                register(ByteArray::class.java)
                register(ShortArray::class.java)
                register(IntArray::class.java)
                register(LongArray::class.java)
                register(BooleanArray::class.java)
                register(Array<String>::class.java)
                register(Array<Char>::class.java)
                register(Array<Byte>::class.java)
                register(Array<Short>::class.java)
                register(Array<Int>::class.java)
                register(Array<Long>::class.java)
                register(Array<Boolean>::class.java)
            }

            fun registerDefaultInternalTypes() {
                register(Class.forName("kotlin.collections.EmptyList"))
                register(Class.forName("kotlin.collections.EmptySet"))
                register(Class.forName("kotlin.collections.EmptyMap"))
                register(Class.forName("java.util.Arrays\$ArrayList"))
                register(Class.forName("java.util.Collections\$SingletonList"))
                register(Class.forName("java.util.Collections\$SingletonSet"))
                register(Class.forName("java.util.Collections\$SingletonMap"))
            }

            fun registerDefaultCollectionTypes() {
                registerCollection(ArrayList::class.java)
                registerCollection(TreeSet::class.java)
                registerCollection(HashSet::class.java)
                registerCollection(LinkedHashSet::class.java)
                registerCollection(LinkedList::class.java)
            }

            fun registerDefaultMapTypes() {
                registerMap(TreeMap::class.java)
                registerMap(HashMap::class.java)
                registerMap(LinkedHashMap::class.java)
            }

            fun registerJavaTimes() {
                register(Duration::class.java) { TimeSerializers.DurationSerializer() }
                register(Instant::class.java) { TimeSerializers.InstantSerializer() }
                register(LocalDate::class.java) { TimeSerializers.LocalDateSerializer() }
                register(LocalTime::class.java) { TimeSerializers.LocalTimeSerializer() }
                register(LocalDateTime::class.java) { TimeSerializers.LocalDateTimeSerializer() }
                register(ZoneOffset::class.java) { TimeSerializers.ZoneOffsetSerializer() }
                register(ZoneId::class.java) { TimeSerializers.ZoneIdSerializer() }
                register(OffsetTime::class.java) { TimeSerializers.OffsetTimeSerializer() }
                register(OffsetDateTime::class.java) { TimeSerializers.OffsetDateTimeSerializer() }
                register(ZonedDateTime::class.java) { TimeSerializers.ZonedDateTimeSerializer() }
                register(Year::class.java) { TimeSerializers.YearSerializer() }
                register(YearMonth::class.java) { TimeSerializers.YearMonthSerializer() }
                register(MonthDay::class.java) { TimeSerializers.MonthDaySerializer() }
                register(Period::class.java) { TimeSerializers.PeriodSerializer() }
            }

            registerDefaultArrayTypes()
            registerDefaultInternalTypes()
            registerDefaultCollectionTypes()
            registerDefaultMapTypes()
            registerJavaTimes()
        }
    }
}
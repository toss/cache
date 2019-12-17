package im.toss.util.data.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoFactory
import com.esotericsoftware.kryo.pool.KryoPool
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer
import com.esotericsoftware.kryo.serializers.EnumNameSerializer
import org.objenesis.strategy.StdInstantiatorStrategy
import java.util.ArrayList

private const val DEFAULT_BUFFER_SIZE = 8096
private const val DEFAULT_MAX_BUFFER_SIZE_UNLIMITED = -1

class KryoSerializer(
    private val kryoFactory: KryoFactory = KryoFactory {
        val kryo = Kryo()
        kryo.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(StdInstantiatorStrategy())
        kryo.setDefaultSerializer(CompatibleFieldSerializer::class.java)
        kryo.addDefaultSerializer(Enum::class.java, EnumNameSerializer::class.java)
        kryo
    },
    private var bufferSize: Int = DEFAULT_BUFFER_SIZE,
    private var maxBufferSize: Int = DEFAULT_MAX_BUFFER_SIZE_UNLIMITED
) : Serializer {
    private val kryoPool: KryoPool

    init {
        this.kryoPool = createKryoPool()
    }

    private fun createKryoPool(): KryoPool {
        return KryoPool.Builder(kryoFactory).softReferences().build()
    }

    override fun <T> serialize(o: T): ByteArray {
        return kryoPool.run { kryo ->
            val output = Output(bufferSize, maxBufferSize)
            kryo.writeClassAndObject(output, o)
            output.close()
            output.toBytes()
        }
    }

    override fun <T> deserialize(value: ByteArray): T {
        return kryoPool.run { kryo ->
            val input = Input(value)
            @Suppress("UNCHECKED_CAST")
            kryo.readClassAndObject(input) as T
        }
    }

    companion object {
        fun factoryBuilder(): KryoFactoryBuilder {
            return KryoFactoryBuilder()
        }
    }

    class KryoFactoryBuilder {
        private val registeredTypes = ArrayList<Class<*>>()

        fun register(type: Class<*>): KryoFactoryBuilder {
            registeredTypes.add(type)
            return this
        }

        fun build(): KryoFactory {
            return KryoFactory {
                val kryo = Kryo()
                registeredTypes.forEach { kryo.register(it, CompatibleFieldSerializer<Any>(kryo, it)) }
                kryo.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(StdInstantiatorStrategy())
                kryo.setDefaultSerializer(CompatibleFieldSerializer::class.java)
                kryo.addDefaultSerializer(Enum::class.java, EnumNameSerializer::class.java)
                kryo
            }
        }
    }
}
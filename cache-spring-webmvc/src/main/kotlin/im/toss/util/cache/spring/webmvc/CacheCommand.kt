package im.toss.util.cache.spring.webmvc

import im.toss.util.cache.blocking.BlockingCacheValueLoader
import im.toss.util.data.container.Pack
import org.springframework.web.context.request.RequestContextHolder

interface CacheCommand {
    val key: String get() = ""
    val field: String get() = ""
    val data: Pack? get() = null
    fun loadToCacheIfNeeds(block: () -> ByteArray?) {}
    fun fetchOriginIfNeeds(block: () -> Any?): Any?

    companion object {
        private val ATTRIBUTE_CACHE_COMMAND = CacheCommand::class.qualifiedName!!
        fun setCurrent(command: CacheCommand): CacheCommand =
            command.also {
                RequestContextHolder.currentRequestAttributes().setAttribute(ATTRIBUTE_CACHE_COMMAND, it, 0)
            }

        fun getCurrent(): CacheCommand =
            RequestContextHolder.currentRequestAttributes().run {
                (getAttribute(ATTRIBUTE_CACHE_COMMAND,  0) ?: Noop()) as CacheCommand
            }
    }

    class Noop : CacheCommand {
        override fun fetchOriginIfNeeds(block: () -> Any?): Any? = block()
    }

    class CachedResponse(
        override val key: String,
        override val field: String,
        override val data: Pack
    ): CacheCommand {
        override fun fetchOriginIfNeeds(block: () -> Any?): Any? = null
    }

    class LoadToCache(
        override val key: String,
        override val field: String,
        private val loader: BlockingCacheValueLoader<ByteArray>
    ): CacheCommand {
        override fun fetchOriginIfNeeds(block: () -> Any?): Any? = block()

        override fun loadToCacheIfNeeds(block: () -> ByteArray?) {
            val data = block()
            if (data == null) {
                loader.release()
            } else {
                loader.load(data)
            }
        }
    }
}

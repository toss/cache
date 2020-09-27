package im.toss

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import im.toss.util.cache.CacheNamespace
import im.toss.util.cache.CacheResourcesDsl
import im.toss.util.data.serializer.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder

@Configuration
class TestConfiguration {
    @Primary
    @Bean
    fun objectMapper(): ObjectMapper =
        Jackson2ObjectMapperBuilder.json().build<ObjectMapper>()
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)!!
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)!!
            .registerKotlinModule()
            .registerModule(Jdk8Module())
            .registerModule(JavaTimeModule())


    @Bean
    fun cacheResourceDefinition(): CacheResourcesDsl = {
        inMemory()
        namespace { CacheNamespace() }
        serializer { StringSerializer }
    }
}


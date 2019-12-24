package im.toss.util.cache.resources

import im.toss.util.cache.CacheResources
import im.toss.util.concurrent.lock.InMemoryMutexLock
import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.repository.*

class InMemoryCacheResources : CacheResources {
    private val repository = InMemoryRepository()

    override fun keyValueRepository(): KeyValueRepository {
        return InMemoryKeyValueRepository(repository)
    }

    override fun keyFieldValueRepository(): KeyFieldValueRepository {
        return InMemoryKeyFieldValueRepository(repository)
    }

    override fun lock(autoReleaseSeconds: Long): MutexLock {
        return InMemoryMutexLock(autoReleaseSeconds, repository)
    }
}
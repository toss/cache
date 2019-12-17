package im.toss.util.cache

import im.toss.util.concurrent.lock.MutexLock
import im.toss.util.repository.KeyFieldValueRepository
import im.toss.util.repository.KeyValueRepository

interface CacheResources {
    fun keyValueRepository(): KeyValueRepository
    fun keyFieldValueRepository(): KeyFieldValueRepository
    fun lock(autoReleaseSeconds: Long): MutexLock
}
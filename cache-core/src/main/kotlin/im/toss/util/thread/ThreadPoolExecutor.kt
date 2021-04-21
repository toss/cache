package im.toss.util.thread

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

private class NamedThreadFactory(val name: String) : ThreadFactory {
    private val group: ThreadGroup = System.getSecurityManager()?.threadGroup ?: Thread.currentThread().threadGroup
    private val threadIndex = AtomicInteger(1)
    override fun newThread(r: Runnable): Thread {
        val t = Thread(group, r, "$name-${threadIndex.incrementAndGet()}", 0)
        if (t.isDaemon) t.isDaemon = false
        if (t.priority != Thread.NORM_PRIORITY) t.priority = Thread.NORM_PRIORITY
        return t
    }
}

fun newThreadPoolExecutor(threadCount: Int, name: String): ThreadPoolExecutor =
    ThreadPoolExecutor(threadCount, threadCount,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory(name)
    )

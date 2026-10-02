package com.codex.remote.data.runtime

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicBoolean

/** A deadline/cancellation closes the resource, then waits for the blocking operation to unwind. */
internal suspend fun <T> blockingIo(
    timeoutMillis: Long,
    abort: () -> Unit,
    operation: () -> T,
): T = coroutineScope {
    val finished = AtomicBoolean(false)
    val expired = AtomicBoolean(false)
    val watchdog = launch(Dispatchers.IO, start = CoroutineStart.UNDISPATCHED) {
        try {
            delay(timeoutMillis)
            expired.set(true)
        } finally {
            if (!finished.get()) abort()
        }
    }
    try {
        val result = withContext(Dispatchers.IO) {
            try { operation() }
            finally { finished.set(true) }
        }
        if (expired.get()) throw SocketTimeoutException("SSH operation timed out")
        result
    } catch (error: Throwable) {
        if (expired.get()) throw SocketTimeoutException("SSH operation timed out").apply { initCause(error) }
        throw error
    } finally {
        finished.set(true)
        watchdog.cancelAndJoin()
    }
}

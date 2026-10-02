package com.codex.remote.data.runtime

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class BlockingIoTest {
    @Test fun deadlineClosesStuckIoAndWaitsForExitWithoutReplay() = runBlocking {
        val closed = CountDownLatch(1); val entered = CountDownLatch(1)
        val attempts = AtomicInteger(); var exited = false
        val result = runCatching { blockingIo(100, { closed.countDown() }) {
            attempts.incrementAndGet(); entered.countDown(); closed.await(); Thread.sleep(50); exited = true
        } }
        assertTrue(result.exceptionOrNull() is SocketTimeoutException)
        assertTrue(exited); assertEquals(1, attempts.get())
    }
    @Test fun cancellationClosesIoBeforeJoinAndNewDial() = runBlocking {
        val closed = CountDownLatch(1); val entered = CompletableDeferred<Unit>()
        var exited = false
        val job = launch { blockingIo(60_000, { closed.countDown() }) { entered.complete(Unit); closed.await(); Thread.sleep(50); exited = true } }
        entered.await(); withTimeout(2000) { job.cancelAndJoin() }
        assertTrue(exited)
    }
    @Test fun successfulOperationDoesNotAbortResource() = runBlocking {
        var aborted = false
        assertEquals(42, blockingIo(5000, { aborted = true }) { 42 })
        assertFalse(aborted)
    }

    @Test fun cancellingCompletedIoAwaitingCallerDispatchDoesNotCloseHealthySession() = runBlocking {
        val completed = CountDownLatch(1)
        val aborted = AtomicInteger()
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            blockingIo(60_000, { aborted.incrementAndGet() }) { completed.countDown(); 42 }
        }
        // Keep the caller event loop occupied while IO completes and queues its return.
        assertTrue(completed.await(2, java.util.concurrent.TimeUnit.SECONDS))
        Thread.sleep(100)
        job.cancelAndJoin()
        assertEquals(0, aborted.get())
    }

    @Test fun websocketAbortUnblocksWriterWithoutWaitingForItsWriteLock() = runBlocking {
        val released = CountDownLatch(1)
        val writes = AtomicInteger()
        val output = object : java.io.OutputStream() {
            override fun write(value: Int) = error("Unexpected byte write")
            override fun write(bytes: ByteArray, offset: Int, length: Int) {
                writes.incrementAndGet()
                released.await()
                throw java.io.IOException("Closed fixture output")
            }
            override fun close() { released.countDown() }
        }
        val socket = WebSocketOverStdio(java.io.ByteArrayInputStream(byteArrayOf()), output)
        val error = runCatching { blockingIo(100, socket::close) { socket.sendTextBlocking("once") } }.exceptionOrNull()
        assertTrue(error is SocketTimeoutException)
        assertEquals(1, writes.get())
    }
}

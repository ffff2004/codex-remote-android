package com.codex.remote.data.runtime

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.serialization.json.JsonObject
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Shared in-memory [AppServerSession] for transport-free tests.
 *
 * Messages and diagnostics can be scripted up front with [emit]/[emitDiagnostic] and completed explicitly
 * with [completeMessages]/[completeDiagnostics]; [responder] lets request/response protocols answer sends.
 */
class ChannelBackedAppServerSession(
    override val version: CodexRuntimeVersion,
    private val responder: (JsonObject) -> JsonObject? = { null },
) : AppServerSession {
    private val messageChannel = Channel<JsonObject>(Channel.UNLIMITED)
    private val diagnosticChannel = Channel<String>(Channel.BUFFERED)
    private val closed = AtomicBoolean(false)
    private val sent = mutableListOf<JsonObject>()

    override val messages: Flow<JsonObject> = messageChannel.receiveAsFlow()
    override val diagnostics: Flow<String> = diagnosticChannel.receiveAsFlow()

    override suspend fun send(message: JsonObject) {
        if (closed.get()) throw AppServerException.AppServerConnectionLost("session 已关闭")
        sent += message
        responder(message)?.let { messageChannel.trySend(it) }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        messageChannel.close()
        diagnosticChannel.close()
    }

    fun emit(message: JsonObject) {
        messageChannel.trySend(message)
    }

    fun emitDiagnostic(line: String) {
        diagnosticChannel.trySend(line)
    }

    fun completeMessages() {
        messageChannel.close()
    }

    fun completeDiagnostics() {
        diagnosticChannel.close()
    }

    fun failMessages(error: Throwable) {
        messageChannel.close(error)
    }

    fun sentMessages(): List<JsonObject> = sent.toList()
}

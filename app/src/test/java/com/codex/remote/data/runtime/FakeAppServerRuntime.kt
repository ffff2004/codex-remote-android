package com.codex.remote.data.runtime

import com.codex.remote.domain.ConnectionSecrets
import com.codex.remote.domain.SavedConnection
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.serialization.json.JsonObject
import java.util.concurrent.atomic.AtomicBoolean

/** In-memory [AppServerRuntime] used to pin down the session contract without SSH. */
class FakeAppServerRuntime(
    private val version: CodexRuntimeVersion = CodexRuntimeVersion("test-cli", "test-app-server"),
    private val scriptedMessages: List<JsonObject> = emptyList(),
    private val scriptedDiagnostics: List<String> = emptyList(),
) : AppServerRuntime {
    var openCount: Int = 0
        private set

    override suspend fun open(connection: SavedConnection, secrets: ConnectionSecrets): AppServerSession {
        openCount++
        return FakeAppServerSession(version, scriptedMessages, scriptedDiagnostics)
    }
}

class FakeAppServerSession(
    override val version: CodexRuntimeVersion,
    scriptedMessages: List<JsonObject>,
    scriptedDiagnostics: List<String>,
) : AppServerSession {
    private val messageChannel = Channel<JsonObject>(Channel.UNLIMITED)
    private val diagnosticChannel = Channel<String>(Channel.BUFFERED)
    private val closed = AtomicBoolean(false)
    private val sent = mutableListOf<JsonObject>()

    override val messages: Flow<JsonObject> = messageChannel.receiveAsFlow()
    override val diagnostics: Flow<String> = diagnosticChannel.receiveAsFlow()

    init {
        scriptedMessages.forEach { messageChannel.trySend(it) }
        messageChannel.close()
        scriptedDiagnostics.forEach { diagnosticChannel.trySend(it) }
        diagnosticChannel.close()
    }

    override suspend fun send(message: JsonObject) {
        if (closed.get()) throw AppServerException.AppServerConnectionLost("session 已关闭")
        sent += message
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        messageChannel.close()
        diagnosticChannel.close()
    }

    fun sentMessages(): List<JsonObject> = sent.toList()
}

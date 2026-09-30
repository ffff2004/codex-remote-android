package com.codex.remote.data.runtime

import com.codex.remote.domain.ConnectionSecrets
import com.codex.remote.domain.SavedConnection
import kotlinx.serialization.json.JsonObject

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
        return ChannelBackedAppServerSession(version).apply {
            scriptedMessages.forEach { emit(it) }
            completeMessages()
            scriptedDiagnostics.forEach { emitDiagnostic(it) }
            completeDiagnostics()
        }
    }
}

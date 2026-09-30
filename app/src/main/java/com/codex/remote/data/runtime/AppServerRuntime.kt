package com.codex.remote.data.runtime

import com.codex.remote.domain.ConnectionSecrets
import com.codex.remote.domain.SavedConnection
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.JsonObject
import java.io.Closeable

/**
 * Opens a live app-server session over an SSH connection.
 *
 * Implementations must return only fully live sessions: a session is either
 * usable (version known, Upgrade completed, message pump running) or `open`
 * throws.
 */
fun interface AppServerRuntime {
    suspend fun open(connection: SavedConnection, secrets: ConnectionSecrets): AppServerSession
}

interface AppServerSession : Closeable {
    /** Runtime versions reported by the remote daemon lifecycle JSON, fixed before [AppServerRuntime.open] returns. */
    val version: CodexRuntimeVersion

    /**
     * Channel-backed app-server messages in wire order. Intended for a single collector; the flow completes or
     * fails exactly once and never reconnects.
     */
    val messages: Flow<JsonObject>

    /** Best-effort remote stderr lines; lines may be dropped and protocol messages are never routed here. */
    val diagnostics: Flow<String>

    /** Serializes [message] onto the wire in call order. Throws after [close]. */
    suspend fun send(message: JsonObject)
}

data class CodexRuntimeVersion(val cli: String, val appServer: String)

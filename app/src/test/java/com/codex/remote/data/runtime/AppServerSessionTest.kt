package com.codex.remote.data.runtime

import com.codex.remote.domain.AuthType
import com.codex.remote.domain.ConnectionSecrets
import com.codex.remote.domain.SavedConnection
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class AppServerSessionTest {
    private val version = CodexRuntimeVersion(cli = "0.50.0", appServer = "0.50.1")

    @Test
    fun versionIsFixedBeforeOpenReturns() = runBlocking {
        val runtime = FakeAppServerRuntime(version)

        val session = runtime.open(connection(), ConnectionSecrets())

        assertEquals(version, session.version)
        assertEquals(version, session.version)
        assertEquals(1, runtime.openCount)
        session.close()
    }

    @Test
    fun messagesPreserveWireOrderAndCompleteExactlyOnce() = runBlocking {
        val scripted = (1..3).map(::message)

        val session = FakeAppServerRuntime(version, scriptedMessages = scripted)
            .open(connection(), ConnectionSecrets())

        assertEquals(scripted, session.messages.toList())
        assertTrue(session.messages.toList().isEmpty())
    }

    @Test
    fun diagnosticsAreBestEffortAndOrdered() = runBlocking {
        val session = FakeAppServerRuntime(
            version,
            scriptedDiagnostics = listOf("warning one", "warning two"),
        ).open(connection(), ConnectionSecrets())

        assertEquals(listOf("warning one", "warning two"), session.diagnostics.toList())
    }

    @Test
    fun sendAfterCloseThrows() = runBlocking {
        val session = FakeAppServerRuntime(version).open(connection(), ConnectionSecrets())
        val first = message(1)
        session.send(first)
        assertEquals(listOf(first), (session as FakeAppServerSession).sentMessages())

        session.close()
        val error = runCatching { session.send(message(2)) }.exceptionOrNull()

        assertTrue(error is AppServerException.AppServerConnectionLost)
    }

    @Test
    fun closeIsIdempotentAndDoesNotReconnect() = runBlocking {
        val runtime = FakeAppServerRuntime(version)
        val session = runtime.open(connection(), ConnectionSecrets())

        session.close()
        session.close()

        assertEquals(1, runtime.openCount)
    }

    private fun connection() = SavedConnection(
        name = "test",
        host = "example.com",
        username = "codex",
        authType = AuthType.PASSWORD,
    )

    private fun message(id: Int): JsonObject = buildJsonObject {
        put("id", id)
        put("method", "test/$id")
    }
}

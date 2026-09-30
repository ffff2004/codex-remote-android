package com.codex.remote.data.rpc

import com.codex.remote.data.runtime.ChannelBackedAppServerSession
import com.codex.remote.data.runtime.CodexRuntimeVersion
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.put
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * Pins [CodexRpcClient]'s public seam against an in-memory [AppServerSession]: JSON-RPC routing by id,
 * server notification routing, diagnostics and flow termination must not depend on the SSH transport.
 */
class CodexRpcClientRoutingTest {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @After
    fun tearDown() {
        scope.cancel()
    }

    @Test
    fun routesIdBasedResponsesAndInitializesFromSessionVersion() = runBlocking {
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("0.50.0", "0.50.1")) { request ->
            when (request.text("method")) {
                "initialize" -> successResponse(request, buildJsonObject {
                    put("userAgent", "codex/0.50.0")
                    put("codexHome", "/home/codex/.codex")
                    put("platformFamily", "unix")
                    put("platformOs", "linux")
                })
                "demo/echo" -> successResponse(request, buildJsonObject {
                    put("echo", (request["params"] as? JsonObject)?.text("marker"))
                })
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        try {
            val info = client.initialize()
            assertEquals("0.50.0", info.codexVersion)
            assertEquals("unix", info.platformFamily)
            assertEquals("linux", info.platformOs)

            val result = client.request("demo/echo", buildJsonObject { put("marker", "hi") })

            assertEquals("hi", result.text("echo"))
            assertEquals(
                listOf("initialize", "initialized", "demo/echo"),
                session.sentMessages().map { it.text("method") },
            )
        } finally {
            client.close()
        }
    }

    @Test
    fun fallsBackToAppServerVersionAndUnknownPlatform() = runBlocking {
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("", "9.9.9")) { request ->
            if (request.text("method") == "initialize") successResponse(request, buildJsonObject {}) else null
        }
        val client = CodexRpcClient(session)
        try {
            val info = client.initialize()

            assertEquals("9.9.9", info.codexVersion)
            assertEquals("unknown", info.platformFamily)
            assertEquals("unknown", info.platformOs)
        } finally {
            client.close()
        }
    }

    @Test
    fun routesServerNotificationsToEvents() = runBlocking {
        val session = initializingSession()
        val client = CodexRpcClient(session)
        try {
            val events = scope.collectEvents(client)
            client.initialize()

            session.emit(buildJsonObject {
                put("method", "turn/started")
                put("params", buildJsonObject {
                    put("threadId", "thread-1")
                    put("turn", buildJsonObject { put("id", "turn-1") })
                })
            })

            val event = events.next()
            assertTrue(event is AppServerEvent.TurnRunning)
            event as AppServerEvent.TurnRunning
            assertEquals("thread-1", event.threadId)
            assertEquals("turn-1", event.turnId)
            assertTrue(event.running)
        } finally {
            client.close()
        }
    }

    @Test
    fun routesDiagnosticsToDiagnosticEventsAndSkipsBlankLines() = runBlocking {
        val session = initializingSession()
        val client = CodexRpcClient(session)
        try {
            val events = scope.collectEvents(client)
            client.initialize()

            session.emitDiagnostic("   ")
            session.emitDiagnostic("warning: disk full")

            assertEquals(AppServerEvent.Diagnostic("warning: disk full"), events.next())
        } finally {
            client.close()
        }
    }

    @Test
    fun messageFlowCompletionEmitsDisconnectedFailure() = runBlocking {
        val session = initializingSession()
        val client = CodexRpcClient(session)
        try {
            val events = scope.collectEvents(client)
            client.initialize()

            session.completeMessages()

            assertEquals(AppServerEvent.Failure("远端 app-server 已断开"), events.next())
        } finally {
            client.close()
        }
    }

    @Test
    fun messageFlowFailureEmitsFailureWithMessage() = runBlocking {
        val session = initializingSession()
        val client = CodexRpcClient(session)
        try {
            val events = scope.collectEvents(client)
            client.initialize()

            session.failMessages(IllegalStateException("boom"))

            assertEquals(AppServerEvent.Failure("boom"), events.next())
        } finally {
            client.close()
        }
    }

    private fun initializingSession(): ChannelBackedAppServerSession =
        ChannelBackedAppServerSession(CodexRuntimeVersion("0.50.0", "0.50.1")) { request ->
            if (request.text("method") == "initialize") successResponse(request, buildJsonObject {}) else null
        }

    private fun successResponse(request: JsonObject, result: JsonObject): JsonObject = buildJsonObject {
        put("id", request.getValue("id"))
        put("result", result)
    }

    private fun CoroutineScope.collectEvents(client: CodexRpcClient): Channel<AppServerEvent> {
        val events = Channel<AppServerEvent>(Channel.UNLIMITED)
        launch(start = CoroutineStart.UNDISPATCHED) {
            client.events.collect { events.send(it) }
        }
        return events
    }

    private suspend fun Channel<AppServerEvent>.next(): AppServerEvent = withTimeout(5_000) { receive() }
}

private fun JsonObject.text(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

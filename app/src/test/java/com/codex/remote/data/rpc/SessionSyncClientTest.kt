package com.codex.remote.data.rpc

import com.codex.remote.data.runtime.ChannelBackedAppServerSession
import com.codex.remote.data.runtime.CodexRuntimeVersion
import com.codex.remote.domain.TimelineKind
import com.codex.remote.session.ResumeEventCoordinator
import com.codex.remote.session.ResumeEventDrainDisposition
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.json.*
import org.junit.Assert.*
import org.junit.Test

class SessionSyncClientTest {
    @Test fun resumeBufferKeepsReaderLiveForMoreThanSharedFlowCapacity() = runBlocking {
        val resumes = Channel<JsonObject>(Channel.UNLIMITED)
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { request ->
            when (request["method"]?.jsonPrimitive?.content) {
                "initialize" -> reply(request, buildJsonObject {})
                "thread/resume" -> { resumes.trySend(request); null }
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        val coordinator = ResumeEventCoordinator<AppServerEvent>(300)
        val epoch = coordinator.beginResume()
        val applied = mutableListOf<String>()
        val collector = launch(start = CoroutineStart.UNDISPATCHED) {
            client.events.collect { event -> coordinator.processOrBuffer(event) {
                applied += (it as AppServerEvent.AgentDelta).delta
            } }
        }
        try {
            client.initialize()
            val resume = async { client.resumeThread("a", "/a") }
            val request = withTimeout(2000) { resumes.receive() }
            repeat(256) { index -> session.emit(notification("item/agentMessage/delta", buildJsonObject {
                put("threadId", "a"); put("turnId", "t"); put("itemId", "i"); put("delta", index.toString())
            })) }
            session.emit(reply(request, buildJsonObject {
                put("initialTurnsPage", buildJsonObject { put("data", buildJsonArray {}) })
            }))
            withTimeout(2000) { resume.await() }
            assertTrue(applied.isEmpty())
            assertEquals(ResumeEventDrainDisposition.DRAINED, coordinator.drainAfterResume(epoch,
                applySnapshot = { applied += "snapshot" }, applyEvent = { applied += (it as AppServerEvent.AgentDelta).delta }))
            withTimeout(2000) { while (applied.size != 257) yield() }
            assertEquals(listOf("snapshot") + (0 until 256).map(Int::toString), applied)
        } finally { client.close(); collector.cancelAndJoin() }
    }

    @Test fun sendAndSteerCarryOriginIdsAndParseExactReturnedTurnWithoutReplay() = runBlocking {
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { request ->
            when (request["method"]?.jsonPrimitive?.content) {
                "initialize" -> reply(request, buildJsonObject {})
                "turn/start" -> reply(request, buildJsonObject { put("turn", buildJsonObject { put("id", "t") }) })
                "turn/steer" -> reply(request, buildJsonObject { put("turnId", "t") })
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        try {
            client.initialize()
            assertEquals("t", client.startTurn("a", "same", "/a", "model", null, null, "on-request", "user",
                ":workspace", null, clientUserMessageId = "local-start"))
            assertEquals("t", client.steerTurn("a", "t", "same", clientUserMessageId = "local-steer"))
            val writes = session.sentMessages().filter { it["method"]?.jsonPrimitive?.content in setOf("turn/start", "turn/steer") }
            assertEquals(listOf("local-start", "local-steer"), writes.map { it["params"]!!.jsonObject["clientUserMessageId"]!!.jsonPrimitive.content })
            assertEquals("t", writes.last()["params"]!!.jsonObject["expectedTurnId"]!!.jsonPrimitive.content)
            assertEquals(0, client.pendingRequestCount)
        } finally { client.close() }
        assertEquals(2, session.sentMessages().count { it["method"]?.jsonPrimitive?.content in setOf("turn/start", "turn/steer") })
    }

    @Test fun failedWriteIsSentOnceAndNeverRetried() = runBlocking {
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { request ->
            when (request["method"]?.jsonPrimitive?.content) {
                "initialize" -> reply(request, buildJsonObject {})
                "turn/start" -> buildJsonObject { put("id", request.getValue("id")); put("error", buildJsonObject { put("message", "uncertain") }) }
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        try {
            client.initialize()
            assertTrue(runCatching { client.startTurn("a", "same", "/a", "model", null, null,
                "on-request", "user", ":workspace", null, clientUserMessageId = "local") }.isFailure)
            assertEquals(1, session.sentMessages().count { it["method"]?.jsonPrimitive?.content == "turn/start" })
        } finally { client.close() }
    }

    @Test fun activeTurnAndOriginIdentityAreParsedFromSnapshotEvenWhenItemsViewIsEmpty() = runBlocking {
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { request ->
            when (request["method"]?.jsonPrimitive?.content) {
                "initialize" -> reply(request, buildJsonObject {})
                "thread/resume" -> reply(request, Json.parseToJsonElement("""{"thread":{"status":{"type":"active"}},"initialTurnsPage":{"data":[{"id":"t","status":"inProgress","items":[]}],"nextCursor":"older"}}""").jsonObject)
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        try {
            client.initialize()
            val snapshot = client.resumeThread("a", "/a")
            assertEquals("t", snapshot.activeTurnId)
            assertTrue(snapshot.isTurnRunning)
            assertTrue(snapshot.timeline.isEmpty())
            val user = CodexRpcClient.parseTimelineItem(Json.parseToJsonElement("""{"id":"u","type":"userMessage","clientId":"local-phone","content":[{"type":"text","text":"same"}]}"""))!!
            assertEquals(TimelineKind.USER, user.kind)
            assertEquals("local-phone", user.clientId)
            assertEquals("same", user.body)
        } finally { client.close() }
    }

    private fun reply(request: JsonObject, result: JsonObject) = buildJsonObject {
        put("id", request.getValue("id")); put("result", result)
    }
    private fun notification(method: String, params: JsonObject) = buildJsonObject {
        put("method", method); put("params", params)
    }
}

package com.codex.remote.data.rpc

import com.codex.remote.data.runtime.ChannelBackedAppServerSession
import com.codex.remote.data.runtime.CodexRuntimeVersion
import com.codex.remote.domain.ApprovalKind
import com.codex.remote.domain.ApprovalRequest
import com.codex.remote.domain.RpcRequestId
import com.codex.remote.domain.canRespond
import kotlinx.coroutines.async
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.*
import org.junit.Assert.*
import org.junit.Test

class ApprovalClientBehaviorTest {
    @Test
    fun numericAndTextIdsWithTheSameValueAreIndependentAndReturnedLosslessly() = runBlocking {
        val session = session()
        val client = CodexRpcClient(session)
        val events = Channel<AppServerEvent>(Channel.UNLIMITED)
        val subscription = launch(start = CoroutineStart.UNDISPATCHED) { client.events.collect { events.send(it) } }
        try {
            client.initialize()
            val ids = listOf(JsonPrimitive("1"), JsonPrimitive(1), Json.parseToJsonElement("123456789012345678901234567890"), Json.parseToJsonElement("1.50"))
            ids.forEach { id -> session.emit(command(id)) }
            val requests = ids.map { withTimeout(2_000) { (events.receive() as AppServerEvent.Approval).request } }
            assertEquals(4, requests.map { it.requestId }.toSet().size)
            requests.forEach { client.respondToApproval(it, "accept") }
            val replies = session.sentMessages().filter { it.containsKey("result") }
            assertEquals(ids, replies.map { it["id"] })
            assertTrue(runCatching { client.respondToApproval(requests.first(), "accept") }.isFailure)
            assertEquals(4, session.sentMessages().count { it.containsKey("result") })
        } finally { client.close(); subscription.cancel() }
    }

    @Test
    fun changedUiCommandCannotBypassTheFrozenRpcReview() = runBlocking {
        val session = session()
        val client = CodexRpcClient(session)
        val events = Channel<AppServerEvent>(Channel.UNLIMITED)
        val subscription = launch(start = CoroutineStart.UNDISPATCHED) { client.events.collect { events.send(it) } }
        try {
            client.initialize()
            session.emit(command(JsonPrimitive("request")))
            val approval = withTimeout(2_000) { (events.receive() as AppServerEvent.Approval).request }
            assertTrue(runCatching { client.respondToApproval(approval.copy(detail = "rm -rf /"), "accept") }.isFailure)
            assertEquals(0, session.sentMessages().count { it.containsKey("result") })
            client.respondToApproval(approval, "decline")
            assertEquals("decline", session.sentMessages().last().jsonObject["result"]!!.jsonObject["decision"]!!.jsonPrimitive.content)
        } finally { client.close(); subscription.cancel() }
    }

    @Test
    fun exactLivePatchIsFrozenAndLateCompletionCannotAuthorizeTheOldSnapshot() = runBlocking {
        val session = session()
        val client = CodexRpcClient(session)
        val events = Channel<AppServerEvent>(Channel.UNLIMITED)
        val subscription = launch(start = CoroutineStart.UNDISPATCHED) { client.events.collect { events.send(it) } }
        try {
            client.initialize()
            val live = Json.parseToJsonElement("""{"method":"item/started","params":{"threadId":"thread-a","turnId":"turn-a","item":{"type":"fileChange","id":"patch-a","status":"inProgress","changes":[{"path":"a.kt","kind":{"type":"update","move_path":"b.kt"},"diff":"-old\n+new"}]}}}""").jsonObject
            session.emit(live)
            withTimeout(2_000) { events.receive() }
            session.emit(Json.parseToJsonElement("""{"id":"file-approval","method":"item/fileChange/requestApproval","params":{"threadId":"thread-a","turnId":"turn-a","itemId":"patch-a","cwd":"/srv/app","availableDecisions":["accept","decline"]}}""").jsonObject)
            val approval = withTimeout(2_000) { (events.receive() as AppServerEvent.Approval).request }
            assertTrue(approval.canRespond("thread-a", "accept"))
            assertEquals("b.kt", approval.fileChanges.single().movePath)
            val params = live["params"]!!.jsonObject
            val completedItem = JsonObject(params["item"]!!.jsonObject + ("status" to JsonPrimitive("completed")))
            session.emit(buildJsonObject { put("method", "item/completed"); put("params", JsonObject(params + ("item" to completedItem))) })
            val completion = withTimeout(2_000) { events.receive() as AppServerEvent.ItemUpsert }
            assertTrue(runCatching { client.respondToApproval(approval, "accept") }.isFailure)
            val invalidated = approval.bindFileChangesSnapshot("thread-a", completion.item)
            assertFalse(invalidated.canRespond("thread-a", "accept"))
            client.respondToApproval(invalidated, "decline")
            assertEquals(1, session.sentMessages().count { it.containsKey("result") })
        } finally { client.close(); subscription.cancel() }
    }

    @Test
    fun lateOwnerReadAfterResolutionDoesNotBindTheNewApprovalOrBlockTheReader() = runBlocking {
        val reads = Channel<JsonObject>(Channel.UNLIMITED)
        val session = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { message ->
            when (message["method"]?.jsonPrimitive?.content) {
                "initialize" -> buildJsonObject { put("id", message["id"]!!); put("result", buildJsonObject {}) }
                "thread/read" -> { reads.trySend(message); null }
                else -> null
            }
        }
        val client = CodexRpcClient(session)
        val events = Channel<AppServerEvent>(Channel.UNLIMITED)
        val subscription = launch(start = CoroutineStart.UNDISPATCHED) { client.events.collect { events.send(it) } }
        try {
            client.initialize()
            fun fileRequest(id: String, owner: String) = Json.parseToJsonElement("""{"id":"$id","method":"item/fileChange/requestApproval","params":{"threadId":"$owner","turnId":"turn-a","itemId":"patch-a"}}""").jsonObject
            session.emit(fileRequest("A", "thread-a"))
            val a = withTimeout(2_000) { (events.receive() as AppServerEvent.Approval).request }
            assertNull(a.cwd)
            val read = async { client.request("thread/read", buildJsonObject { put("threadId", "thread-a") }) }
            val call = withTimeout(2_000) { reads.receive() }
            session.emit(Json.parseToJsonElement("""{"method":"serverRequest/resolved","params":{"threadId":"thread-a","requestId":"A"}}""").jsonObject)
            assertTrue(withTimeout(2_000) { events.receive() } is AppServerEvent.ApprovalResolved)
            session.emit(fileRequest("B", "thread-b"))
            val b = withTimeout(2_000) { (events.receive() as AppServerEvent.Approval).request }
            session.emit(buildJsonObject { put("id", call["id"]!!); put("result", buildJsonObject {
                put("thread", buildJsonObject { put("id", "thread-a"); put("cwd", "/srv/a") })
            }) })
            withTimeout(2_000) { read.await() }
            assertNull(b.cwd)
            assertFalse(b.canRespond("thread-b", "accept"))
            assertTrue(events.tryReceive().isFailure)
            client.respondToApproval(b, "decline")
            assertEquals(JsonPrimitive("B"), session.sentMessages().last()["id"])
        } finally { client.close(); subscription.cancel() }
    }

    @Test
    fun additiveMetadataIsCompatibleButUnknownAuthorizationAndOversizedReviewBlockAllow() {
        val base = command(JsonPrimitive("a"))["params"]!!.jsonObject
        val compatible = CodexRpcClient.parseApprovalRequest(RpcRequestId.Text("a"), "item/commandExecution/requestApproval",
            JsonObject(base + ("metadata" to buildJsonObject { put("trace", "abc") })))
        assertTrue(compatible.canRespond("thread-a", "accept"))
        assertFalse(compatible.canRespond("thread-b", "accept"))
        assertTrue(compatible.canRespond("thread-b", "decline"))
        assertFalse(compatible.copy(threadId = null).canRespond("thread-a", "accept"))
        assertTrue(compatible.copy(threadId = null).canRespond(null, "decline"))
        val unknownScope = CodexRpcClient.parseApprovalRequest(RpcRequestId.Text("a"), "item/commandExecution/requestApproval",
            JsonObject(base + ("allowAllDirectories" to JsonPrimitive(true))))
        assertFalse(unknownScope.canRespond("thread-a", "accept"))
        assertTrue(unknownScope.canRespond(null, "decline"))
        val huge = CodexRpcClient.parseApprovalRequest(RpcRequestId.Text("a"), "item/commandExecution/requestApproval",
            JsonObject(base + ("command" to JsonPrimitive("x".repeat(32_769)))))
        assertFalse(huge.canRespond("thread-a", "accept"))
        assertTrue(huge.canRespond(null, "decline"))
    }

    private fun session() = ChannelBackedAppServerSession(CodexRuntimeVersion("test", "test")) { message ->
        if (message["method"]?.jsonPrimitive?.content == "initialize") buildJsonObject {
            put("id", message["id"]!!); put("result", buildJsonObject {})
        } else null
    }

    private fun command(id: JsonElement) = buildJsonObject {
        put("id", id); put("method", "item/commandExecution/requestApproval")
        put("params", buildJsonObject {
            put("threadId", "thread-a"); put("turnId", "turn-a"); put("itemId", "command-a")
            put("command", "git status"); put("cwd", "/srv/app")
            put("availableDecisions", buildJsonArray { add("accept"); add("decline") })
        })
    }
}

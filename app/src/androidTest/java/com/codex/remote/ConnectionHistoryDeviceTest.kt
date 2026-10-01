package com.codex.remote

import android.app.Application
import androidx.lifecycle.ViewModelStore
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.codex.remote.data.runtime.AppServerRuntime
import com.codex.remote.data.runtime.AppServerSession
import com.codex.remote.data.runtime.CodexRuntimeVersion
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.domain.ConnectionDraft
import com.codex.remote.domain.ConnectionStatus
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.atomic.AtomicInteger

@RunWith(AndroidJUnit4::class)
class ConnectionHistoryDeviceTest {
    @Test fun firstPageConnectsWhileSecondPageIsBlocked() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = withTimeout(2000) { session.tailRequests.receive() }
            val first = withTimeout(1000) { vm.state.first { it.connectionStatus == ConnectionStatus.CONNECTED } }
            assertEquals(listOf("recent"), first.threads.map { it.id })
            assertTrue(first.isThreadsLoading)
            session.respond(tail, page("older", null, "recent"))
            val complete = withTimeout(2000) { vm.state.first { it.threads.size == 2 && !it.isThreadsLoading } }
            assertEquals(setOf("recent", "older"), complete.threads.map { it.id }.toSet())
        }
    }

    @Test fun tailFailureKeepsConnectionAndCanRetry() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = withTimeout(2000) { session.tailRequests.receive() }
            session.fail(tail)
            val partial = withTimeout(2000) { vm.state.first { it.threadsError != null && !it.isThreadsLoading } }
            assertEquals(ConnectionStatus.CONNECTED, partial.connectionStatus)
            assertEquals(listOf("recent"), partial.threads.map { it.id })
            onMain { vm.retryThreads() }
            val retry = withTimeout(2000) { session.tailRequests.receive() }
            assertEquals(null, vm.state.value.threadsError)
            session.respond(retry, page("older", null))
            withTimeout(2000) { vm.state.first { it.threads.size == 2 && !it.isThreadsLoading } }
        }
    }

    @Test fun stalePagesDoNotUndoLocalRenameOrArchive() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = withTimeout(2000) { session.tailRequests.receive() }
            onMain { vm.renameThread(vm.state.value.threads.single(), "Renamed") }
            withTimeout(2000) { vm.state.first { it.threads.singleOrNull()?.title == "Renamed" } }
            session.respond(tail, page("older", "last", "recent"))
            val last = withTimeout(2000) { session.tailRequests.receive() }
            assertEquals("Renamed", vm.state.value.threads.first { it.id == "recent" }.title)
            onMain { vm.archiveThread(vm.state.value.threads.first { it.id == "recent" }) }
            withTimeout(2000) { vm.state.first { it.threads.none { t -> t.id == "recent" } } }
            session.respond(last, page("recent", null))
            val complete = withTimeout(2000) { vm.state.first { !it.isThreadsLoading } }
            assertEquals(listOf("older"), complete.threads.map { it.id })
        }
    }

    @Test fun disconnectAndSwitchDiscardOldHistory() = runBlocking<Unit> {
        val old = HistorySession()
        val replacement = HistorySession("replacement")
        withViewModel(listOf(old, replacement)) { vm, _ ->
            val oldTail = withTimeout(2000) { old.tailRequests.receive() }
            val next = vm.state.value.activeConnection!!.copy(id = "replacement", name = "Replacement")
            onMain { vm.connect(next) }
            withTimeout(2000) { replacement.tailRequests.receive() }
            val connected = withTimeout(2000) { vm.state.first {
                it.connectionStatus == ConnectionStatus.CONNECTED && it.activeConnection?.id == "replacement"
            } }
            old.respond(oldTail, page("stale", null))
            assertEquals(listOf("replacement"), connected.threads.map { it.id })
            onMain { vm.disconnect() }
            val disconnected = withTimeout(2000) { vm.state.first { it.connectionStatus == ConnectionStatus.DISCONNECTED } }
            assertTrue(disconnected.threads.isEmpty())
            assertFalse(disconnected.isThreadsLoading)
            assertEquals(null, disconnected.threadsError)
        }
    }

    @Test fun refreshesDuringImportCoalesceIntoOneFreshPass() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = withTimeout(2000) { session.tailRequests.receive() }
            onMain { vm.retryThreads(); vm.retryThreads() }
            assertEquals(1, session.firstPageRequests.get())
            session.respond(tail, page("older", null))
            val refreshTail = withTimeout(2000) { session.tailRequests.receive() }
            assertEquals(2, session.firstPageRequests.get())
            session.respond(refreshTail, page("older", null))
            withTimeout(2000) { vm.state.first { !it.isThreadsLoading } }
            assertEquals(2, session.firstPageRequests.get())
        }
    }

    @Test fun importingHistoryPreservesOpenConversationAndHistoryCursor() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = withTimeout(2000) { session.tailRequests.receive() }
            onMain { vm.selectThread(vm.state.value.threads.single()) }
            val opened = withTimeout(2000) { vm.state.first { it.timeline.isNotEmpty() && !it.isBusy } }
            session.respond(tail, page("older", null))
            val complete = withTimeout(2000) { vm.state.first { !it.isThreadsLoading } }
            assertEquals(opened.selectedThreadId, complete.selectedThreadId)
            assertEquals(opened.timeline, complete.timeline)
            assertEquals("older-turns", complete.olderHistoryCursor)
            assertTrue(complete.hasOlderHistory)
        }
    }

    private fun onMain(action: () -> Unit) = InstrumentationRegistry.getInstrumentation().runOnMainSync(action)

    private suspend fun withViewModel(
        sessions: List<HistorySession> = listOf(HistorySession()),
        block: suspend (AppViewModel, HistorySession) -> Unit,
    ) {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val app = instrumentation.targetContext.applicationContext as Application
        val store = ConnectionStore(app)
        val iterator = sessions.iterator()
        val holder = ViewModelStore()
        lateinit var vm: AppViewModel
        instrumentation.runOnMainSync {
            vm = AppViewModel(app, AppServerRuntime { _, _ -> iterator.next() })
            holder.put("history-test", vm)
        }
        val connection = store.save(ConnectionDraft(name = "History test", host = "fixture.invalid",
            username = "fixture", password = "fixture"))
        try {
            withTimeout(2000) { vm.state.first { !it.isRestoringLastConnection } }
            instrumentation.runOnMainSync { vm.connect(connection) }
            block(vm, sessions.first())
        } finally {
            instrumentation.runOnMainSync { vm.disconnect(); holder.clear() }
            store.delete(connection.id)
        }
    }

    private class HistorySession(private val firstId: String = "recent") : AppServerSession {
        override val version = CodexRuntimeVersion("fixture", "fixture")
        private val incoming = Channel<JsonObject>(Channel.UNLIMITED)
        override val messages = incoming.receiveAsFlow()
        override val diagnostics = Channel<String>(Channel.UNLIMITED).receiveAsFlow()
        val tailRequests = Channel<JsonObject>(Channel.UNLIMITED)
        val firstPageRequests = AtomicInteger()

        override suspend fun send(message: JsonObject) {
            if (message["id"] == null) return
            when (message["method"]?.jsonPrimitive?.content) {
                "thread/list" -> {
                    if (message["params"]?.jsonObject?.get("cursor") == null) {
                        firstPageRequests.incrementAndGet()
                        respond(message, page(firstId, "tail"))
                    } else tailRequests.send(message)
                }
                "thread/resume" -> respond(message, buildJsonObject {
                    put("initialTurnsPage", buildJsonObject {
                        put("data", buildJsonArray { add(buildJsonObject {
                            put("id", "turn"); put("items", buildJsonArray { add(buildJsonObject {
                                put("type", "agentMessage"); put("id", "message"); put("text", "Loaded conversation")
                            }) })
                        }) })
                        put("nextCursor", "older-turns")
                    })
                })
                else -> respond(message, buildJsonObject { put("data", buildJsonArray {}) })
            }
        }
        fun respond(request: JsonObject, result: JsonObject) {
            incoming.trySend(buildJsonObject { put("id", request.getValue("id")); put("result", result) })
        }
        fun fail(request: JsonObject) {
            incoming.trySend(buildJsonObject {
                put("id", request.getValue("id"))
                put("error", buildJsonObject { put("code", -32000); put("message", "fixture tail failure") })
            })
        }
        override fun close() { incoming.close() }
    }

    companion object {
        private fun page(id: String, cursor: String?, vararg overlap: String): JsonObject = buildJsonObject {
            put("data", buildJsonArray { (listOf(id) + overlap).forEach { threadId -> add(buildJsonObject {
                put("id", threadId); put("name", threadId); put("cwd", "/fixture/project"); put("updatedAt", 1)
            }) } })
            put("nextCursor", cursor?.let { kotlinx.serialization.json.JsonPrimitive(it) } ?: JsonNull)
        }
    }
}

package com.codex.remote

import android.app.Application
import androidx.lifecycle.ViewModelStore
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.codex.remote.data.runtime.*
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.domain.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.*
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith

/** In-memory public AppServerSession injection; these are not SSH/daemon integration tests. */
@RunWith(AndroidJUnit4::class)
class TaskSessionsDeviceTest {
    @Test fun backgroundEventsAndCompletionBeforeResponseKeepOriginalOwner() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain { vm.sendMessage("phone") }
            val write = receive(session.turnWrites)
            val origin = write["params"]!!.jsonObject["clientUserMessageId"]!!.jsonPrimitive.content
            session.turn("a", "ta", true)
            session.user("a", "ta", "phone-user", "phone", origin)
            await(vm) { it.activeTurnId == "ta" && it.timeline.any { i -> i.id == "phone-user" } }
            open(vm, session, "b")
            session.delta("a", "ta", "agent-a", "background A")
            session.turn("a", "ta", false)
            await(vm) { it.taskIndicators["a"]?.running == false && (it.taskIndicators["a"]?.unread ?: 0) > 0 }
            session.respond(write, obj("""{"turn":{"id":"ta"}}"""))
            assertEquals("b", vm.state.value.selectedThreadId)
            assertFalse(vm.state.value.isTurnRunning)
            assertFalse(vm.state.value.timeline.any { it.body == "background A" })
            open(vm, session, "a")
            assertTrue(vm.state.value.timeline.any { it.id == "agent-a" && it.body == "background A" })
            assertFalse(vm.state.value.isTurnRunning)
            assertEquals(0, vm.state.value.taskIndicators["a"]!!.unread)
            assertEquals(1, session.startWrites)
        }
    }

    @Test fun aToBToALateResumeCannotOverwriteNewestSnapshotAndBufferedEvents() = runBlocking<Unit> {
        withViewModel { vm, session ->
            select(vm, "a"); val firstA = receive(session.resumes)
            select(vm, "b"); val b = receive(session.resumes)
            select(vm, "a"); val latestA = receive(session.resumes)
            session.delta("a", "live", "live-item", "before snapshot")
            session.respond(firstA, snapshot("a", "stale A"))
            session.respond(b, snapshot("b", "stale B"))
            session.respond(latestA, snapshot("a", "fresh A", activeTurn = "live"))
            await(vm) { it.timeline.any { i -> i.body == "fresh A" } && it.timeline.any { i -> i.body == "before snapshot" } }
            assertEquals("a", vm.state.value.selectedThreadId)
            assertFalse(vm.state.value.timeline.any { it.body in setOf("stale A", "stale B") })
            session.delta("a", "live", "live-item", " after drain")
            await(vm) { it.timeline.any { i -> i.body == "before snapshot after drain" } }
        }
    }

    @Test fun taskDraftModelAndFullAccessAreIsolatedAndStaleConfirmationCannotGrant() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain {
                vm.updateComposer("a", TaskComposer("draft A", 4))
                vm.setModel("second")
                vm.setPermissionMode(PermissionMode.FULL_ACCESS)
            }
            val stale = vm.state.value.fullAccessConfirmation!!
            open(vm, session, "b")
            onMain { vm.confirmFullAccess(stale) }
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            assertEquals("model", vm.state.value.selectedModel)
            assertEquals("", vm.state.value.composer.text)
            onMain { vm.updateComposer("b", TaskComposer("draft B", 7)) }
            open(vm, session, "a", remoteGrant = true)
            assertEquals("draft A", vm.state.value.composer.text)
            assertEquals("second", vm.state.value.selectedModel)
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            onMain { vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            val exact = vm.state.value.fullAccessConfirmation!!
            onMain { vm.confirmFullAccess(exact) }
            assertEquals(":danger-full-access", vm.state.value.selectedPermissionProfile)
            assertEquals("never", vm.state.value.approvalPolicy)
            assertEquals("user", vm.state.value.approvalsReviewer)
            open(vm, session, "b")
            assertEquals("draft B", vm.state.value.composer.text)
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            onMain { vm.newThread() }
            assertNull(vm.state.value.selectedThreadId)
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            assertEquals("", vm.state.value.composer.text)
        }
    }

    @Test fun identicalDesktopAndPhoneTextOnlyReconcileByClientId() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain { vm.sendMessage("same") }
            val write = receive(session.turnWrites)
            val origin = write["params"]!!.jsonObject["clientUserMessageId"]!!.jsonPrimitive.content
            session.turn("a", "t", true)
            session.user("a", "t", "desktop", "same", "desktop-origin")
            await(vm) { it.timeline.count { i -> i.body == "same" } == 2 }
            session.user("a", "t", "no-echo", "same", null)
            await(vm) { it.timeline.count { i -> i.body == "same" } == 3 }
            session.user("a", "t", "phone", "same", origin)
            await(vm) { it.timeline.any { i -> i.id == "phone" } }
            assertEquals(setOf("desktop", "no-echo", "phone"), vm.state.value.timeline.filter { it.body == "same" }.map { it.id }.toSet())
            session.respond(write, obj("""{"turn":{"id":"t"}}"""))
        }
    }

    @Test fun progressiveCompleteImportRetainsSelectedAndRunningTaskSettings() = runBlocking<Unit> {
        withViewModel { vm, session ->
            val tail = receive(session.tails)
            open(vm, session, "a")
            onMain { vm.setModel("second") }
            session.turn("a", "ta", true)
            await(vm) { it.isTurnRunning }
            open(vm, session, "b")
            session.respond(tail, listPage(listOf("b", "c"), null))
            await(vm) { !it.isThreadsLoading }
            assertTrue(vm.state.value.threads.any { it.id == "a" })
            assertTrue(vm.state.value.taskIndicators["a"]!!.running)
            open(vm, session, "a", activeTurn = "ta")
            assertEquals("second", vm.state.value.selectedModel)
            assertEquals("ta", vm.state.value.activeTurnId)
        }
    }

    @Test fun backgroundApprovalPreservesExactOwnerAndProtectsItWhileAnotherTaskRuns() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            session.turn("a", "ta", true)
            await(vm) { it.activeTurnId == "ta" }
            open(vm, session, "b")
            session.emit(obj("""{"id":"approval-a","method":"item/commandExecution/requestApproval","params":{"threadId":"a","turnId":"ta","itemId":"command-a","command":"pwd","cwd":"/fixture/a","availableDecisions":["accept","decline"]}}"""))
            await(vm) { it.approvalQueue.entries.isNotEmpty() && it.taskIndicators["a"]?.approval == true }
            val entry = vm.state.value.approvalQueue.entries.single()
            assertEquals("a", entry.key.threadId)
            onMain { vm.respondToApproval(entry.key, "accept") }
            assertEquals(0, session.approvalReplies)
            open(vm, session, "a", activeTurn = "ta")
            onMain { vm.respondToApproval(entry.key, "decline") }
            await(vm) { it.approvalQueue.entries.isEmpty() }
            assertEquals(1, session.approvalReplies)
        }
    }

    @Test fun resumeOverflowDisconnectsAndRequiresManualRecovery() = runBlocking<Unit> {
        withViewModel { vm, session ->
            select(vm, "a")
            receive(session.resumes)
            repeat(257) { session.delta("a", "t", "item-$it", "event") }
            await(vm) { it.recoveryBlocked && it.connectionStatus == ConnectionStatus.ERROR }
            assertTrue(session.closed)
            assertTrue(vm.state.value.notice!!.contains("Manual recovery"))
            assertEquals(0, session.startWrites)
        }
    }

    @Test fun runningPermissionChangeSaysNextTurnAndDoesNotAlterOtherTask() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a", activeTurn = "t")
            onMain { vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            val confirmation = vm.state.value.fullAccessConfirmation!!
            onMain { vm.confirmFullAccess(confirmation) }
            assertTrue(vm.state.value.notice!!.contains("下个 turn"))
            assertEquals("t", vm.state.value.activeTurnId)
            open(vm, session, "b")
            assertEquals("on-request", vm.state.value.approvalPolicy)
        }
    }

    @Test fun resumeBufferPreservesGlobalApprovalWireOrderAcrossOwners() = runBlocking<Unit> {
        withViewModel { vm, session ->
            select(vm, "a")
            val resume = receive(session.resumes)
            fun approval(id: String, owner: String) = obj("""{"id":"$id","method":"item/commandExecution/requestApproval","params":{"threadId":"$owner","turnId":"turn-$owner","itemId":"item-$owner","command":"pwd","cwd":"/fixture/$owner","availableDecisions":["accept","decline"]}}""")
            session.emit(approval("first-a", "a"))
            session.emit(approval("second-b", "b"))
            session.respond(resume, snapshot("a", "snapshot a"))
            await(vm) { it.approvalQueue.entries.size == 2 && it.taskIndicators["a"]?.approval == true && it.taskIndicators["b"]?.approval == true }
            assertEquals(listOf("a", "b"), vm.state.value.approvalQueue.entries.map { it.key.threadId })
            assertTrue(vm.state.value.taskIndicators["a"]!!.approval)
            assertTrue(vm.state.value.taskIndicators["b"]!!.approval)
        }
    }

    @Test fun outgoingNewThreadResponseCannotSelectOrSendToAnotherTask() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain { vm.newThread(); vm.sendMessage("new task prompt") }
            val start = receive(session.starts)
            open(vm, session, "b")
            session.respond(start, obj("""{"thread":{"id":"new-task"},"cwd":"/fixture/a","model":"model"}"""))
            val turn = receive(session.turnWrites)
            assertEquals("new-task", turn["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
            assertEquals("b", vm.state.value.selectedThreadId)
            assertFalse(vm.state.value.timeline.any { it.body == "new task prompt" })
            session.respond(turn, obj("""{"turn":{"id":"new-turn"}}"""))
            await(vm) { it.taskIndicators["new-task"]?.running == true }
            assertEquals("b", vm.state.value.selectedThreadId)
            assertEquals(1, session.startWrites)
        }
    }

    @Test fun failedResumeKeepsCachedUnsavedNewTaskWithoutGuessingTurn() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain { vm.updateComposer("a", TaskComposer("keep draft", 10)) }
            open(vm, session, "b")
            select(vm, "a")
            val resume = receive(session.resumes)
            session.emit(buildJsonObject { put("id", resume.getValue("id")); put("error", buildJsonObject { put("message", "no rollout found") }) })
            await(vm) { !it.isGoalLoading && it.notice?.contains("no rollout found") == true }
            assertEquals("a", vm.state.value.selectedThreadId)
            assertEquals("keep draft", vm.state.value.composer.text)
            assertTrue(vm.state.value.timeline.any { it.id == "snapshot-a" })
            assertNull(vm.state.value.activeTurnId)
        }
    }

    @Test fun recoveryRestoresExactCachedSelectionOutsideFirstPageWithoutWrites() = runBlocking<Unit> {
        val first = TaskSession()
        val recovered = TaskSession(firstPageIds = listOf("b"))
        withViewModel(listOf(first, recovered)) { vm, session ->
            open(vm, session, "a")
            onMain { vm.updateComposer("a", TaskComposer("cached draft", 12)) }
            val connection = vm.state.value.activeConnection!!
            val selected = vm.selectedTaskForRecovery
            onMain { vm.closeForRecovery(); vm.connect(connection, selected, preserveTaskState = true) }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            val resume = receive(recovered.resumes)
            assertEquals("a", resume["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
            recovered.respond(resume, snapshot("a", "recovered a"))
            await(vm) { it.selectedThreadId == "a" && !it.isGoalLoading && it.timeline.any { i -> i.body == "recovered a" } }
            assertEquals("cached draft", vm.state.value.composer.text)
            assertEquals(0, recovered.startWrites)
            assertEquals(0, recovered.approvalReplies)
        }
    }

    @Test fun outgoingResponseDuringResumeCannotBeUndoneByOlderSnapshot() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            select(vm, "a")
            val resume = receive(session.resumes)
            onMain { vm.sendMessage("while resuming") }
            val write = receive(session.turnWrites)
            session.respond(write, obj("""{"turn":{"id":"new-live"}}"""))
            await(vm) { it.activeTurnId == "new-live" }
            session.respond(resume, snapshot("a", "older snapshot"))
            await(vm) { !it.isGoalLoading && it.timeline.any { i -> i.body == "older snapshot" } }
            assertTrue(vm.state.value.isTurnRunning)
            assertEquals("new-live", vm.state.value.activeTurnId)
        }
    }

    @Test fun staleComposerCallbackCannotOverwriteCurrentTaskAfterNavigation() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            val oldEpoch = vm.state.value.taskSelectionEpoch
            open(vm, session, "b")
            onMain { vm.updateComposer("b", TaskComposer("current B")); vm.updateComposer("a", TaskComposer("stale A"), oldEpoch) }
            assertEquals("current B", vm.state.value.composer.text)
            open(vm, session, "a")
            assertEquals("", vm.state.value.composer.text)
        }
    }

    @Test fun abandoningSlowResumeRetainsBufferedBackgroundEventsAndRejectsOldSnapshot() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            select(vm, "a")
            val slowA = receive(session.resumes)
            session.turn("a", "live-a", true)
            session.delta("a", "live-a", "abandoned-live", "buffered A output")
            open(vm, session, "b")
            await(vm) { (it.taskIndicators["a"]?.unread ?: 0) > 0 }
            session.respond(slowA, snapshot("a", "stale abandoned snapshot"))
            open(vm, session, "a", activeTurn = "live-a")
            assertTrue(vm.state.value.timeline.any { it.id == "abandoned-live" && it.body == "buffered A output" })
            assertFalse(vm.state.value.timeline.any { it.body == "stale abandoned snapshot" })
        }
    }

    @Test fun changingHostClearsGrantAndDraftEvenWhenPreservationIsRequested() = runBlocking<Unit> {
        val original = TaskSession()
        val otherHost = TaskSession()
        withViewModel(listOf(original, otherHost)) { vm, session ->
            open(vm, session, "a")
            onMain { vm.updateComposer("a", TaskComposer("private draft")); vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            val confirmation = vm.state.value.fullAccessConfirmation!!
            onMain { vm.confirmFullAccess(confirmation) }
            val connection = vm.state.value.activeConnection!!.copy(id = "other-host-fixture", host = "other-host.invalid")
            onMain { vm.connect(connection, "a", preserveTaskState = true) }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && it.activeConnection?.host == "other-host.invalid" }
            val resume = receive(otherHost.resumes)
            otherHost.respond(resume, snapshot("a", "other host", remoteGrant = true))
            await(vm) { !it.isGoalLoading && it.timeline.any { i -> i.body == "other host" } }
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            assertEquals("on-request", vm.state.value.approvalPolicy)
            assertEquals("", vm.state.value.composer.text)
        }
    }

    @Test fun freshConnectionOwnerCannotRestorePreviousInMemoryGrantOrDraft() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a")
            onMain { vm.updateComposer("a", TaskComposer("private draft")); vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            val confirmation = vm.state.value.fullAccessConfirmation!!
            onMain { vm.confirmFullAccess(confirmation); vm.disconnect() }
            await(vm) { it.connectionStatus == ConnectionStatus.DISCONNECTED }
            val application = InstrumentationRegistry.getInstrumentation().targetContext.applicationContext as Application
            val restartedSession = TaskSession()
            val holder = ViewModelStore()
            lateinit var restarted: AppViewModel
            onMain { restarted = AppViewModel(application, AppServerRuntime { _, _ -> restartedSession }); holder.put("restart", restarted) }
            try {
                await(restarted) { it.connectionStatus == ConnectionStatus.CONNECTED && it.models.isNotEmpty() }
                open(restarted, restartedSession, "a", remoteGrant = true)
                assertEquals(":workspace", restarted.state.value.selectedPermissionProfile)
                assertEquals("on-request", restarted.state.value.approvalPolicy)
                assertEquals("", restarted.state.value.composer.text)
            } finally { onMain { restarted.disconnect(); holder.clear() } }
        }
    }

    @Test fun authoritativeBackgroundRecoveryReadKeepsSelectedTaskAndReceivesItsEvents() = runBlocking<Unit> {
        withViewModel { vm, session ->
            open(vm, session, "a", activeTurn = "a-running")
            open(vm, session, "b")
            assertEquals(setOf("a", "b"), vm.tasksForRecovery.map { it.threadId }.toSet())
            assertTrue(vm.tasksForRecovery.first { it.threadId == "a" }.running)
            coroutineScope {
                val recovered = async { vm.resumeCachedTaskForRecovery("a") }
                val request = receive(session.resumes)
                assertEquals("a", request["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
                session.delta("a", "a-running", "recovery-live", "background recovery event")
                session.respond(request, snapshot("a", "background recovered snapshot", activeTurn = "a-running"))
                assertTrue(withTimeout(5000) { recovered.await() })
            }
            assertEquals("b", vm.state.value.selectedThreadId)
            assertFalse(vm.state.value.timeline.any { it.id == "recovery-live" })
            open(vm, session, "a", activeTurn = "a-running")
            assertTrue(vm.state.value.timeline.any { it.id == "recovery-live" })
            assertEquals(0, session.startWrites)
        }
    }

    private suspend fun open(vm: AppViewModel, session: TaskSession, id: String, remoteGrant: Boolean = false, activeTurn: String? = null) {
        select(vm, id)
        val request = receive(session.resumes)
        assertEquals(id, request["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
        session.respond(request, snapshot(id, "snapshot $id", activeTurn, remoteGrant))
        await(vm) { it.selectedThreadId == id && it.timeline.any { i -> i.id == "snapshot-$id" } && !it.isGoalLoading }
    }
    private fun select(vm: AppViewModel, id: String) = onMain { vm.selectThread(vm.state.value.threads.first { it.id == id }) }
    private suspend fun receive(channel: Channel<JsonObject>) = withTimeout(5000) { channel.receive() }
    private suspend fun await(vm: AppViewModel, condition: (AppUiState) -> Boolean) = withTimeout(5000) { vm.state.first(condition) }
    private fun onMain(action: () -> Unit) = InstrumentationRegistry.getInstrumentation().runOnMainSync(action)

    private suspend fun withViewModel(sessions: List<TaskSession> = listOf(TaskSession()), block: suspend (AppViewModel, TaskSession) -> Unit) {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val app = instrumentation.targetContext.applicationContext as Application
        val store = ConnectionStore(app)
        val session = sessions.first()
        val iterator = sessions.iterator()
        val holder = ViewModelStore()
        lateinit var vm: AppViewModel
        onMain { vm = AppViewModel(app, AppServerRuntime { _, _ -> iterator.next() }); holder.put("tasks", vm) }
        val connection = store.save(ConnectionDraft(name = "Task sessions", host = "task-fixture.invalid", username = "fixture", password = "fixture"))
        try {
            await(vm) { !it.isRestoringLastConnection }
            onMain { vm.connect(connection) }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && it.models.isNotEmpty() }
            block(vm, session)
        } finally { onMain { vm.disconnect(); holder.clear() }; store.delete(connection.id) }
    }

    internal class TaskSession(private val firstPageIds: List<String> = listOf("a", "b")) : AppServerSession {
        override val version = CodexRuntimeVersion("fixture", "fixture")
        private val incoming = Channel<JsonObject>(Channel.UNLIMITED)
        override val messages = incoming.receiveAsFlow()
        override val diagnostics = Channel<String>(Channel.UNLIMITED).receiveAsFlow()
        val resumes = Channel<JsonObject>(Channel.UNLIMITED)
        val starts = Channel<JsonObject>(Channel.UNLIMITED)
        val tails = Channel<JsonObject>(Channel.UNLIMITED)
        val turnWrites = Channel<JsonObject>(Channel.UNLIMITED)
        var startWrites = 0
        var approvalReplies = 0
        var closed = false
        override suspend fun send(message: JsonObject) {
            if (message["result"] != null) { approvalReplies++; return }
            if (message["id"] == null) return
            when (message["method"]?.jsonPrimitive?.content) {
                "thread/list" -> if (message["params"]!!.jsonObject["cursor"] == null) respond(message, listPage(firstPageIds, "tail")) else tails.send(message)
                "model/list" -> respond(message, obj("""{"data":[{"id":"model","model":"model","isDefault":true},{"id":"second","model":"second"}]}"""))
                "thread/resume" -> resumes.send(message)
                "thread/start" -> starts.send(message)
                "turn/start" -> { startWrites++; turnWrites.send(message) }
                "turn/steer" -> turnWrites.send(message)
                "thread/goal/get" -> respond(message, obj("""{"goal":null}"""))
                else -> respond(message, obj("""{"data":[],"requiresOpenaiAuth":false}"""))
            }
        }
        fun turn(owner: String, turn: String, running: Boolean) = emit(buildJsonObject {
            put("method", if (running) "turn/started" else "turn/completed")
            put("params", buildJsonObject { put("threadId", owner); put("turn", buildJsonObject { put("id", turn); put("status", if (running) "inProgress" else "completed") }) })
        })
        fun delta(owner: String, turn: String, item: String, delta: String) = emit(buildJsonObject {
            put("method", "item/agentMessage/delta")
            put("params", buildJsonObject { put("threadId", owner); put("turnId", turn); put("itemId", item); put("delta", delta) })
        })
        fun user(owner: String, turn: String, id: String, text: String, clientId: String?) = emit(buildJsonObject {
            put("method", "item/started")
            put("params", buildJsonObject { put("threadId", owner); put("turnId", turn); put("item", buildJsonObject {
                put("type", "userMessage"); put("id", id); clientId?.let { put("clientId", it) }
                put("content", buildJsonArray { add(buildJsonObject { put("type", "text"); put("text", text) }) })
            }) })
        })
        fun respond(request: JsonObject, result: JsonObject) = emit(buildJsonObject { put("id", request.getValue("id")); put("result", result) })
        fun emit(message: JsonObject) { incoming.trySend(message) }
        override fun close() { closed = true; incoming.close() }
    }

    companion object {
        internal fun obj(text: String) = Json.parseToJsonElement(text).jsonObject
        internal fun listPage(ids: List<String>, cursor: String?) = buildJsonObject {
            put("data", buildJsonArray { ids.forEach { id -> add(buildJsonObject {
                put("id", id); put("name", id); put("cwd", "/fixture/$id"); put("updatedAt", 1)
            }) } })
            put("nextCursor", cursor?.let(::JsonPrimitive) ?: JsonNull)
        }
        internal fun snapshot(id: String, body: String, activeTurn: String? = null, remoteGrant: Boolean = false) = buildJsonObject {
            put("thread", buildJsonObject { put("id", id); put("cwd", "/fixture/$id"); put("status", buildJsonObject { put("type", if (activeTurn == null) "idle" else "active") }) })
            put("model", "model")
            if (remoteGrant) { put("activePermissionProfile", buildJsonObject { put("id", ":danger-full-access") }); put("approvalPolicy", "never") }
            put("initialTurnsPage", buildJsonObject { put("data", buildJsonArray { add(buildJsonObject {
                put("id", activeTurn ?: "snapshot-turn-$id"); put("status", if (activeTurn == null) "completed" else "inProgress")
                put("items", buildJsonArray { add(buildJsonObject { put("id", "snapshot-$id"); put("type", "agentMessage"); put("text", body) }) })
            }) }); put("nextCursor", "older-$id") })
        }
    }
}

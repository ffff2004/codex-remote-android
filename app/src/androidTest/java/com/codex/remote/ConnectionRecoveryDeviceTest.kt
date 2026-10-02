package com.codex.remote

import android.app.Application
import androidx.lifecycle.ViewModelStore
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.codex.remote.TaskSessionsDeviceTest.TaskSession
import com.codex.remote.TaskSessionsDeviceTest.Companion.snapshot
import com.codex.remote.TaskSessionsDeviceTest.Companion.obj
import com.codex.remote.connection.*
import com.codex.remote.data.runtime.*
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.domain.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.*
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/** Policy/owner tests with an injected session and foreground platform, not physical network tests. */
@RunWith(AndroidJUnit4::class)
class ConnectionRecoveryDeviceTest {
    private val app get() = InstrumentationRegistry.getInstrumentation().targetContext.applicationContext as Application
    private fun onMain(block: () -> Unit) = InstrumentationRegistry.getInstrumentation().runOnMainSync(block)
    private suspend fun await(vm: AppViewModel, predicate: (AppUiState) -> Boolean) = withTimeout(10_000) { vm.state.first(predicate) }
    private suspend fun open(vm: AppViewModel, session: TaskSession, id: String, running: Boolean = false) {
        onMain { vm.selectThread(vm.state.value.threads.first { it.id == id }) }
        val request = withTimeout(5000) { session.resumes.receive() }
        session.respond(request, snapshot(id, "snapshot $id", if (running) "live-$id" else null))
        await(vm) { it.selectedThreadId == id && !it.isGoalLoading && it.timeline.isNotEmpty() }
    }
    private class Foreground : ForegroundMaintenance {
        var started = false; var stops = 0
        override fun start() { started = true }
        override fun stop() { stops++; started = false }
    }

    private suspend fun withOwner(runtime: AppServerRuntime, block: suspend (ProcessConnectionOwner, SavedConnection, Foreground) -> Unit) {
        val foreground = Foreground(); val store = ConnectionStore(app)
        val saved = store.save(ConnectionDraft(name = "Recovery fixture", host = "recovery.invalid", username = "fixture", password = "fixture", hostKeyFingerprint = "SHA256:fixture"))
        lateinit var owner: ProcessConnectionOwner
        val holder = ViewModelStore()
        onMain { owner = ProcessConnectionOwner(app, runtime, foreground); holder.put("recovery", owner.viewModel) }
        try { await(owner.viewModel) { !it.isRestoringLastConnection }; block(owner, saved, foreground) }
        finally { onMain { owner.viewModel.disconnect(); owner.close(); holder.clear() }; store.delete(saved.id) }
    }

    @Test fun foregroundStartsBeforeBlockingDialAndDisconnectCancelsWithoutLeavingAttempt() = runBlocking<Unit> {
        val entered = CompletableDeferred<Unit>(); val release = CountDownLatch(1); var exited = false
        lateinit var foreground: Foreground
        withOwner(AppServerRuntime { _, _ ->
            assertTrue(foreground.started)
            blockingIo(60_000, { release.countDown() }) { entered.complete(Unit); release.await(); exited = true }
            throw java.io.IOException("fixture closed")
        }) { owner, saved, platform ->
            foreground = platform
            onMain { owner.viewModel.connect(saved); owner.networkAvailable(1) }
            withTimeout(5000) { entered.await() }
            assertEquals(saved.id, ConnectionMaintenanceStore(app).desiredConnectionId())
            onMain { owner.viewModel.disconnect() }
            withTimeout(5000) { owner.viewModel.awaitConnectionAttempt() }
            assertTrue(exited); assertFalse(platform.started)
            assertNull(ConnectionMaintenanceStore(app).desiredConnectionId())
            assertNull(owner.viewModel.state.value.activeConnection)
        }
    }

    @Test fun transportRecoveryResumesSelectedBThenRunningAAndNeverReplaysWritesOrApprovals() = runBlocking<Unit> {
        val first = TaskSession(); val recovered = TaskSession(listOf("b")); val count = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> if (count.getAndIncrement() == 0) first else recovered }) { owner, saved, _ ->
            val vm = owner.viewModel
            onMain { vm.connect(saved) }; await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            open(vm, first, "a", running = true)
            onMain { vm.updateComposer("a", TaskComposer("private A")); vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            onMain { vm.confirmFullAccess(vm.state.value.fullAccessConfirmation!!) }
            open(vm, first, "b")
            first.emit(obj("""{"id":901,"method":"item/commandExecution/requestApproval","params":{"threadId":"a","turnId":"live-a","itemId":"cmd","command":"pwd","cwd":"/fixture/a"}}"""))
            await(vm) { it.approvalQueue.entries.isNotEmpty() }
            first.close()
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && count.get() == 2 }
            assertEquals("b", vm.state.value.selectedThreadId)
            val b = withTimeout(5000) { recovered.resumes.receive() }
            assertEquals("b", b["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
            // A network callback during recovery must not abandon background A's subscription.
            onMain { owner.networkAvailable(1); owner.networkAvailable(2) }
            recovered.respond(b, snapshot("b", "B recovered"))
            val a = withTimeout(5000) { recovered.resumes.receive() }
            assertEquals("a", a["params"]!!.jsonObject["threadId"]!!.jsonPrimitive.content)
            recovered.respond(a, snapshot("a", "A recovered", "live-a", remoteGrant = true))
            recovered.delta("a", "live-a", "background-after-recovery", "still subscribed")
            await(vm) { (it.taskIndicators["a"]?.unread ?: 0) > 0 }
            assertEquals("b", vm.state.value.selectedThreadId)
            assertTrue(vm.state.value.recoveryApprovalWarning!!.contains("no response will be retried"))
            assertTrue(vm.state.value.approvalQueue.entries.isEmpty())
            open(vm, recovered, "a", running = true)
            assertEquals("private A", vm.state.value.composer.text)
            assertEquals(":danger-full-access", vm.state.value.selectedPermissionProfile)
            assertTrue(vm.state.value.timeline.any { it.id == "background-after-recovery" })
            assertEquals(0, recovered.startWrites); assertEquals(0, recovered.approvalReplies)
        }
    }

    @Test fun initialDialWaitsForDozeAndNetworkAndStoppedIntentDoesNotWake() = runBlocking<Unit> {
        val calls = AtomicInteger(); val session = TaskSession()
        withOwner(AppServerRuntime { _, _ -> calls.incrementAndGet(); session }) { owner, saved, _ ->
            onMain { owner.deviceIdle(true); owner.noNetwork(); owner.viewModel.connect(saved) }
            delay(100); assertEquals(0, calls.get())
            onMain { owner.networkAvailable(1) }
            delay(100); assertEquals(0, calls.get())
            onMain { owner.deviceIdle(false) }
            await(owner.viewModel) { it.connectionStatus == ConnectionStatus.CONNECTED }
            assertEquals(1, calls.get())
            onMain { owner.viewModel.disconnect(); owner.deviceIdle(true); owner.deviceIdle(false); owner.networkAvailable(2) }
            delay(100); assertEquals(1, calls.get())
            assertNull(ConnectionMaintenanceStore(app).desiredConnectionId())
        }
    }

    @Test fun fatalHostKeyFailureDoesNotRetryOnEveryPlatformCallback() = runBlocking<Unit> {
        val calls = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> calls.incrementAndGet(); throw UnknownHostKeyException("SHA256:unknown") }) { owner, saved, _ ->
            onMain { owner.viewModel.connect(saved) }
            await(owner.viewModel) { it.pendingHostKeyFingerprint != null && it.maintenanceStatus == "Manual Connect required" }
            onMain { repeat(10) { owner.networkAvailable(it.toLong()); owner.deviceIdle(true); owner.deviceIdle(false) } }
            delay(300); assertEquals(1, calls.get())
            onMain { owner.viewModel.connect(saved) }
            await(owner.viewModel) { calls.get() == 2 && it.connectionStatus == ConnectionStatus.ERROR }
        }
    }

    @Test fun malformedRpcIdentityBlocksPlatformRecoveryUntilManualConnect() = runBlocking<Unit> {
        val first = TaskSession(); val recovered = TaskSession(); val calls = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> if (calls.getAndIncrement() == 0) first else recovered }) { owner, saved, _ ->
            val vm = owner.viewModel
            onMain { vm.connect(saved) }; await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            first.emit(obj("""{"id":{},"method":"item/commandExecution/requestApproval","params":{}}"""))
            await(vm) { it.recoveryBlocked && it.connectionStatus == ConnectionStatus.ERROR }
            onMain { repeat(10) { owner.networkAvailable(it.toLong()); owner.deviceIdle(true); owner.deviceIdle(false) } }
            delay(300)
            assertEquals(1, calls.get()); assertTrue(vm.state.value.recoveryBlocked)
            onMain { vm.connect(saved) }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && calls.get() == 2 }
            assertFalse(vm.state.value.recoveryBlocked)
        }
    }

    @Test fun changedPersistedTrustMakesNewTaskRequestsSafeAndRejectsOldConfirmation() = runBlocking<Unit> {
        val first = TaskSession(); val recovered = TaskSession(); val calls = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> if (calls.getAndIncrement() == 0) first else recovered }) { owner, saved, _ ->
            val vm = owner.viewModel
            onMain { vm.connect(saved) }; await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            open(vm, first, "a")
            onMain { vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            val oldConfirmation = vm.state.value.fullAccessConfirmation!!
            onMain { vm.confirmFullAccess(oldConfirmation) }
            assertEquals("never", vm.state.value.approvalPolicy)
            val changed = ConnectionStore(app).save(ConnectionDraft(id = saved.id, name = saved.name, host = saved.host,
                username = saved.username, hostKeyFingerprint = "SHA256:new-owner"), saved)
            await(vm) { it.activeConnection?.hostKeyFingerprint == changed.hostKeyFingerprint }
            first.close()
            await(vm) { calls.get() == 2 && it.connectionStatus == ConnectionStatus.CONNECTED && it.remoteAccount != null }
            assertNull(vm.state.value.selectedThreadId)
            onMain { vm.confirmFullAccess(oldConfirmation); vm.sendMessage("safe new task") }
            val start = withTimeout(5000) { recovered.starts.receive() }
            recovered.respond(start, obj("""{"thread":{"id":"new-task","cwd":"/fixture/a"},"model":"model"}"""))
            val turn = withTimeout(5000) { recovered.turnWrites.receive() }
            val startParams = start["params"]!!.jsonObject; val turnParams = turn["params"]!!.jsonObject
            assertEquals("on-request", startParams["approvalPolicy"]!!.jsonPrimitive.content)
            assertEquals("on-request", turnParams["approvalPolicy"]!!.jsonPrimitive.content)
            assertEquals(":workspace", startParams["permissions"]!!.jsonPrimitive.content)
            assertEquals(":workspace", turnParams["permissions"]!!.jsonPrimitive.content)
            assertFalse(startParams.toString().contains("danger-full-access"))
            assertFalse(turnParams.toString().contains("danger-full-access"))
            recovered.respond(turn, obj("""{"turn":{"id":"safe-turn"}}"""))
            onMain { vm.disconnect() }
            await(vm) { it.activeConnection == null }
            assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
            assertEquals("on-request", vm.state.value.approvalPolicy)
            assertEquals("user", vm.state.value.approvalsReviewer)
        }
    }

    @Test fun persistedFingerprintEditUnderSameIdDropsDraftAndGrantBeforeRecovery() = runBlocking<Unit> {
        val first = TaskSession(); val recovered = TaskSession(); val calls = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> if (calls.getAndIncrement() == 0) first else recovered }) { owner, saved, _ ->
            val vm = owner.viewModel
            onMain { vm.connect(saved) }; await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            open(vm, first, "a")
            onMain { vm.updateComposer("a", TaskComposer("private old host")); vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            onMain { vm.confirmFullAccess(vm.state.value.fullAccessConfirmation!!) }
            val changed = ConnectionStore(app).save(ConnectionDraft(id = saved.id, name = saved.name, host = saved.host,
                username = saved.username, hostKeyFingerprint = "SHA256:replacement"), saved)
            await(vm) { it.activeConnection?.hostKeyFingerprint == changed.hostKeyFingerprint }
            onMain { vm.closeForRecovery(); vm.connect(changed, "a", preserveTaskState = true) }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            val resume = withTimeout(5000) { recovered.resumes.receive() }
            recovered.respond(resume, snapshot("a", "new trust context", remoteGrant = true))
            await(vm) { it.timeline.any { item -> item.body == "new trust context" } && !it.isGoalLoading }
            assertEquals("", vm.state.value.composer.text); assertEquals(":workspace", vm.state.value.selectedPermissionProfile)
        }
    }

    @Test fun deletionBeforeStickyRestoreClearsIntentWithoutDialling() = runBlocking<Unit> {
        val calls = AtomicInteger()
        withOwner(AppServerRuntime { _, _ -> calls.incrementAndGet(); TaskSession() }) { owner, saved, platform ->
            ConnectionMaintenanceStore(app).remember(saved.id)
            assertNull(owner.viewModel.state.value.activeConnection)
            onMain { owner.viewModel.deleteConnection(saved) }
            await(owner.viewModel) { it.savedConnections.none { connection -> connection.id == saved.id } }
            assertNull(ConnectionMaintenanceStore(app).desiredConnectionId())
            onMain { owner.restoreDesiredConnection() }
            delay(100)
            assertEquals(0, calls.get()); assertFalse(platform.started)
        }
    }

    @Test fun deletionClearsDesiredTargetAndRestartOnlyRestoresSavedIdWithSafeTaskSettings() = runBlocking<Unit> {
        val session = TaskSession()
        withOwner(AppServerRuntime { _, _ -> session }) { owner, saved, platform ->
            val vm = owner.viewModel
            onMain { vm.connect(saved) }; await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED }
            open(vm, session, "a")
            onMain { vm.updateComposer("a", TaskComposer("not durable")); vm.setPermissionMode(PermissionMode.FULL_ACCESS) }
            onMain { vm.confirmFullAccess(vm.state.value.fullAccessConfirmation!!) }
            // A new process owner reads only the desired saved ID; it has no former cache or grant.
            onMain { owner.close() }
            val freshSession = TaskSession(); lateinit var fresh: ProcessConnectionOwner
            onMain { fresh = ProcessConnectionOwner(app, AppServerRuntime { _, _ -> freshSession }, platform); fresh.restoreDesiredConnection() }
            try {
                await(fresh.viewModel) { it.connectionStatus == ConnectionStatus.CONNECTED }
                open(fresh.viewModel, freshSession, "a")
                assertEquals("", fresh.viewModel.state.value.composer.text)
                assertEquals(":workspace", fresh.viewModel.state.value.selectedPermissionProfile)
            } finally { onMain { fresh.close() } }
            onMain { vm.deleteConnection(saved) }
            await(vm) { it.savedConnections.none { connection -> connection.id == saved.id } }
            assertNull(ConnectionMaintenanceStore(app).desiredConnectionId()); assertFalse(platform.started)
        }
    }
}

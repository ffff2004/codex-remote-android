package com.codex.remote

import android.Manifest
import android.app.NotificationManager
import android.content.Intent
import android.os.Build
import android.os.PowerManager
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat
import androidx.test.core.app.ActivityScenario
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.codex.remote.connection.ConnectionMaintenanceStore
import com.codex.remote.connection.SshConnectionService
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.domain.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import java.net.Socket

/** Configured loopback SSH fixtures. Both modes exercise the production Android SSH runtime. */
@RunWith(AndroidJUnit4::class)
class SshRecoveryDeviceTest {
    private val instrumentation get() = InstrumentationRegistry.getInstrumentation()
    private val app get() = instrumentation.targetContext.applicationContext as CodexRemoteApplication
    private val args get() = InstrumentationRegistry.getArguments()
    private fun required(name: String) = requireNotNull(args.getString(name)) { "Configure $name for the SSH fixture" }
    private fun onMain(block: () -> Unit) = instrumentation.runOnMainSync(block)
    private suspend fun await(vm: AppViewModel, phase: String = "state", predicate: (AppUiState) -> Boolean): AppUiState = try {
        withTimeout(90_000) { vm.state.first(predicate) }
    } catch (error: TimeoutCancellationException) {
        val state = vm.state.value
        throw AssertionError("Timed out in $phase: status=${state.connectionStatus}, selected=${state.selectedThreadId}, loading=${state.isGoalLoading}, blocked=${state.recoveryBlocked}, notice=${state.notice}, maintenance=${state.maintenanceStatus}", error)
    }
    private suspend fun control(port: String, command: String) = withContext(Dispatchers.IO) {
        Socket("127.0.0.1", port.toInt()).use { socket ->
            socket.soTimeout = 30_000
            socket.getOutputStream().write((command + "\n").toByteArray())
            check(socket.getInputStream().bufferedReader().readLine() == "OK")
        }
    }

    @Test fun productionSshRecoveryAndForegroundLifecycle() = runBlocking<Unit> {
        val fixture = required("sshFixture")
        val store = ConnectionStore(app)
        val saved = store.save(ConnectionDraft(name = "SSH integration $fixture", host = "127.0.0.1", port = required("sshPort"),
            username = required("sshUsername"), password = required("sshPassword"), hostKeyFingerprint = required("sshFingerprint")))
        var scenario: ActivityScenario<MainActivity>? = null
        val vm = app.appViewModel
        try {
            if (Build.VERSION.SDK_INT >= 33) instrumentation.uiAutomation.grantRuntimePermission(app.packageName, Manifest.permission.POST_NOTIFICATIONS)
            scenario = ActivityScenario.launch(MainActivity::class.java)
            onMain { vm.connect(saved) }
            val connected = await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && it.activeConnection?.id == saved.id }
            assertTrue(connected.models.isNotEmpty())
            if (fixture == "mock") {
                assertTrue("First page must connect while the delayed tail is still loading", connected.isThreadsLoading)
                assertEquals(2, connected.threads.size)
            }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && !it.isThreadsLoading && it.threads.isNotEmpty() }
            val threadId = if (fixture == "real") required("sshThreadId") else "thread-demo-primary"
            val thread = vm.state.value.threads.first { it.id == threadId }
            onMain { vm.selectThread(thread) }
            await(vm, "selected task resume") { it.selectedThreadId == threadId && !it.isGoalLoading && it.timeline.isNotEmpty() }
            if (fixture == "mock") {
                onMain { vm.sendMessage("Android production subscription marker") }
                await(vm, "live marker turn") { !it.isTurnRunning && it.timeline.any { item -> item.body.contains("收到：Android production subscription marker") } }
                onMain { vm.sendMessage("request approval") }
                val approvalState = await(vm, "approval request") { it.approvalQueue.entries.isNotEmpty() }
                val approval = approvalState.approvalQueue.entries.single()
                assertEquals(threadId, approval.request.threadId)
                assertTrue(approval.request.detail.contains("git status --short"))
                onMain { vm.respondToApproval(approval.key, "decline") }
                await(vm) { it.approvalQueue.entries.isEmpty() && !it.isTurnRunning }
            }
            val previousServer = vm.state.value.remoteServer
            scenario.close(); scenario = null
            // Activity removal retains the exact process owner and its foreground service.
            assertSame(vm, app.appViewModel)
            assertEquals(ConnectionStatus.CONNECTED, vm.state.value.connectionStatus)
            assertEquals(saved.id, ConnectionMaintenanceStore(app).desiredConnectionId())
            val notifications = app.getSystemService(NotificationManager::class.java)
            withTimeout(5000) { while (notifications.activeNotifications.none { it.id == SshConnectionService.NOTIFICATION_ID }) delay(50) }
            if (fixture == "real") {
                // Actual Android platform idle state/broadcast, simulated by shell; no radio proof.
                check(!app.getSystemService(PowerManager::class.java).isDeviceIdleMode)
                try {
                    val result = shell("dumpsys deviceidle force-idle")
                    println("Stage3 platform Doze: $result")
                    await(vm) { it.maintenanceStatus?.contains("Doze") == true }
                    assertEquals(saved.id, ConnectionMaintenanceStore(app).desiredConnectionId())
                    assertEquals(ConnectionStatus.CONNECTED, vm.state.value.connectionStatus)
                } finally { shell("dumpsys deviceidle unforce") }
                await(vm) { it.maintenanceStatus?.contains("Doze") == false }
            }
            control(required("sshControlPort"), "DROP")
            await(vm) { it.connectionStatus != ConnectionStatus.CONNECTED }
            await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && it.remoteServer != null && it.selectedThreadId == threadId }
            await(vm) { !it.isGoalLoading && it.timeline.isNotEmpty() }
            assertEquals(previousServer?.codexVersion, vm.state.value.remoteServer?.codexVersion)
            if (fixture == "real") {
                // A second genuine daemon client sends a fresh test turn after Android's recovery.
                delay(500)
                control(required("sshEventPort"), "EVENT")
                await(vm) { it.timeline.any { item -> item.clientId == "stage3-external-after-recovery" } }
            }
            val notification = notifications.activeNotifications.single { it.id == SshConnectionService.NOTIFICATION_ID }.notification
            val action = notification.actions.single { it.title.toString() == "Disconnect" }
            action.actionIntent.send()
            await(vm) { it.activeConnection == null && it.connectionStatus == ConnectionStatus.DISCONNECTED }
            assertNull(ConnectionMaintenanceStore(app).desiredConnectionId())
            withTimeout(5000) { while (notifications.activeNotifications.any { it.id == SshConnectionService.NOTIFICATION_ID }) delay(50) }
        } finally {
            scenario?.close()
            onMain { vm.disconnect() }
            store.delete(saved.id)
        }
    }

    @Test fun configuredNotificationPermissionMaintainsAcrossRotationAndAppDisconnect() = runBlocking<Unit> {
        val store = ConnectionStore(app)
        val saved = store.save(ConnectionDraft(name = "SSH denied notification", host = "127.0.0.1", port = required("sshPort"),
            username = required("sshUsername"), password = required("sshPassword"), hostKeyFingerprint = required("sshFingerprint")))
        val vm = app.appViewModel
        try {
            if (Build.VERSION.SDK_INT >= 33) {
                if (args.getString("sshNotificationPermission") == "denied") {
                    assertEquals(PackageManager.PERMISSION_DENIED, ContextCompat.checkSelfPermission(app, Manifest.permission.POST_NOTIFICATIONS))
                    assertFalse(app.getSystemService(NotificationManager::class.java).areNotificationsEnabled())
                } else {
                    instrumentation.uiAutomation.grantRuntimePermission(app.packageName, Manifest.permission.POST_NOTIFICATIONS)
                }
            }
            // MainActivity may ask once; the test has already made the denied choice.
            ActivityScenario.launch(MainActivity::class.java).use { scenario ->
                onMain { vm.connect(saved) }
                await(vm) { it.connectionStatus == ConnectionStatus.CONNECTED && it.activeConnection?.id == saved.id }
                scenario.recreate()
                assertSame(vm, app.appViewModel)
                assertEquals(ConnectionStatus.CONNECTED, vm.state.value.connectionStatus)
                assertEquals(saved.id, ConnectionMaintenanceStore(app).desiredConnectionId())
                onMain { vm.disconnect() }
                await(vm) { it.activeConnection == null }
                assertNull(ConnectionMaintenanceStore(app).desiredConnectionId())
            }
        } finally {
            onMain { vm.disconnect() }; store.delete(saved.id)
        }
    }
    private fun shell(command: String): String = instrumentation.uiAutomation.executeShellCommand(command).use { descriptor ->
        java.io.FileInputStream(descriptor.fileDescriptor).readBytes().toString(Charsets.UTF_8)
    }
}

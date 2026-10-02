package com.codex.remote.connection

import android.app.Application
import android.content.Intent
import android.os.SystemClock
import androidx.core.content.ContextCompat
import com.codex.remote.AppViewModel
import com.codex.remote.data.runtime.AppServerException
import com.codex.remote.data.runtime.AppServerRuntime
import com.codex.remote.data.runtime.SshCodexAppServerRuntime
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.domain.ConnectionStatus
import com.codex.remote.domain.SavedConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select

/** Main-thread process owner. The foreground service has no dial or session of its own. */
class ProcessConnectionOwner(
    private val application: Application,
    runtime: AppServerRuntime = SshCodexAppServerRuntime(application),
    private val foreground: ForegroundMaintenance = AndroidForegroundMaintenance(application),
) : ConnectionMaintenance, java.io.Closeable {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val store = ConnectionMaintenanceStore(application)
    private val policy = ConnectionRecoveryPolicy()
    private val wake = Channel<Unit>(Channel.CONFLATED)
    val viewModel = AppViewModel(application, runtime, restoreLastConnection = false, maintenance = this)
    private var desired: SavedConnection? = null
    private var attemptMonitor: Job? = null
    private var attemptFatal = false
    private var ownerGeneration = 0L
    private var subscriptionError: String? = null
    private var foregroundError: String? = null
    private var selectedForRecovery: String? = null
    private var tasksForRecovery = emptyList<String>()
    private var lastHealthAt = now()

    init {
        scope.launch { viewModel.state.collect { state ->
            if (state.recoveryBlocked) policy.block()
            wake.trySend(Unit)
        } }
        scope.launch { runPolicy() }
    }

    override fun connectRequested(connection: SavedConnection): Boolean {
        ownerGeneration++
        subscriptionError = null; foregroundError = null
        store.remember(connection.id)
        desired = connection
        policy.manualConnect(now())
        selectedForRecovery = null; tasksForRecovery = emptyList()
        startService()
        val ready = policy.evaluate(now(), connected = false, running = false, approval = false, rpc = false) is RecoveryDecision.Dial
        if (ready) monitorAttempt(policy.epoch)
        wake.trySend(Unit)
        return ready
    }

    override fun isDesiredConnection(id: String) = desired?.id == id || store.desiredConnectionId() == id

    override fun disconnectRequested() {
        ownerGeneration++
        policy.stop()
        desired = null
        // Commit before stopping: sticky recreation must not revive a manually stopped target.
        store.clear()
        attemptMonitor?.cancel()
        viewModel.setMaintenanceStatus(null)
        foreground.stop()
        wake.trySend(Unit)
    }

    override fun dialFailed(error: Throwable) { attemptFatal = isFatalConnectionFailure(error) }
    override fun transportLost() { captureTasks(); policy.transportLost(now()); wake.trySend(Unit) }

    /** Invoked by sticky service recreation, never by Activity creation or last-used metadata. */
    fun restoreDesiredConnection() {
        if (desired != null) return
        val id = store.desiredConnectionId() ?: return
        scope.launch {
            val saved = ConnectionStore(application).connections.first().firstOrNull { it.id == id }
            if (store.desiredConnectionId() != id || desired != null) return@launch
            if (saved == null) { store.clear(); foreground.stop(); return@launch }
            desired = saved; policy.manualConnect(now()); wake.trySend(Unit)
        }
    }

    fun networkAvailable(id: Long) { policy.networkAvailable(id, now()); wake.trySend(Unit) }
    fun networkLost(id: Long) { policy.networkLost(id); wake.trySend(Unit) }
    fun noNetwork() { policy.noNetwork(); wake.trySend(Unit) }
    fun deviceIdle(value: Boolean) { policy.deviceIdle(value); wake.trySend(Unit) }

    private fun startService() {
        try { foreground.start() }
        catch (error: RuntimeException) {
            policy.block()
            foregroundError = "Foreground maintenance unavailable: ${error.message}. Open the app and Connect to retry."
            viewModel.setMaintenanceStatus(foregroundError)
        }
    }

    private fun captureTasks() {
        selectedForRecovery = viewModel.selectedTaskForRecovery
        tasksForRecovery = viewModel.tasksForRecovery.map { it.threadId }
            .sortedBy { if (it == selectedForRecovery) 0 else 1 }
    }

    private fun monitorAttempt(token: Long) {
        attemptMonitor?.cancel()
        attemptFatal = false
        policy.dialStarted(token)
        val generation = ownerGeneration
        attemptMonitor = scope.launch {
            // Manual connect creates its VM job immediately after this synchronous callback returns.
            yield()
            viewModel.awaitConnectionAttempt()
            val success = viewModel.state.value.connectionStatus == ConnectionStatus.CONNECTED
            policy.dialFinished(token, success, attemptFatal || viewModel.state.value.recoveryBlocked, now())
            if (generation == ownerGeneration && policy.enabled && success) {
                lastHealthAt = now()
                for (id in tasksForRecovery) {
                    if (generation != ownerGeneration || !policy.enabled || viewModel.state.value.connectionStatus != ConnectionStatus.CONNECTED) break
                    val restored = try { withTimeout(30_000) { viewModel.resumeCachedTaskForRecovery(id) } }
                    catch (cancelled: CancellationException) {
                        if (cancelled !is TimeoutCancellationException) throw cancelled
                        false
                    }
                    if (!restored && generation == ownerGeneration) subscriptionError =
                        "Task subscription not restored (${id.take(128)}); open the task or Connect to retry."
                    if (viewModel.state.value.recoveryBlocked) { policy.block(); break }
                }
            }
            wake.trySend(Unit)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun runPolicy() {
        while (currentCoroutineContext().isActive) {
            val state = viewModel.state.value
            val connected = state.connectionStatus == ConnectionStatus.CONNECTED
            var until: Long? = null
            when (val decision = policy.evaluate(now(), connected, viewModel.hasRunningTasks, viewModel.hasPendingApprovals, viewModel.hasPendingRpc)) {
                is RecoveryDecision.Dial -> if (attemptMonitor?.isActive != true) {
                    val target = desired
                    if (target != null) {
                        // Use the current saved trust context, including edits made under the same ID.
                        val current = state.savedConnections.firstOrNull { it.id == target.id } ?: target
                        desired = current
                        viewModel.connect(current, preserveTaskState = true, automaticRecovery = true)
                        monitorAttempt(decision.epoch)
                    }
                }
                RecoveryDecision.Handoff -> {
                    attemptMonitor?.cancelAndJoin()
                    captureTasks(); viewModel.closeForRecovery(); policy.handoffCompleted(now()); wake.trySend(Unit)
                }
                is RecoveryDecision.Wait -> {
                    viewModel.setMaintenanceStatus(if (policy.enabled) foregroundError ?: subscriptionError ?: decision.message else null)
                    until = decision.until
                    // Health is a bounded authoritative read, never a write or a duplicate subscription.
                    if (connected && policy.enabled && !policy.blocked && decision.message == "Foreground connection maintained") {
                        val healthDue = lastHealthAt + 30_000
                        until = minOf(until ?: healthDue, healthDue)
                        if (now() >= healthDue && attemptMonitor?.isActive != true && !viewModel.hasPendingRpc) {
                            lastHealthAt = now()
                            attemptMonitor = scope.launch {
                                try { viewModel.checkConnectionHealth() }
                                catch (cancelled: CancellationException) {
                                    if (cancelled !is TimeoutCancellationException) throw cancelled
                                    transportLost(); viewModel.closeForRecovery()
                                } catch (error: Exception) {
                                    if (isFatalConnectionFailure(error)) {
                                        policy.block()
                                        subscriptionError = "Health check failed: ${error.message}. Manual Connect required."
                                        viewModel.closeForRecovery()
                                    } else { transportLost(); viewModel.closeForRecovery() }
                                }
                                wake.trySend(Unit)
                            }
                        } else if (now() >= healthDue) until = now() + 5_000
                    }
                }
            }
            select<Unit> {
                wake.onReceive { }
                until?.let { deadline -> onTimeout((deadline - now()).coerceAtLeast(1)) { } }
            }
        }
    }
    override fun close() {
        scope.cancel()
        viewModel.closeForRecovery()
        foreground.stop()
    }
    private fun now() = SystemClock.elapsedRealtime()
}

internal fun isFatalConnectionFailure(error: Throwable): Boolean = generateSequence(error) { it.cause }.any {
    it is SecurityException || it is net.schmizz.sshj.userauth.UserAuthException ||
        it is AppServerException.DaemonStartFailed || it is AppServerException.IncompatibleDaemon ||
        it is AppServerException.WebSocketHandshakeFailed || it is com.codex.remote.data.rpc.RpcException
}

interface ForegroundMaintenance {
    fun start()
    fun stop()
}

private class AndroidForegroundMaintenance(private val context: Application) : ForegroundMaintenance {
    override fun start() { ContextCompat.startForegroundService(context, Intent(context, SshConnectionService::class.java)) }
    override fun stop() { context.stopService(Intent(context, SshConnectionService::class.java)) }
}

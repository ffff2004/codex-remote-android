package com.codex.remote.connection

/** Pure policy. Only a completed, failed dial consumes budget; observations never do. */
internal class ConnectionRecoveryPolicy {
    var epoch = 0L; private set
    var enabled = false; private set
    var failures = 0; private set
    var blocked = false; private set
    var networkId: Long? = null; private set
    private var observedNetwork = false
    private var available = true
    private var idle = false
    private var handoff = false
    private var rpcGraceStarted: Long? = null
    private var nextDialAt = 0L
    private var dialEpoch: Long? = null

    fun manualConnect(now: Long) {
        epoch++; enabled = true; failures = 0; blocked = false
        nextDialAt = now; handoff = false; rpcGraceStarted = null; dialEpoch = null
    }
    fun stop() { epoch++; enabled = false; handoff = false }
    fun block() { blocked = true; handoff = false }
    fun deviceIdle(value: Boolean) { idle = value }
    fun networkAvailable(id: Long, now: Long) {
        val changed = observedNetwork && (!available || networkId != id)
        observedNetwork = true; available = true; networkId = id
        if (changed) {
            epoch++; failures = 0; nextDialAt = now; handoff = true; rpcGraceStarted = null
        }
    }
    fun networkLost(id: Long) {
        if (networkId == id && available) { available = false; observedNetwork = true }
    }
    fun noNetwork() { available = false; observedNetwork = true }

    fun evaluate(now: Long, connected: Boolean, running: Boolean, approval: Boolean, rpc: Boolean): RecoveryDecision {
        if (!enabled) return RecoveryDecision.Wait("Maintenance stopped")
        if (blocked) return RecoveryDecision.Wait("Manual Connect required")
        if (idle) return RecoveryDecision.Wait("Recovery paused in Doze")
        if (observedNetwork && !available) return RecoveryDecision.Wait("Waiting for default network")
        if (dialEpoch != null) return RecoveryDecision.Wait("Connecting over SSH")
        if (connected) {
            if (!handoff) return RecoveryDecision.Wait("Foreground connection maintained")
            if (running || approval) { rpcGraceStarted = null; return RecoveryDecision.Wait("Network handoff deferred: running task or approval") }
            if (rpc) {
                val started = rpcGraceStarted ?: now.also { rpcGraceStarted = it }
                if (now - started < 5_000) return RecoveryDecision.Wait("Network handoff waiting for RPC", started + 5_000)
            }
            return RecoveryDecision.Handoff
        }
        if (failures >= 10) return RecoveryDecision.Wait("Recovery exhausted after 10 failed dials; Connect to retry")
        if (now < nextDialAt) return RecoveryDecision.Wait("Retry ${failures + 1}/10 in ${(nextDialAt - now + 999) / 1000}s", nextDialAt)
        return RecoveryDecision.Dial(epoch)
    }
    fun dialStarted(token: Long): Boolean {
        if (!enabled || blocked || token != epoch || dialEpoch != null || idle || (observedNetwork && !available) || failures >= 10) return false
        dialEpoch = token; handoff = false; rpcGraceStarted = null
        return true
    }
    fun dialFinished(token: Long, success: Boolean, fatal: Boolean, now: Long) {
        if (dialEpoch != token) return
        dialEpoch = null
        if (!enabled) return
        if (fatal) { block(); return }
        if (token != epoch) return
        if (success) { failures = 0; nextDialAt = now; return }
        failures++
        nextDialAt = now + backoff(failures - 1)
    }
    fun transportLost(now: Long) { nextDialAt = now + 1_000; handoff = false }
    fun handoffCompleted(now: Long) { handoff = false; rpcGraceStarted = null; nextDialAt = now }
    fun isCurrent(token: Long) = enabled && token == epoch
    companion object {
        fun backoff(attempt: Int): Long = (1_000L shl attempt.coerceIn(0, 5)).coerceAtMost(30_000)
    }
}

internal sealed interface RecoveryDecision {
    data class Dial(val epoch: Long) : RecoveryDecision
    data object Handoff : RecoveryDecision
    data class Wait(val message: String, val until: Long? = null) : RecoveryDecision
}

/** The service forwards platform events; it never owns another SSH client. */
interface ConnectionMaintenance {
    fun connectRequested(connection: com.codex.remote.domain.SavedConnection): Boolean
    fun isDesiredConnection(id: String): Boolean = false
    fun disconnectRequested()
    fun dialFailed(error: Throwable)
    fun transportLost()
    fun recoveryBlocked() {}
}

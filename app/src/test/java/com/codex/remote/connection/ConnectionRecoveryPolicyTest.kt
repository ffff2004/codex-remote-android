package com.codex.remote.connection

import org.junit.Assert.*
import org.junit.Test

class ConnectionRecoveryPolicyTest {
    private fun ConnectionRecoveryPolicy.action(now: Long = 0, connected: Boolean = false, running: Boolean = false, approval: Boolean = false, rpc: Boolean = false) = evaluate(now, connected, running, approval, rpc)

    @Test fun initialAndDuplicateNetworkCallbacksDoNotInvalidateInitialDial() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0)
        val token = (p.action() as RecoveryDecision.Dial).epoch
        assertTrue(p.dialStarted(token))
        p.networkAvailable(1, 0); p.networkAvailable(1, 0)
        assertEquals(token, p.epoch)
        assertTrue(p.action() is RecoveryDecision.Wait)
        p.dialFinished(token, true, false, 0)
        assertEquals("Foreground connection maintained", (p.action(connected = true) as RecoveryDecision.Wait).message)
    }
    @Test fun actualDefaultNetworkChangesIgnoreStaleLossAndOldDialCompletion() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.networkAvailable(1, 0)
        val old = p.epoch; assertTrue(p.dialStarted(old))
        p.networkAvailable(2, 0); p.networkLost(1)
        p.dialFinished(old, false, false, 0)
        assertFalse(p.blocked); assertEquals(0, p.failures)
        assertTrue(p.action() is RecoveryDecision.Dial)
        assertFalse(p.dialStarted(old))
    }
    @Test fun tenFailedActualDialsExhaustWithCappedBackoffAndNoObservationBudget() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.networkAvailable(1, 0)
        var now = 0L
        repeat(10) { index ->
            val decision = p.action(now) as RecoveryDecision.Dial
            assertEquals(index, p.failures)
            assertTrue(p.dialStarted(decision.epoch)); assertFalse(p.dialStarted(decision.epoch))
            p.dialFinished(decision.epoch, false, false, now)
            val delay = ConnectionRecoveryPolicy.backoff(index)
            assertEquals(listOf(1000L,2000L,4000L,8000L,16000L,30000L,30000L,30000L,30000L,30000L)[index], delay)
            assertTrue(p.action(now) is RecoveryDecision.Wait)
            now += delay
        }
        val exhausted = p.action(now) as RecoveryDecision.Wait
        assertNull(exhausted.until); assertTrue(exhausted.message.contains("exhausted"))
        p.networkAvailable(1, now); assertEquals(10, p.failures)
        p.manualConnect(now); assertTrue(p.action(now) is RecoveryDecision.Dial)
    }
    @Test fun dozeAndNoNetworkPauseDoNotBurnDialsAndChangedNetworkRestartsBudget() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.noNetwork()
        repeat(20) { assertTrue(p.action(it * 1000L) is RecoveryDecision.Wait) }
        assertEquals(0, p.failures)
        p.networkAvailable(1, 20_000); p.deviceIdle(true)
        assertFalse(p.dialStarted(p.epoch)); assertTrue(p.action(99_000) is RecoveryDecision.Wait)
        p.deviceIdle(false); assertTrue(p.dialStarted(p.epoch))
        p.dialFinished(p.epoch, false, false, 99_000)
        p.networkAvailable(2, 99_001); assertEquals(0, p.failures)
        assertTrue(p.action(99_001) is RecoveryDecision.Dial)
    }
    @Test fun fatalFailureRequiresManualActionDespiteDozeAndNetworkChanges() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); assertTrue(p.dialStarted(p.epoch))
        p.dialFinished(p.epoch, false, true, 0)
        p.networkAvailable(1, 0); p.networkAvailable(2, 0); p.deviceIdle(true); p.deviceIdle(false)
        assertTrue(p.blocked); assertFalse(p.dialStarted(p.epoch))
        p.manualConnect(0); assertFalse(p.blocked); assertTrue(p.action() is RecoveryDecision.Dial)
    }
    @Test fun fatalResultFromDialBeforeNetworkChangeStillRequiresManualAction() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.networkAvailable(1, 0)
        val token = p.epoch; assertTrue(p.dialStarted(token)); p.networkAvailable(2, 0)
        p.dialFinished(token, false, true, 0)
        assertTrue(p.blocked); assertTrue(p.action() is RecoveryDecision.Wait)
    }
    @Test fun runningAndApprovalDeferHandoffButRpcHasFiveSecondGrace() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.networkAvailable(1, 0); p.networkAvailable(2, 0)
        assertTrue(p.action(0, connected = true, running = true) is RecoveryDecision.Wait)
        assertTrue(p.action(100_000, connected = true, approval = true) is RecoveryDecision.Wait)
        assertEquals(105_000L, (p.action(100_000, connected = true, rpc = true) as RecoveryDecision.Wait).until)
        assertTrue(p.action(104_999, connected = true, rpc = true) is RecoveryDecision.Wait)
        assertEquals(RecoveryDecision.Handoff, p.action(105_000, connected = true, rpc = true))
        p.handoffCompleted(105_000); assertTrue(p.action(105_000) is RecoveryDecision.Dial)
    }
    @Test fun genuineLossDoesNotDeferToRunningOrApprovalAndStopInvalidatesTimers() {
        val p = ConnectionRecoveryPolicy(); p.manualConnect(0); p.transportLost(0)
        val dial = p.action(1000, running = true, approval = true) as RecoveryDecision.Dial
        p.stop(); assertFalse(p.dialStarted(dial.epoch)); assertTrue(p.action(1000) is RecoveryDecision.Wait)
    }
}

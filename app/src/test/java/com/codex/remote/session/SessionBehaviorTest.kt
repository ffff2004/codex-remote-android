package com.codex.remote.session

import com.codex.remote.data.rpc.AppServerEvent
import com.codex.remote.domain.*
import org.junit.Assert.*
import org.junit.Test

class SessionBehaviorTest {
    private fun registry(limits: SessionLimits = SessionLimits()) = SessionRegistry(limits).also {
        assertTrue(it.register("a", RemoteThread("a", "A", "/a", 1, "")).applied)
        assertTrue(it.register("b", RemoteThread("b", "B", "/b", 1, "")).applied)
        it.select("a")
    }

    @Test fun backgroundEventsKeepExactTaskTimelineAndUnread() {
        val registry = registry()
        val router = SessionEventRouter(SessionRequestTracker())
        router.route(registry, AppServerEvent.TurnRunning("a", true, "ta"))
        registry.select("b")
        router.route(registry, AppServerEvent.AgentDelta("a", "ia", "A output", "ta"))
        router.route(registry, AppServerEvent.AgentDelta("b", "ib", "B output", "tb"))
        assertEquals("A output", registry.sessions.getValue("a").timeline.single().body)
        assertEquals("B output", registry.selected!!.timeline.single().body)
        assertEquals(1, registry.sessions.getValue("a").unreadCount)
        assertTrue(registry.sessions.getValue("a").isTurnRunning)
        registry.select("a")
        assertEquals(0, registry.selected!!.unreadCount)
    }

    @Test fun unknownMissingAndStaleOwnerCannotPolluteSelectedTask() {
        val registry = registry()
        val router = SessionEventRouter(SessionRequestTracker())
        router.route(registry, AppServerEvent.TurnRunning("a", true, "ta"))
        val before = registry.sessions
        listOf(AppServerEvent.AgentDelta(null, "i", "bad", "ta"),
            AppServerEvent.AgentDelta("unknown", "i", "bad", "ta"),
            AppServerEvent.AgentDelta("a", "i", "bad", "old"),
            AppServerEvent.TurnRunning("a", false, "old"),
            AppServerEvent.ItemUpsert("a", TimelineItem("i", TimelineKind.AGENT, body = "bad")))
            .forEach { assertFalse(router.route(registry, it)!!.applied) }
        assertEquals(before, registry.sessions)
    }

    @Test fun completionBeforeResponseAndLateCompletionNeverReopenNewTurn() {
        val registry = registry()
        val requests = SessionRequestTracker()
        val router = SessionEventRouter(requests)
        val token = requests.beginTurnStart("a", "local-phone")
        router.route(registry, AppServerEvent.TurnRunning("a", true, "first"))
        router.route(registry, AppServerEvent.TurnRunning("a", false, "first"))
        assertEquals(TurnStartResponseDisposition.TERMINAL, requests.resolveTurnStartResponse(token, "first"))
        router.route(registry, AppServerEvent.TurnRunning("a", true, "second"))
        assertFalse(router.route(registry, AppServerEvent.TurnRunning("a", false, "first"))!!.applied)
        assertEquals("second", registry.selected!!.activeTurnId)
    }

    @Test fun matchingBodiesNeedDifferentClientIdsAndMissingEchoRemainsSeparate() {
        val registry = registry()
        val router = SessionEventRouter(SessionRequestTracker())
        registry.update("a") { it.copy(timeline = listOf(TimelineItem("local-phone", TimelineKind.USER,
            body = "same", clientId = "local-phone"))) }
        router.route(registry, AppServerEvent.ItemUpsert("a", TimelineItem("desktop", TimelineKind.USER,
            body = "same", turnId = "t", clientId = "desktop-client")))
        assertEquals(2, registry.selected!!.timeline.size)
        router.route(registry, AppServerEvent.ItemUpsert("a", TimelineItem("noecho", TimelineKind.USER,
            body = "same", turnId = "t")))
        assertEquals(3, registry.selected!!.timeline.size)
        router.route(registry, AppServerEvent.ItemUpsert("a", TimelineItem("phone", TimelineKind.USER,
            body = "same", turnId = "t", clientId = "local-phone")))
        assertEquals(listOf("phone", "desktop", "noecho"), registry.selected!!.timeline.map { it.id })
    }

    @Test fun oversizedMutationIsAtomicAndSelectedTaskCannotBeEvicted() {
        val registry = registry(SessionLimits(maxTimelineItems = 1, maxAggregateChars = 1000))
        registry.update("a") { it.copy(timeline = listOf(TimelineItem("i", TimelineKind.USER, body = "good"))) }
        val before = registry.sessions
        val result = registry.update("a") { it.copy(timeline = it.timeline + TimelineItem("j", TimelineKind.USER)) }
        assertFalse(result.applied)
        assertTrue(result.disconnect)
        assertEquals(before, registry.sessions)
    }

    @Test fun evictionPlanIsAtomicWhenRemainingWorkIsProtected() {
        val registry = registry(SessionLimits(maxAggregateChars = 200, maxSessions = 2))
        registry.update("b") { it.copy(isTurnRunning = true) }
        val before = registry.sessions
        val result = registry.update("a") { it.copy(timeline = listOf(TimelineItem("large", TimelineKind.AGENT, body = "x".repeat(300)))) }
        assertFalse(result.applied)
        assertEquals(before, registry.sessions)
        assertFalse(registry.register("c").applied)
        assertEquals(before, registry.sessions)
    }

    @Test fun onlyEligibleInactiveSessionIsEvicted() {
        val registry = registry(SessionLimits(maxSessions = 3))
        registry.update("b") { it.copy(hasApproval = true) }
        registry.register("c")
        registry.register("d")
        assertEquals(setOf("a", "b", "d"), registry.sessions.keys)
    }

    @Test fun goalAndPendingWriteProtectSessionFromEviction() {
        val registry = registry(SessionLimits(maxSessions = 2))
        registry.update("b") { it.copy(pendingWrites = 1) }
        assertFalse(registry.register("c").applied)
        registry.update("b") { it.copy(pendingWrites = 0, goal = ThreadGoal("b", "goal", ThreadGoalStatus.PAUSED, null, 0, 0, 0, 0)) }
        assertFalse(registry.register("c").applied)
    }

    @Test fun cursorBudgetRejectsWithoutDiscardingHistory() {
        val registry = registry(SessionLimits(maxHistoryCursors = 1))
        registry.update("a") { it.copy(olderHistoryCursor = "first") }
        val before = registry.sessions
        assertFalse(registry.update("a") { it.copy(consumedHistoryCursors = setOf("other")) }.applied)
        assertEquals(before, registry.sessions)
    }

    @Test fun metadataImportDoesNotOverwriteResumedSettingsOrActiveWork() {
        val registry = registry()
        val settings = SessionSettings(model = "phone-model", locallyConfigured = true,
            permissionProfile = ":danger-full-access", fullAccessGranted = true, approvalPolicy = "never")
        registry.update("a") { it.copy(settings = settings, isTurnRunning = true, activeTurnId = "turn", resumed = true) }
        registry.register("a", RemoteThread("a", "renamed", "/a", 2, "idle", true))
        assertEquals(settings, registry.selected!!.settings)
        assertTrue(registry.selected!!.isTurnRunning)
        assertEquals("renamed", registry.selected!!.thread!!.title)
        assertTrue(registry.selected!!.thread!!.isPinned)
    }

    @Test fun remoteGrantNeverRegrantsLocalPermissionsAndSettingsAreIsolated() {
        val registry = registry()
        val router = SessionEventRouter(SessionRequestTracker())
        router.route(registry, AppServerEvent.ThreadSettingsUpdated("a", RemoteThreadSettingsSnapshot("model", "high", null,
            "plan", ":danger-full-access", "never", "user")))
        assertEquals(":workspace", registry.sessions.getValue("a").settings.permissionProfile)
        assertEquals("on-request", registry.sessions.getValue("a").settings.approvalPolicy)
        assertNull(registry.sessions.getValue("b").settings.model)
        registry.update("a") { it.copy(composer = TaskComposer("draft A")) }
        registry.select("b")
        assertEquals("", registry.selected!!.composer.text)
        registry.select("a")
        assertEquals("draft A", registry.selected!!.composer.text)
    }

    @Test fun authoritativeSnapshotReplacesCachedBodyAndOnlyExactOptimisticIdentity() {
        val cached = listOf(TimelineItem("message", TimelineKind.AGENT, body = "old", turnId = "t"),
            TimelineItem("local-phone", TimelineKind.USER, body = "same", clientId = "local-phone"))
        val snapshot = listOf(TimelineItem("message", TimelineKind.AGENT, body = "snapshot", turnId = "t"),
            TimelineItem("desktop", TimelineKind.USER, body = "same", turnId = "t", clientId = "desktop"))
        val result = reconcileSessionTimeline(cached, snapshot)
        assertEquals(3, result.size)
        assertEquals("snapshot", result.first().body)
        assertTrue(result.any { it.id == "local-phone" })
    }

    @Test fun oversizedComposerMutationIsRejectedAtomically() {
        val registry = registry(SessionLimits(maxAggregateChars = 200))
        val before = registry.sessions
        assertFalse(registry.update("a", authoritative = false) { it.copy(composer = TaskComposer("x".repeat(1000))) }.applied)
        assertEquals(before, registry.sessions)
    }
}

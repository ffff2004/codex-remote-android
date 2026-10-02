package com.codex.remote.session

import com.codex.remote.data.rpc.AppServerEvent
import com.codex.remote.domain.*

/** Exact wire ownership only. Approval review remains in the RPC client's frozen live cache. */
internal class SessionEventRouter(private val requests: SessionRequestTracker) {
    fun route(registry: SessionRegistry, event: AppServerEvent, knownThreads: Map<String, RemoteThread> = emptyMap()): SessionMutation? {
        val threadId: String? = when (event) {
            is AppServerEvent.ItemUpsert -> event.threadId
            is AppServerEvent.AgentDelta -> event.threadId
            is AppServerEvent.PlanDelta -> event.threadId
            is AppServerEvent.ReasoningDelta -> event.threadId
            is AppServerEvent.OutputDelta -> event.threadId
            is AppServerEvent.TurnRunning -> event.threadId
            is AppServerEvent.GoalUpdated -> event.threadId
            is AppServerEvent.GoalCleared -> event.threadId
            is AppServerEvent.TokenUsageUpdated -> event.threadId
            is AppServerEvent.ThreadSettingsUpdated -> event.threadId
            is AppServerEvent.ContextCompacted -> event.threadId
            is AppServerEvent.Failure -> if (event.threadId == null) return null else event.threadId
            else -> return null
        }
        if (threadId.isNullOrBlank() || threadId.length > 4096) return SessionMutation(false, "Ignored event without exact task owner")
        if (threadId !in registry.sessions) {
            val known = knownThreads[threadId] ?: return SessionMutation(false, "Ignored event for unknown task $threadId")
            val registered = registry.register(threadId, known)
            if (!registered.applied) return registered.copy(disconnect = true)
        }
        val session = registry.sessions.getValue(threadId)
        fun accepts(turnId: String?): Boolean = !turnId.isNullOrBlank() && turnId.length <= 4096 &&
            requests.canObserveTurnStarted(threadId, turnId) &&
            (session.activeTurnId == null || session.activeTurnId == turnId)
        fun delta(turnId: String?, itemId: String, text: String, kind: TimelineKind): SessionMutation {
            if (itemId.isBlank() || !accepts(turnId)) return SessionMutation(false, "Ignored stale or unowned item delta")
            return registry.update(threadId, unread = true) {
                val index = it.timeline.indexOfFirst { item -> item.id == itemId && item.turnId == turnId }
                val items = it.timeline.toMutableList()
                if (index < 0) items += TimelineItem(itemId, kind, body = text, status = "inProgress", turnId = turnId)
                else items[index] = items[index].copy(body = items[index].body + text)
                it.copy(timeline = items, isTurnRunning = true, activeTurnId = turnId, liveRevision = it.liveRevision + 1)
            }
        }
        return when (event) {
            is AppServerEvent.ItemUpsert -> {
                val item = event.item
                if (item.id.isBlank() || !accepts(item.turnId)) return SessionMutation(false, "Ignored stale or unowned timeline item")
                if (item.kind == TimelineKind.USER && item.clientId != null &&
                    !requests.observeUserMessage(threadId, item.turnId!!, item.clientId)) return SessionMutation(false, "Ignored stale user item")
                registry.update(threadId, unread = true) {
                    val items = it.timeline.toMutableList()
                    val index = items.indexOfFirst { existing -> existing.id == item.id && existing.turnId == item.turnId }
                    val local = if (item.kind == TimelineKind.USER && item.clientId != null) items.indexOfFirst { existing ->
                        existing.id.startsWith("local-") && existing.kind == TimelineKind.USER && existing.clientId == item.clientId
                    } else -1
                    when {
                        index >= 0 -> { items[index] = item; if (local >= 0 && local != index) items.removeAt(local) }
                        local >= 0 -> items[local] = item
                        else -> items += item
                    }
                    it.copy(timeline = items, activeTurnId = item.turnId, liveRevision = it.liveRevision + 1,
                        isTurnRunning = true, failed = false)
                }
            }
            is AppServerEvent.AgentDelta -> delta(event.turnId, event.itemId, event.delta, TimelineKind.AGENT)
            is AppServerEvent.PlanDelta -> delta(event.turnId, event.itemId, event.delta, TimelineKind.PLAN)
            is AppServerEvent.ReasoningDelta -> delta(event.turnId, event.itemId, event.delta, TimelineKind.REASONING)
            is AppServerEvent.OutputDelta -> delta(event.turnId, event.itemId, event.delta, TimelineKind.COMMAND)
            is AppServerEvent.TurnRunning -> {
                if (!accepts(event.turnId)) return SessionMutation(false, "Ignored stale or unowned turn event")
                val turnId = event.turnId!!
                if (event.running) requests.observeTurnStarted(threadId, turnId) else requests.observeTurnCompleted(threadId, turnId)
                registry.update(threadId, unread = true) {
                    it.copy(isTurnRunning = event.running, activeTurnId = if (event.running) turnId else null, liveRevision = it.liveRevision + 1,
                        timeline = if (event.running) it.timeline else it.timeline.map { item ->
                            if (item.turnId == turnId && item.status in setOf("inProgress", "running", "started", "in_progress")) item.copy(status = "completed") else item
                        })
                }
            }
            is AppServerEvent.GoalUpdated -> {
                if (event.goal.threadId != threadId) return SessionMutation(false, "Ignored mismatched goal owner")
                registry.update(threadId, unread = true) { it.copy(goal = event.goal, isGoalLoading = false, goalError = null) }
            }
            is AppServerEvent.GoalCleared -> registry.update(threadId, unread = true) { it.copy(goal = null, isGoalLoading = false, goalError = null) }
            is AppServerEvent.TokenUsageUpdated -> registry.update(threadId) { it.copy(tokenUsage = event.usage) }
            is AppServerEvent.ThreadSettingsUpdated -> registry.update(threadId) { it.copy(settings = it.settings.mergeRemote(event.settings)) }
            is AppServerEvent.ContextCompacted -> registry.update(threadId, unread = true) {
                it.copy(timeline = it.timeline + TimelineItem("compaction-${it.revision}", TimelineKind.COMPACTION, title = "Context compacted"))
            }
            is AppServerEvent.Failure -> {
                if (!accepts(event.turnId)) return SessionMutation(false, "Ignored failure without exact current turn")
                requests.observeTurnCompleted(threadId, event.turnId!!)
                registry.update(threadId, unread = true) { it.copy(failed = true, isTurnRunning = false, activeTurnId = null,
                    timeline = it.timeline + TimelineItem("failure-${it.revision}", TimelineKind.ERROR, body = event.message, turnId = event.turnId)) }
            }
            else -> null
        }
    }
}

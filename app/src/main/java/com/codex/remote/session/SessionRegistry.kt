package com.codex.remote.session

import com.codex.remote.domain.*

internal data class SessionLimits(
    val maxSessions: Int = 32,
    val maxTimelineItems: Int = 512,
    val maxTimelineChars: Long = 4L * 1024 * 1024,
    val maxAggregateItems: Int = 2048,
    val maxAggregateChars: Long = 16L * 1024 * 1024,
    val maxHistoryCursors: Int = 128,
    val maxAggregateHistoryCursors: Int = 512,
    val maxCursorChars: Int = 2048,
) {
    init {
        require(maxSessions > 0 && maxTimelineItems > 0 && maxTimelineChars > 0 &&
            maxAggregateItems > 0 && maxAggregateChars > 0 && maxHistoryCursors > 0 &&
            maxAggregateHistoryCursors > 0 && maxCursorChars > 0)
    }
}

internal data class SessionSettings(
    val model: String? = null,
    val reasoningEffort: String? = null,
    val serviceTier: String? = null,
    val collaborationMode: String = "default",
    val permissionProfile: String? = ":workspace",
    val approvalPolicy: String = "on-request",
    val approvalsReviewer: String = "user",
    val fullAccessGranted: Boolean = false,
    val locallyConfigured: Boolean = false,
) {
    // Remote metadata may describe a desktop grant. It is never a local authorization.
    fun mergeRemote(snapshot: RemoteThreadSettingsSnapshot): SessionSettings = if (locallyConfigured) this else copy(
        model = snapshot.model ?: model,
        reasoningEffort = snapshot.reasoningEffort ?: reasoningEffort,
        serviceTier = snapshot.serviceTier,
        collaborationMode = snapshot.collaborationMode ?: collaborationMode,
    )
}

internal data class SessionState(
    val threadId: String,
    val thread: RemoteThread? = null,
    val timeline: List<TimelineItem> = emptyList(),
    val olderHistoryCursor: String? = null,
    val hasOlderHistory: Boolean = false,
    val isOlderHistoryLoading: Boolean = false,
    val olderHistoryError: String? = null,
    val consumedHistoryCursors: Set<String> = emptySet(),
    val goal: ThreadGoal? = null,
    val isGoalLoading: Boolean = false,
    val goalError: String? = null,
    val tokenUsage: RemoteThreadTokenUsage? = null,
    val settings: SessionSettings = SessionSettings(),
    val composer: TaskComposer = TaskComposer(),
    val isTurnRunning: Boolean = false,
    val activeTurnId: String? = null,
    val pendingWrites: Int = 0,
    val hasApproval: Boolean = false,
    val failed: Boolean = false,
    val unreadCount: Int = 0,
    val resumed: Boolean = false,
    val liveRevision: Long = 0,
    val revision: Long = 0,
    val touched: Long = 0,
) {
    val protected: Boolean get() = isTurnRunning || pendingWrites > 0 || hasApproval ||
        goal?.status?.let { it != ThreadGoalStatus.COMPLETE } == true
    val cursors: Set<String> get() = consumedHistoryCursors + listOfNotNull(olderHistoryCursor)
    val retainedChars: Long get() = timeline.sumOf { it.retainedChars } +
        threadId.length + (thread?.let { it.title.length + it.cwd.length + it.status.length } ?: 0) +
        cursors.sumOf { it.length.toLong() } + (goal?.objective?.length ?: 0) +
        (olderHistoryError?.length ?: 0) + (goalError?.length ?: 0) +
        composer.text.length + composer.attachments.sumOf { it.dataUrl.length.toLong() + it.displayName.length } +
        composer.mentions.sumOf { (it.name.length + it.path.length + it.token.length).toLong() } +
        listOfNotNull(settings.model, settings.reasoningEffort, settings.serviceTier, settings.collaborationMode,
            settings.permissionProfile, settings.approvalPolicy, settings.approvalsReviewer, activeTurnId).sumOf { it.length.toLong() }
}

internal val TimelineItem.retainedChars: Long get() =
    (id.length.toLong() + title.length + body.length + status.length + (turnId?.length ?: 0) + (clientId?.length ?: 0)) +
        fileChanges.sumOf { it.path.length.toLong() + it.kind.length + it.diff.length + (it.movePath?.length ?: 0) }

internal data class SessionMutation(val applied: Boolean, val message: String? = null, val disconnect: Boolean = false)

/** All retained changes commit through one validated transaction. No generated copy bypass. */
internal class SessionRegistry(private val limits: SessionLimits = SessionLimits()) {
    private var retained = linkedMapOf<String, SessionState>()
    val sessions: Map<String, SessionState> get() = retained.toMap()
    var selectedThreadId: String? = null
        private set
    val selected: SessionState? get() = selectedThreadId?.let(retained::get)
    private var sequence = 0L

    fun register(threadId: String, thread: RemoteThread? = null): SessionMutation {
        if (threadId.isBlank() || threadId.length > 4096) return SessionMutation(false, "Task metadata has no bounded exact ID")
        return commit(threadId, retained[threadId]?.copy(thread = thread ?: retained[threadId]?.thread)
            ?: SessionState(threadId, thread, isTurnRunning = thread?.status in setOf("active", "inProgress")), authoritative = false, unread = false)
    }

    fun select(threadId: String): SessionMutation {
        if (threadId !in retained) return SessionMutation(false, "Task is not cached")
        selectedThreadId = threadId
        retained[threadId] = retained.getValue(threadId).copy(unreadCount = 0, touched = ++sequence)
        return SessionMutation(true)
    }

    fun clearSelection() { selectedThreadId = null }
    fun reset() { retained.clear(); selectedThreadId = null; sequence = 0 }

    fun update(threadId: String, authoritative: Boolean = true, unread: Boolean = false,
               transform: (SessionState) -> SessionState): SessionMutation {
        val current = retained[threadId] ?: return SessionMutation(false, "Unknown exact task owner: $threadId", authoritative)
        return commit(threadId, transform(current), authoritative, unread)
    }

    private fun commit(threadId: String, candidate: SessionState, authoritative: Boolean, unread: Boolean): SessionMutation {
        require(candidate.threadId == threadId)
        if (candidate.activeTurnId?.length?.let { it > 4096 } == true ||
            candidate.timeline.any { it.id.isBlank() || it.id.length > 4096 || (it.turnId?.length ?: 0) > 4096 || (it.clientId?.length ?: 0) > 4096 } ||
            candidate.timeline.size > limits.maxTimelineItems || candidate.timeline.sumOf { it.retainedChars } > limits.maxTimelineChars ||
            candidate.cursors.size > limits.maxHistoryCursors || candidate.cursors.any { it.length > limits.maxCursorChars }) {
            return SessionMutation(false, "Task retention capacity exceeded; disconnect and recover manually", authoritative)
        }
        val plan = LinkedHashMap(retained)
        val stored = candidate.copy(revision = candidate.revision + 1, touched = sequence + 1,
            unreadCount = if (unread && selectedThreadId != threadId) (candidate.unreadCount + 1).coerceAtMost(99) else candidate.unreadCount)
        plan[threadId] = stored
        fun exceeds() = plan.size > limits.maxSessions || plan.values.sumOf { it.timeline.size } > limits.maxAggregateItems ||
            plan.values.sumOf { it.retainedChars } > limits.maxAggregateChars ||
            plan.values.sumOf { it.cursors.size } > limits.maxAggregateHistoryCursors
        while (exceeds()) {
            val evict = plan.values.filter { it.threadId != threadId && it.threadId != selectedThreadId && !it.protected }
                .minByOrNull { it.touched } ?: return SessionMutation(false, "All cached tasks have protected work; capacity exceeded", authoritative)
            plan.remove(evict.threadId)
        }
        retained = plan
        sequence++
        return SessionMutation(true)
    }
}

/** Authoritative snapshots reconcile only the client identity echoed by the server. */
internal fun reconcileSessionTimeline(cached: List<TimelineItem>, snapshot: List<TimelineItem>): List<TimelineItem> {
    val echoedClients = snapshot.filter { it.kind == TimelineKind.USER }.mapNotNull { it.clientId }.toSet()
    return mergeTimelineHistory(cached.filterNot { it.id.startsWith("local-") && it.clientId in echoedClients }, snapshot)
}

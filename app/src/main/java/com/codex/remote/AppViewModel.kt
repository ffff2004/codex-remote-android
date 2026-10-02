package com.codex.remote

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.codex.remote.data.rpc.AppServerEvent
import com.codex.remote.data.rpc.CodexRpcClient
import com.codex.remote.data.rpc.ThreadPage
import com.codex.remote.data.runtime.AppServerException
import com.codex.remote.data.runtime.AppServerRuntime
import com.codex.remote.data.runtime.SshCodexAppServerRuntime
import com.codex.remote.data.runtime.UnknownHostKeyException
import com.codex.remote.data.store.ConnectionStore
import com.codex.remote.session.ResumeEventEpoch
import com.codex.remote.session.SessionLoadPurpose
import com.codex.remote.session.ResumeEventCoordinator
import com.codex.remote.session.ResumeEventOfferDisposition
import com.codex.remote.session.ResumeEventDrainDisposition
import com.codex.remote.session.SessionRegistry
import com.codex.remote.session.SessionRequestTracker
import com.codex.remote.session.SessionEventRouter
import com.codex.remote.session.SessionSettings
import com.codex.remote.session.SessionState
import com.codex.remote.session.SessionMutation
import com.codex.remote.session.TurnStartToken
import com.codex.remote.session.TurnStartResponseDisposition
import com.codex.remote.session.retainedChars
import com.codex.remote.session.reconcileSessionTimeline
import com.codex.remote.domain.FullAccessConfirmation
import com.codex.remote.domain.TaskComposer
import com.codex.remote.domain.TaskRecoveryTarget
import com.codex.remote.domain.RemoteThreadSession
import com.codex.remote.domain.TaskIndicator
import com.codex.remote.domain.RemoteThreadSettingsSnapshot
import com.codex.remote.domain.AppUiState
import com.codex.remote.domain.ApprovalKind
import com.codex.remote.domain.ApprovalQueue
import com.codex.remote.domain.ApprovalQueueKey
import com.codex.remote.domain.ApprovalDelivery
import com.codex.remote.domain.ApprovalEnqueueStatus
import com.codex.remote.domain.ApprovalFileItemKey
import com.codex.remote.domain.canRespond
import com.codex.remote.domain.fileApprovalSnapshotOrNull
import com.codex.remote.domain.approvalFileSnapshotRetainedCharCount
import com.codex.remote.domain.ConnectionDraft
import com.codex.remote.domain.ConnectionStatus
import com.codex.remote.domain.ComposerMention
import com.codex.remote.domain.ComposerMentionKind
import com.codex.remote.domain.ComposerImageAttachment
import com.codex.remote.domain.PermissionMode
import com.codex.remote.domain.RemoteCollaborationMode
import com.codex.remote.domain.RemoteProject
import com.codex.remote.domain.RemoteAccount
import com.codex.remote.domain.RemoteModel
import com.codex.remote.domain.RemotePathEntry
import com.codex.remote.domain.RemoteServerInfo
import com.codex.remote.domain.RemoteThread
import com.codex.remote.domain.ReviewTargetKind
import com.codex.remote.domain.SavedConnection
import com.codex.remote.domain.TimelineItem
import com.codex.remote.domain.TimelineKind
import com.codex.remote.domain.ThreadGoalStatus
import com.codex.remote.domain.groupThreadsByProject
import com.codex.remote.domain.mergeTimelineHistory
import com.codex.remote.domain.composerToken
import com.codex.remote.domain.containsComposerToken
import com.codex.remote.domain.withThreadArchived
import com.codex.remote.domain.withThreadRenamed
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.UUID

class AppViewModel @JvmOverloads constructor(
    application: Application,
    private val runtime: AppServerRuntime = SshCodexAppServerRuntime(application),
    private val restoreLastConnection: Boolean = true,
    private val maintenance: com.codex.remote.connection.ConnectionMaintenance? = null,
) : AndroidViewModel(application) {
    private val store = ConnectionStore(application)
    private val _state = MutableStateFlow(AppUiState())
    val state: StateFlow<AppUiState> = _state.asStateFlow()

    private val sessions = SessionRegistry()
    private val requests = SessionRequestTracker()
    private val eventRouter = SessionEventRouter(requests)
    private val resumeEvents = ResumeEventCoordinator<AppServerEvent>(256, 4L * 1024 * 1024) { event ->
        when (event) {
            is AppServerEvent.ItemUpsert -> event.item.retainedChars + (event.threadId?.length ?: 0)
            is AppServerEvent.AgentDelta -> event.delta.length.toLong() + event.itemId.length + (event.threadId?.length ?: 0) + (event.turnId?.length ?: 0)
            is AppServerEvent.PlanDelta -> event.delta.length.toLong() + event.itemId.length + (event.threadId?.length ?: 0) + (event.turnId?.length ?: 0)
            is AppServerEvent.ReasoningDelta -> event.delta.length.toLong() + event.itemId.length + (event.threadId?.length ?: 0) + (event.turnId?.length ?: 0)
            is AppServerEvent.OutputDelta -> event.delta.length.toLong() + event.itemId.length + (event.threadId?.length ?: 0) + (event.turnId?.length ?: 0)
            is AppServerEvent.Approval -> event.request.retainedCharCount.toLong()
            else -> event.toString().length.toLong()
        }
    }
    private var sessionHostKey: String? = null
    private var selectionRevision = 0L
    private var pendingOperations = 0
    val hasRunningTasks: Boolean get() = sessions.sessions.values.any { it.isTurnRunning }
    val hasPendingApprovals: Boolean get() = _state.value.approvalQueue.entries.isNotEmpty()
    val hasPendingRpc: Boolean get() = (rpc?.pendingRequestCount ?: 0) > 0 || pendingOperations > 0 || sessions.sessions.values.any { it.pendingWrites > 0 }
    val selectedTaskForRecovery: String? get() = sessions.selectedThreadId
    val tasksForRecovery: List<TaskRecoveryTarget> get() = sessions.sessions.values
        .filter { it.resumed || it.protected || it.threadId == sessions.selectedThreadId }
        .map { TaskRecoveryTarget(it.threadId, it.thread?.cwd.orEmpty(), it.isTurnRunning,
            it.goal?.status?.let { status -> status != ThreadGoalStatus.COMPLETE } == true,
            it.hasApproval, it.pendingWrites > 0) }

    private var rpc: CodexRpcClient? = null
    private var eventJob: Job? = null
    private var connectionJob: Job? = null
    private var threadListJob: Job? = null
    private var composerCatalogJob: Job? = null
    private var threadRefreshPending = false
    private val threadListEdits = mutableMapOf<String, RemoteThread?>()
    private var connectionGeneration = 0L
    private var didRestoreLastConnection = false

    init {
        viewModelScope.launch {
            store.connections.collect { connections ->
                val sortedConnections = connections.sortedByDescending { it.lastUsedAt }
                val connectionToRestore = if (restoreLastConnection && !didRestoreLastConnection) {
                    didRestoreLastConnection = true
                    sortedConnections.lastUsedConnectionOrNull()
                } else {
                    null
                }
                val restoreConnection = connectionToRestore?.takeIf { _state.value.activeConnection == null }
                _state.update { current ->
                    val refreshedActive = current.activeConnection?.let { active ->
                        connections.firstOrNull { it.id == active.id } ?: active
                    }
                    current.copy(
                        savedConnections = sortedConnections,
                        activeConnection = restoreConnection ?: refreshedActive,
                        connectionStatus = if (restoreConnection != null) ConnectionStatus.CONNECTING else current.connectionStatus,
                        connectionMessage = if (restoreConnection != null) {
                            "正在连接 ${restoreConnection.host}…"
                        } else {
                            current.connectionMessage
                        },
                        isRestoringLastConnection = false,
                    )
                }
                restoreConnection?.let(::connect)
            }
        }
    }

    fun showConnections(show: Boolean = true) = _state.update {
        it.copy(showConnections = show, showConnectionEditor = false, editingConnection = null)
    }

    fun editConnection(connection: SavedConnection? = null) = _state.update {
        it.copy(showConnections = true, showConnectionEditor = true, editingConnection = connection, notice = null)
    }

    fun closeEditor() = _state.update { it.copy(showConnectionEditor = false, editingConnection = null) }

    fun saveConnection(draft: ConnectionDraft, connectAfterSave: Boolean) {
        viewModelScope.launch {
            _state.update { it.copy(isBusy = true, notice = null) }
            runCatching {
                store.save(draft, _state.value.editingConnection)
            }.onSuccess { saved ->
                _state.update {
                    it.copy(
                        isBusy = false,
                        showConnectionEditor = false,
                        editingConnection = null,
                        showConnections = !connectAfterSave,
                    )
                }
                if (connectAfterSave) connect(saved)
            }.onFailure(::showError)
        }
    }

    fun deleteConnection(connection: SavedConnection) {
        viewModelScope.launch {
            if (_state.value.activeConnection?.id == connection.id || maintenance?.isDesiredConnection(connection.id) == true) disconnect()
            store.delete(connection.id)
        }
    }

    fun connect(connection: SavedConnection, restoreSelectedTaskId: String? = null, preserveTaskState: Boolean = false, automaticRecovery: Boolean = false) {
        if (automaticRecovery && _state.value.recoveryBlocked) return
        if (!automaticRecovery) {
            _state.update { it.copy(recoveryBlocked = false) }
            if (maintenance?.connectRequested(connection) == false) {
                ++connectionGeneration
                connectionJob?.cancel()
                disconnectInternal(clearActive = false)
                _state.update { it.copy(activeConnection = connection, connectionStatus = ConnectionStatus.CONNECTING,
                    connectionMessage = "等待网络或设备唤醒后连接…", showConnections = false) }
                return
            }
        }
        val generation = ++connectionGeneration
        val previous = connectionJob
        previous?.cancel()
        rpc?.close()
        connectionJob = viewModelScope.launch {
            previous?.join()
            if (generation != connectionGeneration) return@launch
            val keep = preserveTaskState && sessionHostKey == connection.exactSessionHostKey()
            disconnectInternal(clearActive = false, preserveTaskState = keep)
            sessionHostKey = connection.exactSessionHostKey()
            _state.update {
                it.copy(
                    activeConnection = connection,
                    connectionStatus = ConnectionStatus.CONNECTING,
                    connectionMessage = "正在连接 ${connection.host}…",
                    showConnections = false,
                    timeline = emptyList(),
                    olderHistoryCursor = null,
                    hasOlderHistory = false,
                    isOlderHistoryLoading = false,
                    olderHistoryError = null,
                    consumedHistoryCursors = emptySet(),
                    threads = emptyList(),
                    isThreadsLoading = false,
                    threadsError = null,
                    archivedThreads = emptyList(),
                    isArchivedThreadsLoading = false,
                    archivedThreadsError = null,
                    projects = emptyList(),
                    skills = emptyList(),
                    plugins = emptyList(),
                    isComposerCatalogLoading = true,
                    composerCatalogError = null,
                    selectedProjectPath = null,
                    selectedThreadId = null,
                    threadGoal = null,
                    isGoalLoading = false,
                    goalError = null,
                    models = emptyList(),
                    selectedModel = null,
                    selectedReasoningEffort = null,
                    selectedServiceTier = null,
                    collaborationModes = emptyList(),
                    selectedCollaborationMode = "default",
                    permissionProfiles = emptyList(),
                    selectedPermissionProfile = ":workspace",
                    approvalPolicy = "on-request",
                    approvalsReviewer = "user",
                    remoteServer = null,
                    remoteAccount = null,
                    remoteDeviceLogin = null,
                    isLoginStarting = false,
                    mcpServers = emptyList(),
                    isMcpStatusLoading = false,
                    mcpStatusError = null,
                    isMcpLoginStarting = false,
                    mcpAuthorizationUrl = null,
                    isFeedbackSubmitting = false,
                    feedbackError = null,
                    rateLimits = null,
                    threadTokenUsage = null,
                    isStatusLoading = false,
                    statusError = null,
                    pendingHostKeyFingerprint = null,
                    notice = if (keep) it.recoveryApprovalWarning else null,
                    recoveryApprovalWarning = if (keep) it.recoveryApprovalWarning else null,
                    recoveryBlocked = if (automaticRecovery) it.recoveryBlocked else false,
                    fullAccessConfirmation = null,
                )
            }
            if (keep) publishSelectedSession()
            runCatching {
                val secrets = withContext(Dispatchers.IO) { store.decrypt(connection) }
                val session = runtime.open(connection, secrets)
                if (generation != connectionGeneration) { session.close(); throw CancellationException("Stale dial") }
                val client = CodexRpcClient(session)
                rpc = client
                observeEvents(client)
                val server = withTimeout(20_000) { client.initialize() }
                val account = withTimeout(20_000) { client.readAccount() }
                val models = withTimeout(30_000) { client.listModels() }
                val collaborationModes = runCatching {
                    withTimeout(20_000) { client.listCollaborationModes() }
                }.getOrElse { currentCoroutineContext().ensureActive(); emptyList() }
                val permissionProfiles = runCatching {
                    withTimeout(20_000) { client.listPermissionProfiles(null) }
                }.getOrElse { currentCoroutineContext().ensureActive(); emptyList() }
                val firstPage = withTimeout(60_000) { client.listThreadPage() }
                store.recordUsed(connection.id)
                ConnectionBootstrap(server, account, models, firstPage, collaborationModes, permissionProfiles)
            }.onSuccess { bootstrap ->
                if (generation != connectionGeneration) return@onSuccess
                val threads = (bootstrap.firstPage.threads + if (keep) sessions.sessions.values
                    .filter { it.protected || it.threadId == sessions.selectedThreadId }
                    .mapNotNull { it.thread } else emptyList()).distinctBy { it.id }
                val projects = groupThreadsByProject(threads)
                val selectedModel = bootstrap.models.firstOrNull { model -> model.isDefault }
                    ?: bootstrap.models.firstOrNull()
                _state.update {
                    it.copy(
                        connectionStatus = ConnectionStatus.CONNECTED,
                        connectionMessage = connectionSummary(
                            projects.size,
                            threads.size,
                            bootstrap.server.codexVersion,
                        ),
                        models = bootstrap.models,
                        selectedModel = selectedModel?.id,
                        selectedReasoningEffort = selectedModel?.preferredReasoningEffort(),
                        selectedServiceTier = selectedModel?.defaultServiceTier,
                        collaborationModes = bootstrap.collaborationModes,
                        selectedCollaborationMode = bootstrap.collaborationModes
                            .firstOrNull { mode -> mode.mode == "default" }?.mode ?: "default",
                        permissionProfiles = bootstrap.permissionProfiles,
                        selectedPermissionProfile = ":workspace",
                        approvalPolicy = "on-request",
                        remoteServer = bootstrap.server,
                        remoteAccount = bootstrap.account,
                        threads = threads,
                        isThreadsLoading = !bootstrap.firstPage.nextCursor.isNullOrBlank(),
                        threadsError = null,
                        projects = projects,
                        skills = emptyList(),
                        plugins = emptyList(),
                        isComposerCatalogLoading = true,
                        composerCatalogError = null,
                        selectedProjectPath = projects.firstOrNull()?.path,
                    )
                }
                registerPublishedThreads(threads)
                val client = rpc ?: return@onSuccess
                restoreSelectedTaskId?.let { id ->
                    val cached = sessions.sessions[id]?.thread
                    val metadata = cached ?: threads.firstOrNull { it.id == id }
                        ?: RemoteThread(id, id, sessions.sessions[id]?.thread?.cwd.orEmpty(), 0, "")
                    selectThread(metadata)
                }
                if (!bootstrap.firstPage.nextCursor.isNullOrBlank()) loadThreads(client, bootstrap.firstPage)
                refreshComposerCatalog()
            }.onFailure { error ->
                if ((error is CancellationException && error !is TimeoutCancellationException) ||
                    generation != connectionGeneration) return@onFailure
                rpc?.close()
                rpc = null
                maintenance?.dialFailed(error)
                val unknownHostKey = generateSequence(error) { it.cause }
                    .filterIsInstance<UnknownHostKeyException>()
                    .firstOrNull()
                if (unknownHostKey != null) {
                    _state.update {
                        it.copy(
                            connectionStatus = ConnectionStatus.ERROR,
                            connectionMessage = "请先确认 SSH 主机指纹",
                            pendingHostKeyFingerprint = unknownHostKey.fingerprint,
                        )
                    }
                } else {
                    _state.update {
                        it.copy(
                            connectionStatus = ConnectionStatus.ERROR,
                            connectionMessage = friendlyError(error),
                            notice = friendlyError(error),
                        )
                    }
                }
            }
        }
    }

    fun trustPendingHostKey() {
        val connection = _state.value.activeConnection ?: return
        val fingerprint = _state.value.pendingHostKeyFingerprint ?: return
        viewModelScope.launch {
            store.recordFingerprint(connection.id, fingerprint)
            _state.update { it.copy(pendingHostKeyFingerprint = null) }
            connect(connection.copy(hostKeyFingerprint = fingerprint))
        }
    }

    fun rejectPendingHostKey() = _state.update {
        it.copy(
            pendingHostKeyFingerprint = null,
            connectionStatus = ConnectionStatus.ERROR,
            connectionMessage = "已取消未验证主机的连接",
        )
    }

    fun disconnect() {
        maintenance?.disconnectRequested()
        ++connectionGeneration
        connectionJob?.cancel()
        viewModelScope.launch { disconnectInternal(clearActive = true) }
    }

    private fun disconnectInternal(clearActive: Boolean, preserveTaskState: Boolean = false) {
        requests.invalidateConnection()
        resumeEvents.reset()
        selectionRevision++
        pendingOperations = 0
        if (!preserveTaskState) sessions.reset()
        else sessions.sessions.keys.forEach { id -> sessions.update(id, authoritative = false) {
            it.copy(pendingWrites = 0, hasApproval = false, isOlderHistoryLoading = false, isGoalLoading = false)
        } }
        threadListJob?.cancel()
        threadListJob = null
        composerCatalogJob?.cancel()
        threadRefreshPending = false
        threadListEdits.clear()
        eventJob?.cancel()
        eventJob = null
        rpc?.close()
        rpc = null
        _state.update {
            it.copy(
                activeConnection = if (clearActive) null else it.activeConnection,
                connectionStatus = ConnectionStatus.DISCONNECTED,
                connectionMessage = "",
                threads = if (clearActive) emptyList() else it.threads,
                isThreadsLoading = false,
                threadsError = null,
                archivedThreads = if (clearActive) emptyList() else it.archivedThreads,
                isArchivedThreadsLoading = false,
                archivedThreadsError = null,
                projects = if (clearActive) emptyList() else it.projects,
                selectedProjectPath = if (clearActive) null else it.selectedProjectPath,
                selectedThreadId = null,
                threadGoal = null,
                isGoalLoading = false,
                goalError = null,
                timeline = emptyList(),
                olderHistoryCursor = null,
                hasOlderHistory = false,
                isOlderHistoryLoading = false,
                olderHistoryError = null,
                consumedHistoryCursors = emptySet(),
                models = emptyList(),
                selectedModel = null,
                selectedReasoningEffort = null,
                selectedServiceTier = null,
                collaborationModes = emptyList(),
                selectedCollaborationMode = "default",
                permissionProfiles = emptyList(),
                selectedPermissionProfile = ":workspace",
                approvalPolicy = "on-request",
                approvalsReviewer = "user",
                remoteServer = null,
                remoteAccount = null,
                remoteDeviceLogin = null,
                isLoginStarting = false,
                mcpServers = emptyList(),
                isMcpStatusLoading = false,
                mcpStatusError = null,
                isMcpLoginStarting = false,
                mcpAuthorizationUrl = null,
                isFeedbackSubmitting = false,
                feedbackError = null,
                rateLimits = null,
                threadTokenUsage = null,
                isStatusLoading = false,
                statusError = null,
                isTurnRunning = false,
                activeTurnId = null,
                approvalQueue = ApprovalQueue(),
                approvalFileItems = emptyMap(),
                pendingHostKeyFingerprint = null,
                taskIndicators = emptyMap(),
                composer = TaskComposer(),
                fullAccessConfirmation = null,
                recoveryApprovalWarning = if (preserveTaskState) it.recoveryApprovalWarning else null,
            )
        }
    }

    suspend fun awaitConnectionAttempt() { connectionJob?.join() }

    suspend fun checkConnectionHealth() {
        val client = rpc ?: return
        withTimeout(10_000) { client.listThreadPage() }
    }

    fun setMaintenanceStatus(message: String?) = _state.update { it.copy(maintenanceStatus = message) }

    fun newThread() {
        if (_state.value.remoteAccount?.canRunCodex == true &&
            !(_state.value.selectedProjectPath ?: _state.value.projects.firstOrNull()?.path).isNullOrBlank()) leaveSelection()
        _state.update { state ->
            val projectPath = state.selectedProjectPath ?: state.projects.firstOrNull()?.path
            if (state.remoteAccount?.canRunCodex != true) {
                state.copy(notice = "请先登录远端 Codex")
            } else if (projectPath.isNullOrBlank()) {
                state.copy(notice = "远端没有可用于新会话的 Codex 项目")
            } else {
                state.copy(
                    selectedProjectPath = projectPath,
                    selectedThreadId = null,
                    composer = TaskComposer(),
                    isTurnRunning = false,
                    activeTurnId = null,
                    selectedPermissionProfile = ":workspace",
                    approvalPolicy = "on-request",
                    approvalsReviewer = "user",
                    threadGoal = null,
                    isGoalLoading = false,
                    goalError = null,
                    threadTokenUsage = null,
                    timeline = emptyList(),
                    olderHistoryCursor = null,
                    hasOlderHistory = false,
                    isOlderHistoryLoading = false,
                    olderHistoryError = null,
                    consumedHistoryCursors = emptySet(),
                    selectedModel = state.models.firstOrNull { it.isDefault }?.id ?: state.models.firstOrNull()?.id,
                    selectedReasoningEffort = state.models.firstOrNull { it.isDefault }?.preferredReasoningEffort(),
                    selectedServiceTier = null,
                    selectedCollaborationMode = state.defaultCollaborationMode(),
                )
            }
        }
    }

    fun selectProject(project: RemoteProject) {
        leaveSelection()
        _state.update {
            it.copy(
                selectedProjectPath = project.path,
                selectedThreadId = null,
                composer = TaskComposer(),
                isTurnRunning = false,
                activeTurnId = null,
                selectedPermissionProfile = ":workspace",
                approvalPolicy = "on-request",
                approvalsReviewer = "user",
                threadGoal = null,
                isGoalLoading = false,
                goalError = null,
                threadTokenUsage = null,
                timeline = emptyList(),
                olderHistoryCursor = null,
                hasOlderHistory = false,
                isOlderHistoryLoading = false,
                olderHistoryError = null,
                consumedHistoryCursors = emptySet(),
                selectedModel = it.models.firstOrNull { m -> m.isDefault }?.id ?: it.models.firstOrNull()?.id,
                selectedReasoningEffort = it.models.firstOrNull { m -> m.isDefault }?.preferredReasoningEffort(),
                selectedServiceTier = null,
                selectedCollaborationMode = it.defaultCollaborationMode(),
            )
        }
    }

    fun selectThread(thread: RemoteThread) {
        if (_state.value.activeConnection == null) return
        val client = rpc ?: return
        saveSelectedProjection()
        abandonResume()
        requests.advanceSelection()
        selectionRevision++
        _state.update { it.copy(fullAccessConfirmation = null, taskSelectionEpoch = selectionRevision) }
        if (!acceptMutation(sessions.register(thread.id, thread))) return
        if (!acceptMutation(sessions.select(thread.id))) return
        publishSelectedSession()
        val revision = selectionRevision
        requests.retainTasks(sessions.sessions.keys)
        val streamRevision = sessions.sessions.getValue(thread.id).liveRevision
        val historyCursor = sessions.sessions.getValue(thread.id).olderHistoryCursor
        val loadToken = requests.beginSessionLoad(thread.id)
        viewModelScope.launch {
            val epoch = resumeEvents.beginResume()
            if (rpc !== client || revision != selectionRevision) {
                finishResume(client, epoch)
                return@launch
            }
            updateTask(thread.id) { it.copy(isGoalLoading = true, isOlderHistoryLoading = false) }
            pendingOperations++
            try {
                val snapshot = client.resumeThread(thread.id, thread.cwd)
                if (rpc !== client || revision != selectionRevision || !requests.isCurrent(loadToken)) return@launch
                val disposition = resumeEvents.drainAfterResume(epoch, applySnapshot = {
                    installResumeSnapshot(thread.id, snapshot, streamRevision, historyCursor)
                }, applyEvent = { applyServerEvent(client, it) })
                if (disposition == ResumeEventDrainDisposition.OVERFLOW) failSessionCapacity("Resume event buffer exceeded capacity")
                if (rpc !== client || revision != selectionRevision || !requests.isCurrent(loadToken)) return@launch
                runCatching { client.getThreadGoal(thread.id) }.onSuccess { goal ->
                    if (rpc === client && revision == selectionRevision && requests.isCurrent(loadToken))
                        updateTask(thread.id) { it.copy(goal = goal, isGoalLoading = false, goalError = null) }
                }.onFailure { error ->
                    if (rpc === client && revision == selectionRevision && requests.isCurrent(loadToken))
                        updateTask(thread.id) { it.copy(isGoalLoading = false,
                            goalError = if (error.isUnsupportedRpcMethod("thread/goal/get")) null else friendlyGoalError(error)) }
                }
            } catch (error: Throwable) {
                if (rpc === client && revision == selectionRevision && requests.isCurrent(loadToken)) {
                    updateTask(thread.id) { it.copy(isGoalLoading = false) }
                    showError(error)
                }
            } finally {
                finishResume(client, epoch)
                if (rpc === client) pendingOperations = (pendingOperations - 1).coerceAtLeast(0)
            }
        }
    }

    fun loadOlderHistory() {
        val client = rpc ?: return
        saveSelectedProjection()
        val task = sessions.selected ?: return
        val threadId = task.threadId
        val cursor = task.olderHistoryCursor ?: return
        if (!task.hasOlderHistory || task.isOlderHistoryLoading) return
        if (cursor in task.consumedHistoryCursors) {
            updateTask(threadId) { it.copy(olderHistoryCursor = null, hasOlderHistory = false,
                olderHistoryError = "远端返回了重复的历史游标，已停止继续加载") }
            return
        }
        updateTask(threadId) { it.copy(isOlderHistoryLoading = true, olderHistoryError = null) }
        val revision = requests.beginSessionLoad(threadId, SessionLoadPurpose.HISTORY)
        viewModelScope.launch {
            pendingOperations++
            val consumed = task.consumedHistoryCursors + cursor
            try {
                val page = client.loadOlderThreadHistory(threadId, cursor)
                if (rpc !== client || !requests.isCurrent(revision)) return@launch
                val next = CodexRpcClient.checkedNextHistoryCursor(page.nextCursor, consumed)
                updateTask(threadId) { it.copy(timeline = mergeTimelineHistory(page.timeline, it.timeline),
                    olderHistoryCursor = next, hasOlderHistory = next != null, isOlderHistoryLoading = false,
                    olderHistoryError = null, consumedHistoryCursors = consumed) }
            } catch (error: Throwable) {
                if (rpc === client && requests.isCurrent(revision)) updateTask(threadId) {
                    val repeated = error.message.orEmpty().contains("nextCursor", true)
                    it.copy(olderHistoryCursor = if (repeated) null else it.olderHistoryCursor,
                        hasOlderHistory = !repeated && it.hasOlderHistory, isOlderHistoryLoading = false,
                        olderHistoryError = friendlyError(error), consumedHistoryCursors = consumed)
                }
            } finally { if (rpc === client) pendingOperations = (pendingOperations - 1).coerceAtLeast(0) }
        }
    }

    fun sendMessage(
        text: String,
        selectedMentions: List<ComposerMention> = emptyList(),
        attachments: List<ComposerImageAttachment> = emptyList(),
        asGoal: Boolean = false,
    ) {
        val prompt = text.trim()
        if (prompt.isEmpty() && attachments.isEmpty()) return
        if (_state.value.activeConnection == null) return
        val client = rpc ?: return
        saveSelectedProjection()
        val currentState = _state.value
        val ownerThreadId = currentState.selectedThreadId
        val ownerSelectionRevision = selectionRevision
        if (currentState.remoteAccount?.canRunCodex != true) {
            _state.update { it.copy(notice = "远端 Codex 尚未登录") }
            return
        }
        if (asGoal && prompt.isEmpty()) {
            _state.update { it.copy(notice = "Goal 需要包含文字目标") }
            return
        }
        if (asGoal && currentState.isTurnRunning) {
            _state.update { it.copy(notice = "当前任务运行期间不能设置 Goal") }
            return
        }
        val selectedModel = currentState.selectedModel
        if (selectedModel == null) {
            _state.update { it.copy(notice = "远端没有返回可用模型") }
            return
        }
        val selectedReasoningEffort = currentState.selectedReasoningEffort
        val selectedServiceTier = currentState.selectedServiceTier
        val approvalPolicy = currentState.approvalPolicy
        val approvalsReviewer = currentState.approvalsReviewer
        val permissionProfile = currentState.selectedPermissionProfile
        val collaborationMode = currentState.collaborationModes
            .firstOrNull { it.mode == currentState.selectedCollaborationMode }
        val cwd = currentState.threads.firstOrNull { it.id == currentState.selectedThreadId }
            ?.cwd
            ?.takeIf { it.isNotBlank() }
            ?: currentState.selectedProjectPath?.takeIf { it.isNotBlank() }
        if (cwd == null) {
            _state.update { it.copy(notice = "请先选择一个远端项目") }
            return
        }
        val mentions = resolveComposerMentions(prompt, cwd, selectedMentions, currentState)
        val steeringThreadId = currentState.selectedThreadId.takeIf { currentState.isTurnRunning }
        val steeringTurnId = currentState.activeTurnId.takeIf { currentState.isTurnRunning }
        if (currentState.isTurnRunning && (steeringThreadId == null || steeringTurnId == null)) {
            _state.update { it.copy(notice = "正在恢复运行中的任务，请等远端 turn id 同步后再追加消息") }
            return
        }
        viewModelScope.launch {
            val localItemId = "local-${UUID.randomUUID()}"
            val userItem = TimelineItem(id = localItemId, kind = TimelineKind.USER, clientId = localItemId,
                status = if (asGoal) "" else "awaiting confirmation", turnId = steeringTurnId, body = buildString {
                    append(prompt)
                    attachments.forEach { attachment ->
                        if (isNotEmpty()) append('\n')
                        append("[Image: ${attachment.displayName}]")
                    }
                }, isGoal = asGoal)
            var threadId = ownerThreadId
            var startToken: TurnStartToken? = null
            pendingOperations++
            try {
                if (threadId == null) {
                    val started = client.startThread(cwd, selectedModel, selectedServiceTier, approvalPolicy,
                        approvalsReviewer, permissionProfile)
                    if (rpc !== client) return@launch
                    threadId = started.id
                    if (!acceptMutation(sessions.register(started.id, RemoteThread(started.id, prompt.take(100),
                            started.cwd, System.currentTimeMillis() / 1000, "")))) return@launch
                    updateTask(started.id) { it.copy(settings = defaultTaskSettings().copy(
                        model = started.model ?: selectedModel, reasoningEffort = selectedReasoningEffort,
                        serviceTier = selectedServiceTier, collaborationMode = currentState.selectedCollaborationMode)) }
                    if (selectionRevision == ownerSelectionRevision && _state.value.selectedThreadId == null) {
                        sessions.select(started.id)
                        publishSelectedSession()
                    }
                }
                val owner = threadId
                if (rpc !== client || owner !in sessions.sessions) return@launch
                if (!updateTask(owner, authoritative = false) { it.copy(timeline = it.timeline + userItem, liveRevision = it.liveRevision + 1,
                    pendingWrites = it.pendingWrites + 1, isTurnRunning = if (asGoal) it.isTurnRunning else true,
                    isGoalLoading = asGoal || it.isGoalLoading, goalError = if (asGoal) null else it.goalError,
                    composer = TaskComposer(), failed = false) }) return@launch
                if (asGoal) {
                    val goal = client.setThreadGoal(owner, prompt, ThreadGoalStatus.ACTIVE)
                    if (rpc === client) updateTask(owner) { it.copy(goal = goal, isGoalLoading = false, goalError = null) }
                } else if (steeringThreadId != null && steeringTurnId != null) {
                    val returnedTurnId = client.steerTurn(owner, steeringTurnId, prompt, mentions, attachments, localItemId)
                    if (rpc === client && returnedTurnId != steeringTurnId) failSessionCapacity("Steer response changed exact turn ownership")
                } else {
                    startToken = requests.beginTurnStart(owner, localItemId)
                    val turnId = client.startTurn(owner, prompt, cwd, selectedModel, selectedReasoningEffort,
                        selectedServiceTier, approvalPolicy, approvalsReviewer, permissionProfile,
                        collaborationMode, mentions, attachments, localItemId)
                    if (rpc === client) when (requests.resolveTurnStartResponse(startToken, turnId)) {
                        TurnStartResponseDisposition.APPLY, TurnStartResponseDisposition.ALREADY_OBSERVED -> {
                            val task = sessions.sessions[owner]
                            if (task?.activeTurnId != null && task.activeTurnId != turnId)
                                failSessionCapacity("Turn response conflicted with active task owner")
                            else updateTask(owner) { it.copy(isTurnRunning = true, activeTurnId = turnId, liveRevision = it.liveRevision + 1) }
                        }
                        TurnStartResponseDisposition.TERMINAL, TurnStartResponseDisposition.SUPERSEDED -> Unit
                        TurnStartResponseDisposition.CONFLICT -> failSessionCapacity("Turn response conflicted with observed ownership")
                    }
                }
            } catch (error: Throwable) {
                if (rpc === client) {
                    val owner = threadId
                    if (owner != null && owner in sessions.sessions) updateTask(owner) { task ->
                        val rollback = startToken?.let(requests::canRollbackTurnStart) == true
                        task.copy(timeline = task.timeline.map { item ->
                            if (item.id == localItemId) item.copy(status = "delivery uncertain") else item
                        }, isTurnRunning = if (rollback) false else task.isTurnRunning,
                            activeTurnId = if (rollback) null else task.activeTurnId,
                            isGoalLoading = if (asGoal) false else task.isGoalLoading,
                            goalError = if (asGoal) friendlyGoalError(error) else task.goalError,
                            failed = true)
                    }
                    _state.update { it.copy(notice = "${friendlyError(error)}. Delivery uncertain; this write will not be replayed.") }
                }
            } finally {
                if (rpc === client) {
                    pendingOperations = (pendingOperations - 1).coerceAtLeast(0)
                    threadId?.takeIf { it in sessions.sessions }?.let { owner ->
                        updateTask(owner) { it.copy(pendingWrites = (it.pendingWrites - 1).coerceAtLeast(0)) }
                    }
                }
            }
        }
    }

    fun renameThread(thread: RemoteThread, name: String) {
        val trimmedName = name.trim()
        if (trimmedName.isEmpty()) return
        val client = rpc ?: return
        viewModelScope.launch {
            runCatching { client.renameThread(thread.id, trimmedName) }
                .onSuccess {
                    if (rpc !== client) return@onSuccess
                    _state.update { it.withThreadRenamed(thread.id, trimmedName) }
                    recordThreadListEdit(client, thread.id)
                }
                .onFailure(::showError)
        }
    }

    fun archiveThread(thread: RemoteThread) {
        val client = rpc ?: return
        if (_state.value.isTurnRunning && _state.value.selectedThreadId == thread.id) {
            _state.update { it.copy(notice = "请先停止当前任务，再归档会话") }
            return
        }
        if (_state.value.selectedThreadId == thread.id) {
            abandonResume()
            selectionRevision++
            requests.advanceSelection()
        }
        viewModelScope.launch {
            runCatching { client.archiveThread(thread.id) }
                .onSuccess {
                    if (rpc !== client) return@onSuccess
                    if (_state.value.selectedThreadId == thread.id) leaveSelection()
                    _state.update { state ->
                        state.withThreadArchived(thread.id).copy(
                            archivedThreads = (listOf(thread) + state.archivedThreads)
                                .distinctBy { it.id }
                                .sortedByDescending { it.updatedAt },
                        )
                    }
                    recordThreadListEdit(client, thread.id)
                }
                .onFailure(::showError)
        }
    }

    fun loadArchivedThreads() {
        val client = rpc ?: return
        viewModelScope.launch {
            _state.update { it.copy(isArchivedThreadsLoading = true, archivedThreadsError = null) }
            runCatching { client.listArchivedThreads() }
                .onSuccess { archived ->
                    _state.update {
                        it.copy(
                            archivedThreads = archived,
                            isArchivedThreadsLoading = false,
                            archivedThreadsError = null,
                        )
                    }
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            isArchivedThreadsLoading = false,
                            archivedThreadsError = friendlyError(error),
                        )
                    }
                }
        }
    }

    fun unarchiveThread(thread: RemoteThread) {
        val client = rpc ?: return
        viewModelScope.launch {
            runCatching { client.unarchiveThread(thread.id) }
                .onSuccess { restored ->
                    if (rpc !== client) return@onSuccess
                    _state.update { state ->
                        val threads = (listOf(restored) + state.threads).distinctBy { it.id }
                        state.copy(
                            threads = threads,
                            projects = groupThreadsByProject(threads),
                            archivedThreads = state.archivedThreads.filterNot { it.id == restored.id },
                            notice = "任务已恢复",
                        )
                    }
                    recordThreadListEdit(client, restored.id)
                }
                .onFailure(::showError)
        }
    }

    fun deleteArchivedThread(thread: RemoteThread) {
        val client = rpc ?: return
        viewModelScope.launch {
            runCatching { client.deleteThread(thread.id) }
                .onSuccess {
                    if (rpc !== client) return@onSuccess
                    _state.update { state ->
                        state.copy(
                            archivedThreads = state.archivedThreads.filterNot { it.id == thread.id },
                            notice = "任务已永久删除",
                        )
                    }
                    recordThreadListEdit(client, thread.id)
                }
                .onFailure(::showError)
        }
    }

    fun setThreadPinned(thread: RemoteThread, isPinned: Boolean) {
        val client = rpc ?: return
        viewModelScope.launch {
            runCatching { client.setThreadPinned(thread.id, isPinned) }
                .onSuccess { updated ->
                    if (rpc !== client) return@onSuccess
                    _state.update { state ->
                        val threads = state.threads.map { current ->
                            if (current.id == updated.id) updated else current
                        }
                        state.copy(threads = threads, projects = groupThreadsByProject(threads))
                    }
                    recordThreadListEdit(client, updated.id)
                }
                .onFailure(::showError)
        }
    }

    fun compactThread() {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId ?: run {
            _state.update { it.copy(notice = "新任务还没有可压缩的上下文") }
            return
        }
        if (_state.value.isTurnRunning) {
            _state.update { it.copy(notice = "任务运行期间不能压缩上下文") }
            return
        }
        viewModelScope.launch {
            runCatching { client.compactThread(threadId) }
                .onSuccess { _state.update { it.copy(notice = "正在压缩任务上下文") } }
                .onFailure(::showError)
        }
    }

    fun forkThread() {
        val client = rpc ?: return
        saveSelectedProjection()
        val snapshot = _state.value
        val owner = snapshot.selectedThreadId ?: return
        if (snapshot.isTurnRunning) { _state.update { it.copy(notice = "请先停止当前任务，再继续到新任务") }; return }
        val cwd = sessions.sessions[owner]?.thread?.cwd ?: snapshot.selectedProjectPath.orEmpty()
        abandonResume()
        val revision = ++selectionRevision
        requests.advanceSelection()
        viewModelScope.launch {
            pendingOperations++
            try {
                val forked = client.forkThread(owner, cwd, snapshot.selectedModel, snapshot.selectedServiceTier,
                    "on-request", "user", ":workspace")
                if (rpc !== client) return@launch
                if (!acceptMutation(sessions.register(forked.thread.id, forked.thread))) return@launch
                updateTask(forked.thread.id) { it.copy(timeline = forked.session.timeline,
                    olderHistoryCursor = forked.session.olderHistoryCursor,
                    hasOlderHistory = forked.session.olderHistoryCursor != null,
                    settings = defaultTaskSettings().copy(model = forked.session.model ?: snapshot.selectedModel,
                        reasoningEffort = forked.session.reasoningEffort, serviceTier = forked.session.serviceTier), resumed = true) }
                _state.update { state ->
                    val threads = (listOf(forked.thread) + state.threads).distinctBy { it.id }
                    state.copy(threads = threads, projects = groupThreadsByProject(threads))
                }
                if (selectionRevision == revision && _state.value.selectedThreadId == owner) {
                    sessions.select(forked.thread.id)
                    publishSelectedSession()
                    _state.update { it.copy(notice = "已继续到新的远端任务；权限默认为询问") }
                }
                recordThreadListEdit(client, forked.thread.id)
            } catch (error: Throwable) { if (rpc === client) showError(error) }
            finally { if (rpc === client) pendingOperations = (pendingOperations - 1).coerceAtLeast(0) }
        }
    }

    fun startReview(targetKind: ReviewTargetKind, targetValue: String = "") {
        val client = rpc ?: return
        val snapshot = _state.value
        val owner = snapshot.selectedThreadId ?: return
        if (snapshot.isTurnRunning) { _state.update { it.copy(notice = "当前任务仍在运行，暂时不能开始代码审查") }; return }
        if (targetKind != ReviewTargetKind.UNCOMMITTED_CHANGES && targetValue.isBlank()) return
        val token = requests.beginTurnStart(owner)
        updateTask(owner) { it.copy(isTurnRunning = true, pendingWrites = it.pendingWrites + 1, liveRevision = it.liveRevision + 1) }
        viewModelScope.launch {
            try {
                val review = client.startReview(owner, targetKind, targetValue)
                if (rpc !== client) return@launch
                if (review.threadId != owner) { failSessionCapacity("Review changed exact task owner"); return@launch }
                when (requests.resolveTurnStartResponse(token, review.turnId)) {
                    TurnStartResponseDisposition.APPLY, TurnStartResponseDisposition.ALREADY_OBSERVED ->
                        updateTask(owner) { it.copy(isTurnRunning = true, activeTurnId = review.turnId, liveRevision = it.liveRevision + 1) }
                    TurnStartResponseDisposition.CONFLICT -> failSessionCapacity("Review turn ownership conflict")
                    else -> Unit
                }
            } catch (error: Throwable) {
                if (rpc === client) {
                    if (requests.canRollbackTurnStart(token)) updateTask(owner) { it.copy(isTurnRunning = false, failed = true) }
                    showError(error)
                }
            } finally { if (rpc === client) updateTask(owner) { it.copy(pendingWrites = (it.pendingWrites - 1).coerceAtLeast(0)) } }
        }
    }

    fun runInit() {
        val client = rpc ?: return
        val snapshot = _state.value
        if (snapshot.isTurnRunning) {
            _state.update { it.copy(notice = "当前任务仍在运行，暂时不能执行 /init") }
            return
        }
        val cwd = snapshot.threads.firstOrNull { it.id == snapshot.selectedThreadId }?.cwd
            ?.takeIf(String::isNotBlank)
            ?: snapshot.selectedProjectPath?.takeIf(String::isNotBlank)
            ?: run {
                _state.update { it.copy(notice = "请先选择一个远端项目") }
                return
            }
        val revision = selectionRevision
        viewModelScope.launch {
            val agentsPath = remoteChildPath(cwd, "AGENTS.md")
            runCatching { client.remotePathExists(agentsPath) }
                .onSuccess { exists ->
                    if (rpc !== client || selectionRevision != revision) return@onSuccess
                    if (exists) {
                        _state.update { it.copy(notice = "AGENTS.md 已存在，已跳过 /init 以避免覆盖") }
                    } else {
                        sendMessage(INIT_PROMPT)
                    }
                }
                .onFailure(::showError)
        }
    }

    fun loadMcpStatus() {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId
        viewModelScope.launch {
            _state.update { it.copy(isMcpStatusLoading = true, mcpStatusError = null) }
            runCatching { client.listMcpServerStatuses(threadId) }
                .onSuccess { servers ->
                    _state.update {
                        it.copy(mcpServers = servers, isMcpStatusLoading = false, mcpStatusError = null)
                    }
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(
                            mcpServers = emptyList(),
                            isMcpStatusLoading = false,
                            mcpStatusError = friendlyError(error),
                        )
                    }
                }
        }
    }

    fun reloadMcpServers() {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId
        viewModelScope.launch {
            _state.update { it.copy(isMcpStatusLoading = true, mcpStatusError = null) }
            runCatching {
                client.reloadMcpServers()
                client.listMcpServerStatuses(threadId)
            }.onSuccess { servers ->
                _state.update {
                    it.copy(mcpServers = servers, isMcpStatusLoading = false, mcpStatusError = null)
                }
            }.onFailure { error ->
                _state.update {
                    it.copy(isMcpStatusLoading = false, mcpStatusError = friendlyError(error))
                }
            }
        }
    }

    fun startMcpLogin(serverName: String) {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId
        viewModelScope.launch {
            _state.update { it.copy(isMcpLoginStarting = true, mcpStatusError = null) }
            runCatching { client.startMcpOauthLogin(serverName, threadId) }
                .onSuccess { authorizationUrl ->
                    _state.update {
                        it.copy(
                            isMcpLoginStarting = false,
                            mcpAuthorizationUrl = authorizationUrl,
                            notice = "请在浏览器中完成 $serverName 授权",
                        )
                    }
                }
                .onFailure { error ->
                    _state.update {
                        it.copy(isMcpLoginStarting = false, mcpStatusError = friendlyError(error))
                    }
                }
        }
    }

    fun clearMcpAuthorizationUrl() = _state.update { it.copy(mcpAuthorizationUrl = null) }

    fun submitFeedback(classification: String, reason: String) {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId
        viewModelScope.launch {
            _state.update { it.copy(isFeedbackSubmitting = true, feedbackError = null) }
            runCatching { client.submitFeedback(classification, reason, threadId) }
                .onSuccess { feedbackId ->
                    _state.update {
                        it.copy(
                            isFeedbackSubmitting = false,
                            feedbackError = null,
                            notice = if (feedbackId.isBlank()) {
                                "反馈已提交"
                            } else {
                                "反馈已提交：$feedbackId"
                            },
                        )
                    }
                }
                .onFailure { error ->
                    val message = friendlyError(error)
                    _state.update {
                        it.copy(isFeedbackSubmitting = false, feedbackError = message, notice = message)
                    }
                }
        }
    }

    fun showGoalRequirement() = _state.update {
        it.copy(notice = "请先发送第一条消息创建远端任务，再使用 /goal 设置 Goal")
    }

    fun setThreadGoal(objective: String) {
        val prompt = objective.trim()
        val client = rpc ?: return
        val owner = _state.value.selectedThreadId ?: run { showGoalRequirement(); return }
        if (prompt.isBlank() || sessions.sessions[owner]?.isTurnRunning == true) return
        writeGoal(client, owner, "Failed to set goal") { client.setThreadGoal(owner, prompt, ThreadGoalStatus.ACTIVE) }
    }

    fun setThreadGoalStatus(status: ThreadGoalStatus) {
        val client = rpc ?: return
        val owner = _state.value.selectedThreadId ?: run { showGoalRequirement(); return }
        writeGoal(client, owner, "Failed to update goal") { client.setThreadGoal(threadId = owner, status = status) }
    }

    fun clearThreadGoal() {
        val client = rpc ?: return
        val owner = _state.value.selectedThreadId ?: return
        writeGoal(client, owner, "Failed to clear goal") { client.clearThreadGoal(owner); null }
    }

    private fun writeGoal(client: CodexRpcClient, owner: String, action: String,
                          write: suspend () -> com.codex.remote.domain.ThreadGoal?) {
        updateTask(owner) { it.copy(isGoalLoading = true, goalError = null, pendingWrites = it.pendingWrites + 1) }
        viewModelScope.launch {
            try {
                val goal = write()
                if (rpc === client) updateTask(owner) { it.copy(goal = goal, isGoalLoading = false, goalError = null) }
            } catch (error: Throwable) {
                if (rpc === client) {
                    updateTask(owner) { it.copy(isGoalLoading = false, goalError = friendlyGoalError(error)) }
                    _state.update { it.copy(notice = "$action: ${friendlyGoalError(error)}. No automatic replay.") }
                }
            } finally { if (rpc === client) updateTask(owner) { it.copy(pendingWrites = (it.pendingWrites - 1).coerceAtLeast(0)) } }
        }
    }

    fun showConnectionStatus() {
        val client = rpc ?: return
        viewModelScope.launch {
            _state.update { it.copy(isStatusLoading = true, statusError = null) }
            runCatching { client.readRateLimits() }
                .onSuccess { limits ->
                    _state.update {
                        it.copy(rateLimits = limits, isStatusLoading = false, statusError = null)
                    }
                }
                .onFailure { error ->
                    _state.update { state ->
                        if (error.isUnsupportedRpcMethod("account/rateLimits/read")) {
                            state.copy(rateLimits = null, isStatusLoading = false, statusError = null)
                        } else {
                            state.copy(isStatusLoading = false, statusError = friendlyError(error))
                        }
                    }
                }
        }
    }

    fun interruptTurn() {
        val client = rpc ?: return
        val threadId = _state.value.selectedThreadId ?: return
        val turnId = _state.value.activeTurnId ?: return
        viewModelScope.launch {
            runCatching { client.interruptTurn(threadId, turnId) }.onFailure(::showError)
        }
    }

    fun respondToApproval(key: ApprovalQueueKey, decision: String, answers: Map<String, List<String>> = emptyMap()) {
        val state = _state.value
        val entry = state.approvalQueue.entries.firstOrNull { it.key == key } ?: return
        if (decision == "disconnect") {
            disconnect()
            return
        }
        if (decision == "dismiss" && entry.delivery == ApprovalDelivery.UNCERTAIN) {
            _state.update { it.copy(approvalQueue = it.approvalQueue.complete(key)) }
            syncApprovalProtection()
            return
        }
        val approval = state.approvalQueue.requestForResponse(key) ?: return
        if (!approval.canRespond(state.selectedThreadId, decision, answers)) return
        val client = rpc ?: return
        _state.update { it.copy(approvalQueue = it.approvalQueue.markResponding(key)) }
        viewModelScope.launch {
            runCatching { client.respondToApproval(approval, decision, answers) }
                .onSuccess {
                    if (rpc === client) _state.update {
                        it.copy(approvalQueue = it.approvalQueue.complete(key), notice = "Response sent; server delivery is not confirmed. No automatic replay.")
                    }
                    if (rpc === client) syncApprovalProtection()
                }
                .onFailure { error ->
                    if (rpc === client) _state.update {
                        it.copy(approvalQueue = it.approvalQueue.delivery(key, ApprovalDelivery.UNCERTAIN),
                            notice = "Approval delivery uncertain: ${error.message}. This response will not be retried.")
                    }
                }
        }
    }

    fun startRemoteLogin() {
        val client = rpc ?: return
        if (_state.value.isLoginStarting || _state.value.remoteDeviceLogin != null) return
        viewModelScope.launch {
            _state.update { it.copy(isLoginStarting = true, notice = null) }
            runCatching { client.startDeviceLogin() }
                .onSuccess { login ->
                    _state.update { it.copy(remoteDeviceLogin = login, isLoginStarting = false) }
                }
                .onFailure { error ->
                    _state.update { it.copy(isLoginStarting = false) }
                    showError(error)
                }
        }
    }

    fun cancelRemoteLogin() {
        val login = _state.value.remoteDeviceLogin ?: return
        val client = rpc ?: return
        _state.update { it.copy(remoteDeviceLogin = null, isLoginStarting = false) }
        viewModelScope.launch {
            runCatching { client.cancelLogin(login.loginId) }.onFailure(::showError)
        }
    }

    fun setModel(modelId: String) {
        _state.update { state ->
        val model = state.models.firstOrNull { it.id == modelId } ?: return@update state
        state.copy(
            selectedModel = model.id,
            selectedReasoningEffort = model.preferredReasoningEffort(),
            selectedServiceTier = model.defaultServiceTier,
        )
    }
        saveLocalSettings()
    }

    fun setReasoningEffort(effort: String) {
        _state.update { state ->
        val model = state.models.firstOrNull { it.id == state.selectedModel }
        if (model?.supports(effort) == true) state.copy(selectedReasoningEffort = effort) else state
    }
        saveLocalSettings()
    }

    fun setServiceTier(serviceTier: String?) {
        _state.update { state ->
        val model = state.models.firstOrNull { it.id == state.selectedModel } ?: return@update state
        if (serviceTier == null || model.serviceTiers.any { it.id == serviceTier }) {
            state.copy(selectedServiceTier = serviceTier)
        } else {
            state
        }
    }
        saveLocalSettings()
    }

    fun setCollaborationMode(mode: String) {
        _state.update { state ->
        if (state.collaborationModes.any { it.mode == mode }) {
            state.copy(
                selectedCollaborationMode = mode,
                notice = if (mode == "plan") "已切换到计划模式" else null,
            )
        } else {
            state.copy(notice = "远端 Codex 没有提供 $mode 模式")
        }
    }
        saveLocalSettings()
    }

    fun setPermissionProfile(profileId: String?) {
        if (profileId == ":danger-full-access") { setPermissionMode(PermissionMode.FULL_ACCESS); return }
        _state.update { state ->
            if (profileId == null || profileId in BUILT_IN_PERMISSION_PROFILES ||
                state.permissionProfiles.any { it.id == profileId && it.allowed })
                state.copy(selectedPermissionProfile = profileId, approvalPolicy = "on-request") else state
        }
        saveLocalSettings()
    }

    fun setPermissionMode(mode: PermissionMode) {
        val state = _state.value
        if (mode == PermissionMode.FULL_ACCESS) {
            val owner = state.selectedThreadId
            val connection = state.activeConnection
            if (owner == null || connection == null) {
                _state.update { it.copy(notice = "请先创建或打开任务，再确认完全访问") }
                return
            }
            _state.update { it.copy(fullAccessConfirmation = FullAccessConfirmation(connectionGeneration,
                connection.exactSessionHostKey(), owner)) }
            return
        }
        _state.update { it.copy(selectedPermissionProfile = if (mode == PermissionMode.READ_ONLY) ":read-only" else ":workspace",
            approvalPolicy = "on-request", approvalsReviewer = if (mode == PermissionMode.AUTO_REVIEW) "auto_review" else "user",
            notice = if (it.isTurnRunning) "权限更改将在下个 turn 生效" else null) }
        saveLocalSettings()
    }

    private fun saveLocalSettings() {
        saveSelectedProjection()
        sessions.selectedThreadId?.let { id -> updateTask(id, authoritative = false) { it.copy(
            settings = it.settings.copy(locallyConfigured = true, fullAccessGranted =
                it.settings.permissionProfile == ":danger-full-access" && it.settings.fullAccessGranted)) } }
    }

    fun loadRemoteDirectory(path: String) {
        val client = rpc ?: return
        if (path.isBlank()) return
        _state.update {
            it.copy(
                remoteDirectoryPath = path,
                remoteDirectoryEntries = emptyList(),
                isRemoteDirectoryLoading = true,
                remoteDirectoryError = null,
            )
        }
        viewModelScope.launch {
            runCatching { client.readRemoteDirectory(path) }
                .onSuccess { entries ->
                    _state.update { state ->
                        if (state.remoteDirectoryPath != path) return@update state
                        state.copy(
                            remoteDirectoryEntries = entries.sortedWith(
                                compareBy<RemotePathEntry> { !it.isDirectory }
                                    .thenBy { it.name.lowercase() },
                            ),
                            isRemoteDirectoryLoading = false,
                            remoteDirectoryError = null,
                        )
                    }
                }
                .onFailure { error ->
                    _state.update { state ->
                        if (state.remoteDirectoryPath != path) return@update state
                        state.copy(
                            remoteDirectoryEntries = emptyList(),
                            isRemoteDirectoryLoading = false,
                            remoteDirectoryError = friendlyError(error),
                        )
                    }
                }
        }
    }
    fun clearRemoteDirectory() = _state.update {
        it.copy(
            remoteDirectoryPath = null,
            remoteDirectoryEntries = emptyList(),
            isRemoteDirectoryLoading = false,
            remoteDirectoryError = null,
        )
    }
    fun clearNotice() = _state.update { it.copy(notice = null) }

    private fun installResumeSnapshot(threadId: String, snapshot: RemoteThreadSession, streamRevision: Long, historyCursor: String?) {
        val preserveHistory = historyCursor != null && snapshot.olderHistoryCursor == historyCursor
        if (!preserveHistory) requests.invalidateHistoryLoad(threadId)
        updateTask(threadId) { task ->
            val remote = RemoteThreadSettingsSnapshot(snapshot.model, snapshot.reasoningEffort,
                snapshot.serviceTier, snapshot.collaborationMode, snapshot.permissionProfile,
                snapshot.approvalPolicy, snapshot.approvalsReviewer)
            val snapshotTurn = snapshot.activeTurnId?.takeUnless { requests.isCompleted(threadId, it) }
            val preserveLive = task.liveRevision != streamRevision || task.pendingWrites > 0
            task.copy(timeline = reconcileSessionTimeline(task.timeline, snapshot.timeline),
                olderHistoryCursor = if (preserveHistory) task.olderHistoryCursor else snapshot.olderHistoryCursor,
                hasOlderHistory = if (preserveHistory) task.hasOlderHistory else snapshot.olderHistoryCursor != null,
                isOlderHistoryLoading = preserveHistory && task.isOlderHistoryLoading,
                olderHistoryError = if (preserveHistory) task.olderHistoryError else null,
                consumedHistoryCursors = if (preserveHistory) task.consumedHistoryCursors else emptySet(),
                settings = task.settings.mergeRemote(remote),
                activeTurnId = if (preserveLive) task.activeTurnId else snapshotTurn,
                isTurnRunning = if (preserveLive) task.isTurnRunning else
                    (snapshot.isTurnRunning && (snapshot.activeTurnId == null || snapshotTurn != null)),
                resumed = true)
        }
    }

    /** Read-only recovery seam. Call serially after bootstrap; does not change UI selection or replay writes. */
    suspend fun resumeCachedTaskForRecovery(threadId: String): Boolean = withContext(Dispatchers.Main.immediate) {
        val client = rpc ?: return@withContext false
        val task = sessions.sessions[threadId] ?: return@withContext false
        val connection = requests.connectionGeneration
        val token = requests.beginSessionLoad(threadId)
        val epoch = resumeEvents.beginResume()
        updateTask(threadId) { it.copy(isOlderHistoryLoading = false) }
        pendingOperations++
        try {
            val snapshot = client.resumeThread(threadId, task.thread?.cwd.orEmpty())
            if (rpc !== client || !requests.isCurrent(token)) return@withContext false
            val disposition = resumeEvents.drainAfterResume(epoch,
                applySnapshot = { installResumeSnapshot(threadId, snapshot, task.liveRevision, task.olderHistoryCursor) },
                applyEvent = { applyServerEvent(client, it) })
            if (disposition == ResumeEventDrainDisposition.OVERFLOW) failSessionCapacity("Resume event buffer exceeded capacity")
            if (disposition != ResumeEventDrainDisposition.DRAINED || rpc !== client ||
                !requests.isCurrentConnection(connection)) return@withContext false
            runCatching { client.getThreadGoal(threadId) }.onSuccess { goal ->
                if (rpc === client && requests.isCurrent(token)) updateTask(threadId) { it.copy(goal = goal, isGoalLoading = false, goalError = null) }
            }.onFailure { error ->
                if (rpc === client && requests.isCurrent(token) && !error.isUnsupportedRpcMethod("thread/goal/get"))
                    updateTask(threadId) { it.copy(goalError = friendlyGoalError(error)) }
            }
            rpc === client && requests.isCurrent(token)
        } catch (error: Throwable) {
            if (rpc === client && requests.isCurrent(token)) showError(error)
            if (error is CancellationException) throw error
            false
        } finally {
            finishResume(client, epoch)
            if (rpc === client) pendingOperations = (pendingOperations - 1).coerceAtLeast(0)
        }
    }

    private fun SavedConnection.exactSessionHostKey(): String = listOf(id, username, host, port.toString(), hostKeyFingerprint).joinToString("\u0000")

    private fun defaultTaskSettings(): SessionSettings {
        val model = _state.value.models.firstOrNull { it.isDefault } ?: _state.value.models.firstOrNull()
        return SessionSettings(model = model?.id, reasoningEffort = model?.preferredReasoningEffort(),
            serviceTier = model?.defaultServiceTier)
    }

    private fun acceptMutation(result: SessionMutation): Boolean {
        if (!result.applied) {
            if (result.disconnect) failSessionCapacity(result.message ?: "Task capacity exceeded")
            else _state.update { it.copy(notice = result.message) }
        }
        return result.applied
    }

    private fun failSessionCapacity(message: String) {
        // Block maintenance synchronously before publishing the intermediate disconnected state.
        maintenance?.recoveryBlocked()
        ++connectionGeneration
        connectionJob?.cancel()
        disconnectInternal(clearActive = false, preserveTaskState = true)
        _state.update { it.copy(connectionStatus = ConnectionStatus.ERROR, connectionMessage = message,
            notice = "$message. Manual recovery required; no write will be replayed.", recoveryBlocked = true) }
    }

    private fun updateTask(threadId: String, authoritative: Boolean = true, transform: (SessionState) -> SessionState): Boolean {
        val applied = acceptMutation(sessions.update(threadId, authoritative, transform = transform))
        publishSelectedSession()
        return applied
    }

    private fun saveSelectedProjection() {
        val state = _state.value
        val id = state.selectedThreadId ?: return
        if (id != sessions.selectedThreadId) return
        acceptMutation(sessions.update(id, authoritative = false) { task -> task.copy(
            timeline = state.timeline, olderHistoryCursor = state.olderHistoryCursor,
            hasOlderHistory = state.hasOlderHistory, isOlderHistoryLoading = state.isOlderHistoryLoading,
            olderHistoryError = state.olderHistoryError, consumedHistoryCursors = state.consumedHistoryCursors,
            goal = state.threadGoal, isGoalLoading = state.isGoalLoading, goalError = state.goalError,
            tokenUsage = state.threadTokenUsage, composer = state.composer,
            settings = task.settings.copy(model = state.selectedModel, reasoningEffort = state.selectedReasoningEffort,
                serviceTier = state.selectedServiceTier, collaborationMode = state.selectedCollaborationMode,
                permissionProfile = state.selectedPermissionProfile, approvalPolicy = state.approvalPolicy,
                approvalsReviewer = state.approvalsReviewer)) })
    }

    private fun publishSelectedSession() {
        val task = sessions.selected
        val indicators = sessions.sessions.mapValues { (_, cached) -> TaskIndicator(cached.isTurnRunning,
            cached.hasApproval, cached.failed, cached.unreadCount) }
        _state.update { state ->
            if (task == null) return@update state.copy(taskIndicators = indicators)
            val settings = task.settings
            state.copy(taskSelectionEpoch = selectionRevision, selectedThreadId = task.threadId, selectedProjectPath = task.thread?.cwd?.takeIf { it.isNotBlank() }
                ?: state.selectedProjectPath,
                timeline = task.timeline, olderHistoryCursor = task.olderHistoryCursor, hasOlderHistory = task.hasOlderHistory,
                isOlderHistoryLoading = task.isOlderHistoryLoading, olderHistoryError = task.olderHistoryError,
                consumedHistoryCursors = task.consumedHistoryCursors, threadGoal = task.goal,
                isGoalLoading = task.isGoalLoading, goalError = task.goalError, threadTokenUsage = task.tokenUsage,
                selectedModel = settings.model ?: defaultTaskSettings().model,
                selectedReasoningEffort = settings.reasoningEffort ?: defaultTaskSettings().reasoningEffort,
                selectedServiceTier = settings.serviceTier, selectedCollaborationMode = settings.collaborationMode,
                selectedPermissionProfile = settings.permissionProfile, approvalPolicy = settings.approvalPolicy,
                approvalsReviewer = settings.approvalsReviewer, isTurnRunning = task.isTurnRunning,
                activeTurnId = task.activeTurnId, composer = task.composer, taskIndicators = indicators,
                isBusy = false)
        }
    }

    private fun registerPublishedThreads(threads: List<RemoteThread>) {
        threads.forEach { thread ->
            val result = sessions.register(thread.id, thread)
            // Directory size is not the session cache size. Refused metadata stays in the sidebar.
            if (!result.applied && result.disconnect) failSessionCapacity(result.message ?: "Task capacity exceeded")
        }
        requests.retainTasks(sessions.sessions.keys)
        publishSelectedSession()
    }

    private fun syncApprovalProtection() {
        val owners = _state.value.approvalQueue.entries.mapNotNull { it.request.threadId }.toSet()
        owners.forEach { id ->
            if (id !in sessions.sessions && !acceptMutation(sessions.register(id,
                    _state.value.threads.firstOrNull { it.id == id }))) {
                failSessionCapacity("Cannot retain exact approval owner")
                return
            }
        }
        sessions.sessions.keys.forEach { id ->
            if (!acceptMutation(sessions.update(id) { it.copy(hasApproval = id in owners) })) return
        }
        publishSelectedSession()
    }

    /** Only this resume may abandon its buffer; a late callback cannot drain a successor. */
    private suspend fun finishResume(client: CodexRpcClient, epoch: ResumeEventEpoch) = withContext(NonCancellable) {
        if (rpc !== client) return@withContext
        val abandoned = resumeEvents.abandonResume(epoch) ?: return@withContext
        val disposition = resumeEvents.drainAbandonedResume(abandoned) { applyServerEvent(client, it) }
        if (disposition == ResumeEventDrainDisposition.OVERFLOW) failSessionCapacity("Resume event buffer exceeded capacity")
    }

    private fun abandonResume() {
        val epoch = resumeEvents.abandonResume() ?: return
        val client = rpc ?: return
        viewModelScope.launch {
            val disposition = resumeEvents.drainAbandonedResume(epoch) { applyServerEvent(client, it) }
            if (disposition == ResumeEventDrainDisposition.OVERFLOW) failSessionCapacity("Resume event buffer exceeded capacity")
        }
    }

    private fun leaveSelection() {
        saveSelectedProjection()
        abandonResume()
        requests.advanceSelection()
        selectionRevision++
        sessions.clearSelection()
        _state.update { it.copy(fullAccessConfirmation = null, taskSelectionEpoch = selectionRevision) }
    }

    fun updateComposer(threadId: String?, composer: TaskComposer, selectionEpoch: Long = selectionRevision) {
        if (selectionEpoch != selectionRevision) return
        if (threadId == null) {
            if (_state.value.selectedThreadId == null) _state.update { it.copy(composer = composer) }
        } else if (threadId in sessions.sessions) updateTask(threadId, authoritative = false) { it.copy(composer = composer) }
    }

    fun confirmFullAccess(confirmation: FullAccessConfirmation) {
        val state = _state.value
        if (state.fullAccessConfirmation != confirmation || confirmation.connectionGeneration != connectionGeneration ||
            state.activeConnection?.exactSessionHostKey() != confirmation.hostKey ||
            state.selectedThreadId != confirmation.threadId) {
            _state.update { it.copy(fullAccessConfirmation = null, notice = "任务选择已变化，请重新确认权限") }
            return
        }
        updateTask(confirmation.threadId, authoritative = false) { it.copy(settings = it.settings.copy(
            permissionProfile = ":danger-full-access", approvalPolicy = "never", approvalsReviewer = "user",
            fullAccessGranted = true, locallyConfigured = true)) }
        _state.update { it.copy(fullAccessConfirmation = null,
            notice = if (it.isTurnRunning) "完全访问已确认；下个 turn 生效" else "完全访问已对当前任务确认") }
    }

    fun cancelFullAccess() = _state.update { it.copy(fullAccessConfirmation = null) }

    /** Stage3 can close the transport and preserve exact cached selection for authoritative recovery. */
    fun closeForRecovery() {
        ++connectionGeneration
        connectionJob?.cancel()
        saveSelectedProjection()
        val approvals = _state.value.approvalQueue.entries
        if (approvals.isNotEmpty()) _state.update { it.copy(recoveryApprovalWarning =
            "Disconnected with ${approvals.size} pending/uncertain approval(s). Delivery cannot be confirmed; no response will be retried. Check the owning tasks on the server before authorizing new requests. " +
                approvals.joinToString("; ") { entry -> "${entry.request.threadId ?: "unknown owner"} / ${entry.key.requestId}: ${entry.delivery}" }) }
        disconnectInternal(clearActive = false, preserveTaskState = true)
    }

    private fun retainApprovalFileItem(threadId: String?, item: TimelineItem) {
        if (threadId.isNullOrBlank() || item.kind != TimelineKind.FILE_CHANGE || item.turnId.isNullOrBlank() ||
            threadId.length > 4_096 || item.turnId.length > 4_096 || item.id.length > 4_096) return
        val key = ApprovalFileItemKey(threadId, item.turnId, item.id)
        _state.update { state ->
            val cache = state.approvalFileItems.toMutableMap()
            cache.remove(key)
            item.fileApprovalSnapshotOrNull()?.let { snapshot ->
                if (cache.size < 200 && cache.values.sumOf { it.approvalFileSnapshotRetainedCharCount } +
                    snapshot.approvalFileSnapshotRetainedCharCount <= ApprovalQueue.MAX_RETAINED_CHARS) cache[key] = snapshot
            }
            state.copy(approvalFileItems = cache.toMap(),
                approvalQueue = state.approvalQueue.bindFileChangeSnapshot(threadId, item))
        }
    }

    private fun observeEvents(client: CodexRpcClient) {
        eventJob?.cancel()
        eventJob = viewModelScope.launch(start = CoroutineStart.UNDISPATCHED) {
            client.events.collect { event ->
                if (rpc !== client) return@collect
                val offered = resumeEvents.processOrBuffer(event) { applyServerEvent(client, it) }
                if (offered == ResumeEventOfferDisposition.OVERFLOW) failSessionCapacity("Resume event buffer exceeded capacity")
            }
        }
    }

    private fun applyServerEvent(client: CodexRpcClient, event: AppServerEvent) {
        if (rpc !== client) return
        if (event is AppServerEvent.ItemUpsert) retainApprovalFileItem(event.threadId, event.item)
        val routed = try { eventRouter.route(sessions, event, _state.value.threads.associateBy { it.id }) }
            catch (error: IllegalStateException) { failSessionCapacity(error.message ?: "Event identity capacity exceeded"); return }
        routed?.let { result ->
            acceptMutation(result)
            publishSelectedSession()
            if (event is AppServerEvent.TurnRunning && !event.running) refreshThreads()
            return
        }
        when (event) {
            is AppServerEvent.Approval -> {
                val state = _state.value
                val request = event.request
                val item = if (request.threadId != null && request.turnId != null && request.itemId != null) {
                    state.approvalFileItems[ApprovalFileItemKey(request.threadId, request.turnId, request.itemId)]
                } else null
                val result = state.approvalQueue.enqueueResult(request.bindFileChangesSnapshot(request.threadId, item))
                if (result.status == ApprovalEnqueueStatus.CAPACITY_EXCEEDED) {
                    failSessionCapacity("Approval queue exceeded its safe capacity; disconnected without responding")
                } else _state.update { it.copy(approvalQueue = result.queue) }
            }
            is AppServerEvent.ApprovalReviewUpdated -> _state.update {
                it.copy(approvalQueue = it.approvalQueue.updateReview(event.request))
            }
            is AppServerEvent.ApprovalResolved -> _state.update {
                it.copy(approvalQueue = it.approvalQueue.complete(event.threadId, event.requestId))
            }
            is AppServerEvent.FatalProtocolError -> failSessionCapacity(event.message)
            AppServerEvent.AccountChanged -> refreshRemoteAccount()
            AppServerEvent.ThreadsChanged -> refreshThreads()
            AppServerEvent.SkillsChanged -> refreshComposerCatalog(forceReload = true)
            is AppServerEvent.RateLimitsUpdated -> _state.update {
                it.copy(rateLimits = event.rateLimits, isStatusLoading = false, statusError = null)
            }
            is AppServerEvent.McpLoginCompleted -> {
                _state.update {
                    it.copy(
                        isMcpLoginStarting = false,
                        notice = if (event.success) {
                            "${event.name} 授权完成"
                        } else {
                            event.error ?: "${event.name} 授权未完成"
                        },
                    )
                }
                if (event.success) reloadMcpServers()
            }
            is AppServerEvent.LoginCompleted -> {
                if (event.success) {
                    _state.update { it.copy(remoteDeviceLogin = null, isLoginStarting = false) }
                    refreshRemoteAccount()
                } else {
                    _state.update {
                        it.copy(
                            remoteDeviceLogin = null,
                            isLoginStarting = false,
                            notice = event.error ?: "远端 Codex 登录失败",
                        )
                    }
                }
            }
            is AppServerEvent.Failure -> {
                maintenance?.transportLost()
                closeForRecovery()
                _state.update { it.copy(connectionStatus = ConnectionStatus.ERROR,
                    connectionMessage = event.message, notice = event.message) }
            }
            is AppServerEvent.ItemUpsert, is AppServerEvent.AgentDelta,
            is AppServerEvent.PlanDelta, is AppServerEvent.ReasoningDelta,
            is AppServerEvent.OutputDelta, is AppServerEvent.TurnRunning,
            is AppServerEvent.GoalUpdated, is AppServerEvent.GoalCleared,
            is AppServerEvent.TokenUsageUpdated, is AppServerEvent.ContextCompacted,
            is AppServerEvent.ThreadSettingsUpdated -> Unit
            is AppServerEvent.Warning -> _state.update { it.copy(notice = event.message) }
            is AppServerEvent.Diagnostic -> {
                if (event.message.contains("not found", ignoreCase = true) ||
                    event.message.contains("not recognized", ignoreCase = true)
                ) {
                    _state.update { it.copy(notice = "远端登录 shell 找不到 codex 命令：${event.message}") }
                }
            }
        }
        syncApprovalProtection()
    }

    private fun refreshRemoteAccount() {
        val client = rpc ?: return
        viewModelScope.launch {
            runCatching {
                val account = client.readAccount()
                val models = client.listModels()
                account to models
            }.onSuccess { (account, models) ->
                _state.update { state ->
                    val selected = models.firstOrNull { it.id == state.selectedModel }
                        ?: models.firstOrNull { it.isDefault }
                        ?: models.firstOrNull()
                    val effort = state.selectedReasoningEffort
                        ?.takeIf { selected?.supports(it) == true }
                        ?: selected?.preferredReasoningEffort()
                    val serviceTier = state.selectedServiceTier
                        ?.takeIf { tier -> selected?.serviceTiers?.any { it.id == tier } == true }
                        ?: selected?.defaultServiceTier
                    state.copy(
                        remoteAccount = account,
                        models = models,
                        selectedModel = selected?.id,
                        selectedReasoningEffort = effort,
                        selectedServiceTier = serviceTier,
                    )
                }
            }.onFailure(::showError)
        }
    }

    private fun friendlyGoalError(error: Throwable): String {
        val message = generateSequence(error) { it.cause }
            .mapNotNull { it.message }
            .firstOrNull { it.isNotBlank() }
            ?: error::class.java.simpleName
        return when {
            message.contains("goals feature is disabled", ignoreCase = true) ->
                "远端 Codex 未启用 Goals；请在远端 config.toml 的 [features] 下设置 goals = true 后重连"
            message.contains("ephemeral thread does not support goals", ignoreCase = true) ->
                "该任务尚未持久化，发送第一条消息后才能设置 Goal"
            error.isUnsupportedRpcMethod("thread/goal/get") ||
                error.isUnsupportedRpcMethod("thread/goal/set") ||
                error.isUnsupportedRpcMethod("thread/goal/clear") ->
                "远端 Codex 版本不支持 Goal，请先更新远端 Codex"
            message.contains("method not found", ignoreCase = true) ||
                message.contains("unknown method", ignoreCase = true) ->
                "远端 Codex 版本不支持 Goal，请先更新远端 Codex"
            else -> friendlyError(error)
        }
    }

    fun retryThreads() = refreshThreads()

    private fun refreshThreads() {
        val client = rpc ?: return
        if (_state.value.connectionStatus != ConnectionStatus.CONNECTED) return
        if (threadListJob?.isActive == true) {
            threadRefreshPending = true
        } else {
            loadThreads(client)
        }
    }

    private fun loadThreads(client: CodexRpcClient, firstPage: ThreadPage? = null) {
        threadListJob = viewModelScope.launch {
            var seed = firstPage
            try {
                do {
                    threadRefreshPending = false
                    threadListEdits.clear()
                    _state.update { it.copy(isThreadsLoading = true, threadsError = null) }
                    val threads = withTimeout(60_000) {
                        client.listThreads(seed) { partial ->
                            if (rpc === client) publishThreadList(partial, complete = false)
                        }
                    }
                    if (rpc !== client) return@launch
                    publishThreadList(threads, complete = true)
                    seed = null
                } while (threadRefreshPending)
                refreshComposerCatalog()
            } catch (error: Throwable) {
                currentCoroutineContext().ensureActive()
                if (rpc === client) _state.update { it.copy(threadsError = friendlyError(error)) }
            } finally {
                if (rpc === client) _state.update { it.copy(isThreadsLoading = false) }
            }
        }
    }

    /** Local mutations stay authoritative until the next fresh listing pass. */
    private fun recordThreadListEdit(client: CodexRpcClient, id: String) {
        if (rpc === client && threadListJob?.isActive == true) {
            threadListEdits[id] = _state.value.threads.firstOrNull { it.id == id }
        }
    }

    private fun publishThreadList(imported: List<RemoteThread>, complete: Boolean) {
        _state.update { state ->
            val byId = linkedMapOf<String, RemoteThread>()
            if (!complete) state.threads.forEach { byId[it.id] = it }
            else sessions.sessions.values.filter { it.protected || it.threadId == sessions.selectedThreadId }
                .mapNotNull { it.thread }.forEach { byId[it.id] = it }
            imported.forEach { byId[it.id] = it }
            threadListEdits.forEach { (id, thread) ->
                if (thread == null) byId.remove(id) else byId[id] = thread
            }
            val threads = byId.values.sortedWith(compareByDescending<RemoteThread> { it.updatedAt }.thenBy { it.id })
            val projects = groupThreadsByProject(threads)
            val selectedThreadId = state.selectedThreadId
            state.copy(
                threads = threads,
                projects = projects,
                selectedProjectPath = state.selectedProjectPath
                    ?.takeIf { path -> selectedThreadId != null || projects.any { it.path == path } } ?: projects.firstOrNull()?.path,
                selectedThreadId = selectedThreadId,
                threadGoal = if (selectedThreadId == null) null else state.threadGoal,
                isGoalLoading = if (selectedThreadId == null) false else state.isGoalLoading,
                goalError = if (selectedThreadId == null) null else state.goalError,
                threadTokenUsage = if (selectedThreadId == null) null else state.threadTokenUsage,
                timeline = state.timeline,
                olderHistoryCursor = if (selectedThreadId == null) null else state.olderHistoryCursor,
                hasOlderHistory = selectedThreadId != null && state.hasOlderHistory,
                isOlderHistoryLoading = selectedThreadId != null && state.isOlderHistoryLoading,
                olderHistoryError = if (selectedThreadId == null) null else state.olderHistoryError,
                consumedHistoryCursors = if (selectedThreadId == null) emptySet() else state.consumedHistoryCursors,
                connectionMessage = connectionSummary(projects.size, threads.size, state.remoteServer?.codexVersion.orEmpty()),
            )
        }
        registerPublishedThreads(_state.value.threads)
    }

    private suspend fun loadComposerCatalog(
        client: CodexRpcClient,
        cwds: List<String>,
        forceReload: Boolean = false,
    ): ComposerCatalog {
        val distinctCwds = cwds.distinct()
        val skills = runCatching {
            withTimeout(45_000) { client.listSkills(distinctCwds, forceReload) }
        }
        val plugins = runCatching {
            withTimeout(45_000) { client.listInstalledPlugins(distinctCwds) }
        }
        val errors = listOfNotNull(
            skills.exceptionOrNull()?.message?.let { "Skills: $it" },
            plugins.exceptionOrNull()?.message?.let { "Plugins: $it" },
        )
        return ComposerCatalog(
            skills = skills.getOrDefault(emptyList()),
            plugins = plugins.getOrDefault(emptyList()),
            error = errors.takeIf { it.isNotEmpty() }?.joinToString(" · "),
        )
    }

    private fun refreshComposerCatalog(forceReload: Boolean = false) {
        val client = rpc ?: return
        val cwds = _state.value.projects.map { it.path }.filter(String::isNotBlank)
        _state.update { it.copy(isComposerCatalogLoading = true, composerCatalogError = null) }
        composerCatalogJob?.cancel()
        composerCatalogJob = viewModelScope.launch {
            val catalog = loadComposerCatalog(client, cwds, forceReload)
            currentCoroutineContext().ensureActive()
            if (rpc !== client) return@launch
            _state.update {
                it.copy(
                    skills = catalog.skills,
                    plugins = catalog.plugins,
                    isComposerCatalogLoading = false,
                    composerCatalogError = catalog.error,
                )
            }
        }
    }

    private fun showError(error: Throwable) {
        _state.update { it.copy(isBusy = false, notice = friendlyError(error)) }
    }

    private fun friendlyError(error: Throwable): String {
        generateSequence(error) { it.cause }
            .filterIsInstance<AppServerException>()
            .firstOrNull()
            ?.let { return friendlyAppServerError(it) }
        val message = generateSequence(error) { it.cause }
            .mapNotNull { it.message }
            .firstOrNull { it.isNotBlank() }
            ?: error::class.java.simpleName
        return when {
            message.contains("not logged in", ignoreCase = true) ||
                message.contains("OpenAI authentication", ignoreCase = true) ->
                "远端 Codex 尚未登录。请在应用中登录，或在远端运行 codex login。"
            message.contains("auth", ignoreCase = true) -> "SSH 认证失败，请检查用户名和凭据。$message"
            message.contains("timed out", ignoreCase = true) -> "连接超时，请检查主机、端口、VPN 和防火墙。"
            message.contains("refused", ignoreCase = true) -> "SSH 连接被拒绝，请确认 sshd 正在监听。"
            else -> message
        }
    }

    private fun friendlyAppServerError(error: AppServerException): String = when (error) {
        is AppServerException.DaemonStartFailed -> buildString {
            append("远端 Codex app-server daemon 启动失败（exit=${error.exitStatus}）。")
            append("请确认远端已安装 Codex：https://chatgpt.com/codex/install.sh。")
            if (error.stderrDetail.isNotBlank()) append("远端输出：${error.stderrDetail}")
        }
        is AppServerException.IncompatibleDaemon ->
            "远端 Codex daemon 响应不兼容：${error.detail}。请更新远端 Codex 后重试。"
        is AppServerException.WebSocketHandshakeFailed ->
            "与远端 app-server 的 WebSocket 握手失败：${error.detail}。请确认远端 Codex 安装完整后重试。"
        is AppServerException.AppServerConnectionLost ->
            "远端 app-server 连接已断开：${error.detail}。远端任务可能仍在运行。"
    }

    override fun onCleared() {
        rpc?.close()
        super.onCleared()
    }
}

private data class ConnectionBootstrap(
    val server: RemoteServerInfo,
    val account: RemoteAccount,
    val models: List<RemoteModel>,
    val firstPage: ThreadPage,
    val collaborationModes: List<RemoteCollaborationMode>,
    val permissionProfiles: List<com.codex.remote.domain.RemotePermissionProfile>,
)

internal fun AppUiState.acceptsThreadEvent(threadId: String?): Boolean =
    selectedThreadId != null && selectedThreadId == threadId

/**
 * Applies an app-server failure. A failure without a thread id is session-level: it stops the active turn and marks
 * the connection as errored regardless of the selected thread. A thread-level failure only applies to the selected
 * thread and leaves the connection status alone.
 */
internal fun AppUiState.applyServerFailure(message: String, threadId: String?): AppUiState {
    val sessionLevel = threadId == null
    if (!sessionLevel && !acceptsThreadEvent(threadId)) return this
    return copy(
        notice = message,
        isTurnRunning = false,
        activeTurnId = null,
        timeline = timeline.withRunningItemsCompleted(),
        connectionStatus = if (sessionLevel) ConnectionStatus.ERROR else connectionStatus,
        connectionMessage = if (sessionLevel) message else connectionMessage,
    )
}

internal fun List<SavedConnection>.lastUsedConnectionOrNull(): SavedConnection? =
    maxByOrNull(SavedConnection::lastUsedAt)?.takeIf { it.lastUsedAt > 0 }

internal fun Throwable.isUnsupportedRpcMethod(method: String): Boolean =
    generateSequence(this) { it.cause }.any { error ->
        val message = error.message.orEmpty()
        (error as? com.codex.remote.data.rpc.RpcException)?.code == -32601 ||
            (message.contains(method, ignoreCase = true) &&
                listOf("unsupported method", "method not found", "unknown method", "not implemented")
                    .any { marker -> message.contains(marker, ignoreCase = true) })
    }

private data class ComposerCatalog(
    val skills: List<com.codex.remote.domain.RemoteSkill>,
    val plugins: List<com.codex.remote.domain.RemotePlugin>,
    val error: String?,
)

internal fun resolveComposerMentions(
    prompt: String,
    cwd: String,
    selectedMentions: List<ComposerMention>,
    state: AppUiState,
): List<ComposerMention> {
    val selected = selectedMentions.filter { prompt.containsComposerToken(it.token) }
    val skills = state.skills
        .filter { skill -> skill.enabled && (skill.cwds.isEmpty() || cwd in skill.cwds) }
        .filter { prompt.containsComposerToken(it.composerToken()) }
        .map { skill ->
            ComposerMention(ComposerMentionKind.SKILL, skill.name, skill.path, skill.composerToken())
        }
    val plugins = state.plugins
        .filter { it.enabled && prompt.containsComposerToken(it.composerToken()) }
        .map { plugin ->
            ComposerMention(ComposerMentionKind.PLUGIN, plugin.displayName, plugin.mentionPath, plugin.composerToken())
        }
    return (selected + skills + plugins).distinctBy { "${it.kind}:${it.path}" }
}

private fun RemoteModel.supports(effort: String): Boolean =
    supportedReasoningEfforts.any { it.value == effort }

private fun RemoteModel.preferredReasoningEffort(): String? =
    defaultReasoningEffort?.takeIf(::supports)
        ?: supportedReasoningEfforts.firstOrNull()?.value

private fun AppUiState.defaultCollaborationMode(): String =
    collaborationModes.firstOrNull { it.mode == "default" }?.mode
        ?: collaborationModes.firstOrNull()?.mode
        ?: "default"

private fun String.isActiveTimelineStatus(): Boolean =
    equals("inProgress", ignoreCase = true) ||
        equals("in_progress", ignoreCase = true) ||
        equals("running", ignoreCase = true) ||
    equals("started", ignoreCase = true)

internal fun List<TimelineItem>.withRunningItemsCompleted(): List<TimelineItem> = map { item ->
    if (item.status.isActiveTimelineStatus()) item.copy(status = "completed") else item
}

private fun String.isRemoteThreadActive(): Boolean =
    equals("active", ignoreCase = true) || equals("inProgress", ignoreCase = true)

private fun connectionSummary(projects: Int, threads: Int, codexVersion: String): String = buildString {
    append("已导入 $projects 个项目、$threads 个会话")
    if (codexVersion.isNotBlank()) append(" · Codex $codexVersion")
}

internal fun remoteChildPath(root: String, child: String): String {
    val separator = if ('\\' in root && '/' !in root) '\\' else '/'
    val normalizedRoot = root.trimEnd('/', '\\')
    return if (normalizedRoot.isEmpty() && separator == '/') "/$child" else "$normalizedRoot$separator$child"
}

private val INIT_PROMPT = """
    Generate a file named AGENTS.md that serves as a contributor guide for this repository.
    Produce a clear, concise, and well-structured document with descriptive headings and actionable explanations.

    Requirements:
    - Title the document "Repository Guidelines".
    - Use Markdown headings for structure and keep the document around 200-400 words.
    - Describe project structure, build/test commands, coding style, testing guidelines, and commit/PR conventions.
    - Keep guidance specific to this repository and include concise examples where useful.
    - Add other relevant sections such as security, configuration, architecture, or agent instructions when appropriate.
""".trimIndent()

private val BUILT_IN_PERMISSION_PROFILES = setOf(":workspace", ":danger-full-access", ":read-only")

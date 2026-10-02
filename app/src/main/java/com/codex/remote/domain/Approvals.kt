package com.codex.remote.domain

import java.util.UUID

data class ApprovalFileItemKey(
    val threadId: String,
    val turnId: String,
    val itemId: String,
)

internal const val FILE_CHANGE_PREVIEW_MAX_FILES = 200
internal const val FILE_CHANGE_PREVIEW_MAX_LINES = 200
internal const val FILE_CHANGE_PREVIEW_MAX_CHARS = 32_768
internal const val FILE_CHANGE_TARGET_MAX_CHARS = 4_096
internal const val FILE_CHANGE_TARGETS_MAX_CHARS = 32_768

internal data class BoundedFileChangePreview(
    val path: String,
    val movePath: String?,
    val kind: String,
    val diff: String,
    val targetTruncated: Boolean,
    val diffTruncated: Boolean,
)

internal data class AggregateFileChangePreview(
    val files: List<BoundedFileChangePreview>,
    val hiddenTargetCount: Int,
    val renderedLineCount: Int,
    val renderedCharCount: Int,
    val renderedTargetCharCount: Int,
) {
    val targetsTruncated: Boolean get() = hiddenTargetCount > 0
    val targetTruncated: Boolean get() = files.any { it.targetTruncated }
    val diffTruncated: Boolean get() = files.any { it.diffTruncated }
    val fullyReviewable: Boolean get() = !targetsTruncated && !targetTruncated && !diffTruncated
}

private data class BoundedDiffPreview(
    val text: String,
    val lineCount: Int,
    val truncated: Boolean,
)

internal fun aggregateFileChangePreview(
    changes: List<FileChangeSummary>,
): AggregateFileChangePreview {
    val visibleFileCount = minOf(changes.size, FILE_CHANGE_PREVIEW_MAX_FILES)
    val files = ArrayList<BoundedFileChangePreview>(visibleFileCount)
    var remainingLines = FILE_CHANGE_PREVIEW_MAX_LINES
    var remainingChars = FILE_CHANGE_PREVIEW_MAX_CHARS
    var remainingTargetChars = FILE_CHANGE_TARGETS_MAX_CHARS
    var renderedLineCount = 0
    var renderedCharCount = 0
    var renderedTargetCharCount = 0

    for (index in 0 until visibleFileCount) {
        val change = changes[index]
        val diff = boundedDiffPreview(change.diff, remainingLines, remainingChars)
        var fileTargetChars = minOf(FILE_CHANGE_TARGET_MAX_CHARS, remainingTargetChars)
        val path = boundedTextPreview(change.path, fileTargetChars)
        fileTargetChars -= path.text.length
        remainingTargetChars -= path.text.length
        renderedTargetCharCount += path.text.length
        val movePath = change.movePath?.let { value ->
            boundedTextPreview(value, fileTargetChars).also { preview ->
                remainingTargetChars -= preview.text.length
                renderedTargetCharCount += preview.text.length
            }
        }
        files += BoundedFileChangePreview(
            path = path.text,
            movePath = movePath?.text,
            kind = change.kind,
            diff = diff.text,
            targetTruncated = path.truncated || movePath?.truncated == true,
            diffTruncated = diff.truncated,
        )
        remainingLines -= diff.lineCount
        remainingChars -= diff.text.length
        renderedLineCount += diff.lineCount
        renderedCharCount += diff.text.length
    }

    return AggregateFileChangePreview(
        files = files,
        hiddenTargetCount = changes.size - visibleFileCount,
        renderedLineCount = renderedLineCount,
        renderedCharCount = renderedCharCount,
        renderedTargetCharCount = renderedTargetCharCount,
    )
}

internal fun TimelineItem.fileApprovalSnapshotOrNull(): TimelineItem? {
    val exactTurnId = turnId?.takeIf(String::isNotBlank) ?: return null
    if (
        id.isBlank() || kind != TimelineKind.FILE_CHANGE || status != "inProgress" ||
        !fileChangesComplete || fileChanges.isEmpty() ||
        !aggregateFileChangePreview(fileChanges).fullyReviewable
    ) {
        return null
    }
    return TimelineItem(
        id = id,
        kind = TimelineKind.FILE_CHANGE,
        status = "inProgress",
        fileChanges = fileChanges.map { it.copy() },
        turnId = exactTurnId,
        fileChangesComplete = true,
    )
}

internal val TimelineItem.approvalFileSnapshotRetainedCharCount: Long
    get() {
        var retainedChars = 0L
        fun retain(value: String?) {
            retainedChars = retainedChars.saturatingAddRetainedChars(value)
        }
        retain(id)
        retain(title)
        retain(body)
        retain(status)
        retain(turnId)
        fileChanges.forEach { change ->
            retain(change.path)
            retain(change.kind)
            retain(change.diff)
            retain(change.movePath)
        }
        return retainedChars
    }

private data class BoundedTextPreview(
    val text: String,
    val truncated: Boolean,
)

private fun boundedTextPreview(text: String, maxChars: Int): BoundedTextPreview {
    var endExclusive = minOf(text.length, maxChars.coerceAtLeast(0))
    if (
        endExclusive in 1 until text.length &&
        text[endExclusive - 1].isHighSurrogate() && text[endExclusive].isLowSurrogate()
    ) {
        endExclusive -= 1
    }
    return BoundedTextPreview(
        text = text.substring(0, endExclusive),
        truncated = endExclusive < text.length,
    )
}

private fun boundedDiffPreview(
    diff: String,
    maxLines: Int,
    maxChars: Int,
): BoundedDiffPreview {
    if (diff.isEmpty()) return BoundedDiffPreview("", 0, false)
    if (maxLines <= 0 || maxChars <= 0) return BoundedDiffPreview("", 0, true)

    var endExclusive = 0
    var lineCount = 1
    while (endExclusive < diff.length && endExclusive < maxChars) {
        when (val current = diff[endExclusive]) {
            '\n' -> {
                if (lineCount >= maxLines) break
                lineCount += 1
                endExclusive += 1
            }
            '\r' -> {
                if (lineCount >= maxLines) break
                lineCount += 1
                endExclusive += 1
                if (
                    endExclusive < diff.length && endExclusive < maxChars &&
                    diff[endExclusive] == '\n'
                ) {
                    endExclusive += 1
                }
            }
            else -> {
                val charWidth = if (
                    current.isHighSurrogate() && endExclusive + 1 < diff.length &&
                    diff[endExclusive + 1].isLowSurrogate()
                ) {
                    2
                } else {
                    1
                }
                if (endExclusive + charWidth > maxChars) break
                endExclusive += charWidth
            }
        }
    }

    return BoundedDiffPreview(
        text = diff.substring(0, endExclusive),
        lineCount = if (endExclusive == 0) 0 else lineCount,
        truncated = endExclusive < diff.length,
    )
}

enum class ApprovalKind { COMMAND, FILE_CHANGE, PERMISSION, USER_INPUT, UNKNOWN }

enum class PermissionMode { ASK, AUTO_REVIEW, FULL_ACCESS, READ_ONLY }

data class ApprovalQuestion(
    val id: String,
    val header: String,
    val question: String,
    val isOther: Boolean = false,
    val options: List<ApprovalOption> = emptyList(),
)

data class ApprovalOption(
    val label: String,
    val description: String,
)

sealed interface RpcRequestId {
    val displayValue: String

    data class Text(val value: String) : RpcRequestId {
        override val displayValue: String = value
    }

    data class Number(val value: String) : RpcRequestId {
        constructor(value: Long) : this(value.toString())
        override val displayValue: String = value
    }
}

data class ApprovalContextField(
    val label: String,
    val value: String,
)

data class ApprovalRequest(
    val requestId: RpcRequestId,
    val kind: ApprovalKind,
    val title: String,
    val detail: String,
    val rawMethod: String,
    val rawParams: String = "{}",
    val questions: List<ApprovalQuestion> = emptyList(),
    val threadId: String? = null,
    val turnId: String? = null,
    val itemId: String? = null,
    val approvalId: String? = null,
    val startedAtMs: Long? = null,
    val cwd: String? = null,
    val context: List<ApprovalContextField> = emptyList(),
    val fileChanges: List<FileChangeSummary> = emptyList(),
    val availableDecisions: List<String> = defaultApprovalDecisions(kind),
    val securityContextComplete: Boolean = true,
) {
    internal val retainedCharCount: Long = calculateRetainedCharCount()

    /** Only bind a directory read from the exact owner, never from the selected task. */
    internal fun bindWorkingDirectory(ownerThreadId: String, directory: String): ApprovalRequest {
        if (kind != ApprovalKind.FILE_CHANGE || threadId != ownerThreadId || directory.isBlank() || cwd != null) return this
        return copy(cwd = directory, context = context + ApprovalContextField("Working directory", directory))
    }

    fun bindFileChangesSnapshot(
        timeline: List<TimelineItem>,
        selectedThreadId: String?,
    ): ApprovalRequest = bindFileChangesSnapshot(
        threadId = selectedThreadId,
        item = timeline.firstOrNull { item ->
            item.id == itemId && item.turnId == turnId && item.kind == TimelineKind.FILE_CHANGE
        },
    )

    fun bindFileChangesSnapshot(
        threadId: String?,
        item: TimelineItem?,
    ): ApprovalRequest {
        if (kind != ApprovalKind.FILE_CHANGE) return this
        if (rawMethod == "applyPatchApproval") return this
        val targetItemId = itemId
        val targetTurnId = turnId
        val matchingItem = if (
            this.threadId != null && threadId == this.threadId &&
            targetItemId != null && targetTurnId != null
        ) {
            item?.takeIf { candidate ->
                candidate.id == targetItemId && candidate.turnId == targetTurnId &&
                    candidate.kind == TimelineKind.FILE_CHANGE
            }
        } else {
            null
        }
        val safeSnapshot = matchingItem?.fileApprovalSnapshotOrNull()
        if (fileChanges.isEmpty()) {
            return if (safeSnapshot != null) {
                copy(fileChanges = safeSnapshot.fileChanges.map { it.copy() })
            } else {
                this
            }
        }
        if (matchingItem == null) return this
        val changedWhilePending = safeSnapshot == null || safeSnapshot.fileChanges != fileChanges
        return if (changedWhilePending) withoutFileAcceptance() else this
    }

    private fun withoutFileAcceptance(): ApprovalRequest = copy(
        securityContextComplete = false,
        availableDecisions = availableDecisions.filterNot { it.startsWith("accept") },
    )

    fun resolvedFileChanges(
        @Suppress("UNUSED_PARAMETER") timeline: List<TimelineItem>,
        @Suppress("UNUSED_PARAMETER") selectedThreadId: String? = threadId,
    ): List<FileChangeSummary> = fileChanges

    fun canApprove(
        @Suppress("UNUSED_PARAMETER") timeline: List<TimelineItem>,
        @Suppress("UNUSED_PARAMETER") selectedThreadId: String? = threadId,
    ): Boolean = securityContextComplete && reviewWithinBudget() && (
        kind != ApprovalKind.FILE_CHANGE ||
            (!cwd.isNullOrBlank() && fileChanges.isNotEmpty() && aggregateFileChangePreview(fileChanges).fullyReviewable)
        )

    private fun reviewWithinBudget(): Boolean {
        val texts = listOf(detail, cwd.orEmpty()) + context.flatMap { listOf(it.label, it.value) } +
            questions.flatMap { listOf(it.header, it.question) + it.options.flatMap { option -> listOf(option.label, option.description) } }
        return (cwd?.length ?: 0) <= 4_096 && texts.sumOf { it.length.toLong() } <= 32_768L && texts.sumOf { it.count { char -> char == '\n' } + 1 } <= 200
    }

    fun supportsDecision(decision: String): Boolean = decision in availableDecisions

    fun canSubmitAnswers(answers: Map<String, List<String>>): Boolean {
        if (kind != ApprovalKind.USER_INPUT || !securityContextComplete || questions.isEmpty()) return false
        if (answers.keys != questions.mapTo(linkedSetOf(), ApprovalQuestion::id)) return false
        if (answers.values.flatten().sumOf { it.length.toLong() } > 32_768L) return false
        return questions.all { question ->
            val answer = answers[question.id]?.singleOrNull()?.takeIf(String::isNotBlank) ?: return@all false
            question.options.isEmpty() || question.isOther || question.options.any { it.label == answer }
        }
    }

    private fun calculateRetainedCharCount(): Long {
        var retainedChars = 0L
        fun retain(value: String?) {
            retainedChars = retainedChars.saturatingAddRetainedChars(value)
        }
        retain(requestId.displayValue)
        retain(title)
        retain(detail)
        retain(rawMethod)
        retain(rawParams)
        retain(threadId)
        retain(turnId)
        retain(itemId)
        retain(approvalId)
        retain(cwd)
        questions.forEach { question ->
            retain(question.id)
            retain(question.header)
            retain(question.question)
            question.options.forEach { option ->
                retain(option.label)
                retain(option.description)
            }
        }
        context.forEach { field ->
            retain(field.label)
            retain(field.value)
        }
        fileChanges.forEach { change ->
            retain(change.path)
            retain(change.kind)
            retain(change.diff)
            retain(change.movePath)
        }
        availableDecisions.forEach(::retain)
        return retainedChars
    }
}

private fun Long.saturatingAddRetainedChars(value: String?): Long {
    val additionalChars = value?.length?.toLong() ?: return this
    return if (this > Long.MAX_VALUE - additionalChars) Long.MAX_VALUE else this + additionalChars
}

fun defaultApprovalDecisions(kind: ApprovalKind): List<String> = when (kind) {
    ApprovalKind.COMMAND,
    ApprovalKind.FILE_CHANGE,
    ApprovalKind.PERMISSION,
    -> listOf("accept", "acceptForSession", "decline")
    ApprovalKind.USER_INPUT -> listOf("accept")
    ApprovalKind.UNKNOWN -> listOf("decline")
}

data class ApprovalQueueKey(
    val queueInstanceId: String,
    val sequence: Long,
    val requestId: RpcRequestId,
    val threadId: String?,
    val turnId: String?,
    val itemId: String?,
)

data class QueuedApproval(
    val key: ApprovalQueueKey,
    val request: ApprovalRequest,
    val delivery: ApprovalDelivery = ApprovalDelivery.PENDING,
)

enum class ApprovalDelivery { PENDING, SENDING, SENT, UNCERTAIN }

enum class ApprovalEnqueueStatus { ENQUEUED, DUPLICATE_ACTIVE_ID, CAPACITY_EXCEEDED }

data class ApprovalEnqueueResult(
    val queue: ApprovalQueue,
    val status: ApprovalEnqueueStatus,
)

data class ApprovalQueue(
    val entries: List<QueuedApproval> = emptyList(),
    val respondingKeys: Set<ApprovalQueueKey> = emptySet(),
    val queueInstanceId: String = UUID.randomUUID().toString(),
    val nextSequence: Long = 1,
) {
    val requests: List<ApprovalRequest>
        get() = entries.map(QueuedApproval::request)

    val currentEntry: QueuedApproval?
        get() = entries.firstOrNull()

    val current: ApprovalRequest?
        get() = currentEntry?.request

    fun enqueue(request: ApprovalRequest): ApprovalQueue = enqueueResult(request).queue

    fun enqueueResult(request: ApprovalRequest): ApprovalEnqueueResult {
        if (entries.any { it.request.requestId == request.requestId }) {
            return ApprovalEnqueueResult(this, ApprovalEnqueueStatus.DUPLICATE_ACTIVE_ID)
        }
        if (entries.size >= MAX_PENDING_REQUESTS) {
            return ApprovalEnqueueResult(this, ApprovalEnqueueStatus.CAPACITY_EXCEEDED)
        }
        val retainedChars = retainedCharsWithinLimit()
        if (retainedChars == null || request.retainedCharCount > MAX_RETAINED_CHARS - retainedChars) {
            return ApprovalEnqueueResult(this, ApprovalEnqueueStatus.CAPACITY_EXCEEDED)
        }
        val key = ApprovalQueueKey(queueInstanceId, nextSequence, request.requestId, request.threadId, request.turnId, request.itemId)
        return ApprovalEnqueueResult(
            copy(
                entries = entries + QueuedApproval(key, request),
                nextSequence = nextSequence + 1,
            ),
            ApprovalEnqueueStatus.ENQUEUED,
        )
    }

    fun requestForResponse(key: ApprovalQueueKey): ApprovalRequest? = entries
        .firstOrNull { it.key == key }
        ?.request
        ?.takeUnless { key in respondingKeys }

    fun markResponding(key: ApprovalQueueKey): ApprovalQueue {
        if (requestForResponse(key) == null) return this
        return copy(respondingKeys = respondingKeys + key, entries = entries.map { if (it.key == key) it.copy(delivery = ApprovalDelivery.SENDING) else it })
    }

    internal fun updateReview(request: ApprovalRequest): ApprovalQueue = copy(entries = entries.map { entry ->
        val old = entry.request
        if (old.requestId == request.requestId && old.threadId == request.threadId && old.turnId == request.turnId &&
            old.itemId == request.itemId && old.rawMethod == request.rawMethod && old.rawParams == request.rawParams) entry.copy(request = request) else entry
    })

    fun delivery(key: ApprovalQueueKey, status: ApprovalDelivery): ApprovalQueue = copy(
        entries = entries.map { if (it.key == key) it.copy(delivery = status) else it },
    )

    fun complete(key: ApprovalQueueKey): ApprovalQueue {
        if (entries.none { it.key == key }) return this
        return copy(
            entries = entries.filterNot { it.key == key },
            respondingKeys = respondingKeys - key,
        )
    }

    fun complete(requestId: RpcRequestId): ApprovalQueue {
        val entry = entries.firstOrNull { it.request.requestId == requestId } ?: return this
        return complete(entry.key)
    }

    fun complete(threadId: String, requestId: RpcRequestId): ApprovalQueue {
        val entry = entries.firstOrNull { queued ->
            queued.request.requestId == requestId && queued.request.threadId == threadId
        } ?: return this
        return complete(entry.key)
    }

    fun bindFileChangeSnapshots(
        timeline: List<TimelineItem>,
        selectedThreadId: String?,
    ): ApprovalQueue = bindWithinRetainedBudget { request ->
        request.bindFileChangesSnapshot(timeline, selectedThreadId)
    }

    fun bindFileChangeSnapshot(
        threadId: String,
        item: TimelineItem,
    ): ApprovalQueue = bindWithinRetainedBudget { request ->
        request.bindFileChangesSnapshot(threadId, item)
    }

    private inline fun bindWithinRetainedBudget(
        transform: (ApprovalRequest) -> ApprovalRequest,
    ): ApprovalQueue {
        var retainedChars = retainedCharsWithinLimit() ?: return this
        var changed = false
        val boundEntries = entries.map { entry ->
            val original = entry.request
            val candidate = transform(original)
            if (candidate === original) {
                entry
            } else {
                val retainedWithoutOriginal = retainedChars - original.retainedCharCount
                if (candidate.retainedCharCount <= MAX_RETAINED_CHARS - retainedWithoutOriginal) {
                    retainedChars = retainedWithoutOriginal + candidate.retainedCharCount
                    changed = true
                    entry.copy(request = candidate)
                } else {
                    entry
                }
            }
        }
        return if (changed) copy(entries = boundEntries) else this
    }

    private fun retainedCharsWithinLimit(): Long? {
        var retainedChars = 0L
        entries.forEach { entry ->
            val requestChars = entry.request.retainedCharCount
            if (requestChars > MAX_RETAINED_CHARS - retainedChars) return null
            retainedChars += requestChars
        }
        return retainedChars
    }

    companion object {
        const val MAX_PENDING_REQUESTS = 64
        const val MAX_RETAINED_CHARS = 4L * 1024L * 1024L
    }
}


/** The same check is used at presentation and immediately before RPC dispatch. */
fun ApprovalRequest.canRespond(selectedThreadId: String?, decision: String, answers: Map<String, List<String>> = emptyMap()): Boolean {
    if (!supportsDecision(decision)) return false
    if (decision == "decline" || decision == "cancel") return true
    if (threadId.isNullOrBlank() || selectedThreadId != threadId || !canApprove(emptyList())) return false
    return kind != ApprovalKind.USER_INPUT || canSubmitAnswers(answers)
}

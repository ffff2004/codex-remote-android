package com.codex.remote.data.rpc

import com.codex.remote.domain.*
import kotlinx.serialization.json.*
import java.security.MessageDigest

internal class OutstandingApprovalRequests {
    enum class Reservation { NEW, EXACT_REPLAY, STALE_AFTER_RESOLUTION, REJECTED }
    enum class Resolution { RESOLVED, UNKNOWN, REJECTED }
    private enum class State { PENDING, RESPONDING, RESPONDED, RESOLVED_DURING_RESPONSE }
    private data class TrackedRequest(
        val threadId: String?,
        val retainedChars: Long,
        val replayIdentity: ByteArray?,
        var state: State,
    )
    private data class ResolutionTombstone(
        val id: RpcRequestId,
        val threadId: String?,
        val retainedChars: Long,
    )

    private val lock = Any()
    private val requests = mutableMapOf<RpcRequestId, TrackedRequest>()
    private val resolutionTombstones = linkedSetOf<ResolutionTombstone>()
    private var retainedIdChars = 0L
    private var retainedApprovalChars = 0L
    private var retainedTombstoneChars = 0L
    private var invalid = false

    fun reserve(
        id: RpcRequestId,
        threadId: String? = null,
        retainedChars: Long = id.displayValue.length.toLong(),
    ): Boolean = reserveInternal(id, threadId, retainedChars, replayIdentity = null) == Reservation.NEW

    fun reserveOrReplay(
        id: RpcRequestId,
        threadId: String?,
        retainedChars: Long,
        replayIdentity: ByteArray,
    ): Reservation = reserveInternal(id, threadId, retainedChars, replayIdentity)

    private fun reserveInternal(
        id: RpcRequestId,
        threadId: String?,
        retainedChars: Long,
        replayIdentity: ByteArray?,
    ): Reservation = synchronized(lock) {
        val idChars = id.displayValue.length.toLong()
        val effectiveRetainedChars = maxOf(idChars, retainedChars)
        if (invalid) return@synchronized Reservation.REJECTED
        val tombstone = resolutionTombstones.firstOrNull { it.id == id }
        if (tombstone != null) {
            if (tombstone.threadId == threadId) return@synchronized Reservation.STALE_AFTER_RESOLUTION
            invalid = true
            return@synchronized Reservation.REJECTED
        }
        val existing = requests[id]
        if (existing != null) {
            val isExactReplay = replayIdentity != null &&
                existing.replayIdentity != null &&
                existing.threadId == threadId &&
                MessageDigest.isEqual(existing.replayIdentity, replayIdentity)
            if (isExactReplay) return@synchronized Reservation.EXACT_REPLAY
            invalid = true
            return@synchronized Reservation.REJECTED
        }
        if (requests.size + resolutionTombstones.size >= MAX_TRACKED_APPROVAL_REQUESTS ||
            idChars > MAX_TRACKED_APPROVAL_ID_CHARS - retainedIdChars - retainedTombstoneChars ||
            effectiveRetainedChars > MAX_TRACKED_APPROVAL_RETAINED_CHARS - retainedApprovalChars
        ) {
            invalid = true
            Reservation.REJECTED
        } else {
            requests[id] = TrackedRequest(threadId, effectiveRetainedChars, replayIdentity, State.PENDING)
            retainedIdChars += idChars
            retainedApprovalChars += effectiveRetainedChars
            Reservation.NEW
        }
    }

    private fun rememberResolution(id: RpcRequestId, threadId: String?) {
        val chars = id.displayValue.length.toLong() + (threadId?.length ?: 0)
        resolutionTombstones += ResolutionTombstone(id, threadId, chars)
        retainedTombstoneChars += chars
        if (retainedIdChars + retainedTombstoneChars > MAX_TRACKED_APPROVAL_ID_CHARS) invalid = true
    }

    fun invalidate() = synchronized(lock) {
        invalid = true
    }

    fun resolve(id: RpcRequestId, threadId: String? = null): Boolean =
        resolveOrIgnore(id, threadId) == Resolution.RESOLVED

    fun resolveOrIgnore(id: RpcRequestId, threadId: String? = null): Resolution = synchronized(lock) {
        if (invalid) return@synchronized Resolution.REJECTED
        val tracked = requests[id]
        if (tracked == null) {
            val exactThreadId = threadId?.takeIf(String::isNotBlank)
            if (exactThreadId == null) {
                invalid = true
                return@synchronized Resolution.REJECTED
            }
            val existingTombstone = resolutionTombstones.firstOrNull { it.id == id }
            if (existingTombstone != null) {
                if (existingTombstone.threadId == exactThreadId) return@synchronized Resolution.UNKNOWN
                invalid = true
                return@synchronized Resolution.REJECTED
            }
            val tombstoneChars = id.displayValue.length.toLong() + exactThreadId.length.toLong()
            if (requests.size + resolutionTombstones.size >= MAX_TRACKED_APPROVAL_REQUESTS ||
                tombstoneChars > MAX_TRACKED_APPROVAL_ID_CHARS - retainedIdChars - retainedTombstoneChars
            ) {
                invalid = true
                return@synchronized Resolution.REJECTED
            }
            resolutionTombstones += ResolutionTombstone(id, exactThreadId, tombstoneChars)
            retainedTombstoneChars += tombstoneChars
            return@synchronized Resolution.UNKNOWN
        }
        if (tracked.threadId != threadId) {
            invalid = true
            return@synchronized Resolution.REJECTED
        }
        when (tracked.state) {
            State.PENDING, State.RESPONDED -> {
                requests.remove(id)
                rememberResolution(id, threadId)
                retainedIdChars -= id.displayValue.length.toLong()
                retainedApprovalChars -= tracked.retainedChars
                Resolution.RESOLVED
            }
            State.RESPONDING -> {
                tracked.state = State.RESOLVED_DURING_RESPONSE
                Resolution.RESOLVED
            }
            State.RESOLVED_DURING_RESPONSE -> Resolution.UNKNOWN
        }
    }

    suspend fun respondAndTrackUntilResolved(id: RpcRequestId, send: suspend () -> Unit) {
        synchronized(lock) {
            val tracked = requests[id]
            if (invalid || tracked?.state != State.PENDING) {
                invalid = true
                throw RpcException("Approval request is no longer safe to answer")
            }
            tracked.state = State.RESPONDING
        }
        try {
            send()
        } catch (error: Throwable) {
            synchronized(lock) {
                invalid = true
            }
            throw error
        }

        val delivered = synchronized(lock) {
            if (invalid) {
                false
            } else when (requests[id]?.state) {
                State.RESPONDING -> {
                    requests.getValue(id).state = State.RESPONDED
                    true
                }
                State.RESOLVED_DURING_RESPONSE -> {
                    val tracked = requests.remove(id)!!
                    rememberResolution(id, tracked.threadId)
                    retainedIdChars -= id.displayValue.length.toLong()
                    retainedApprovalChars -= tracked.retainedChars
                    true
                }
                else -> {
                    invalid = true
                    false
                }
            }
        }
        if (!delivered) {
            throw RpcException("Approval request changed state while the response was being sent")
        }
    }
}

internal fun trackedServerRequestEvent(
    requests: OutstandingApprovalRequests,
    id: RpcRequestId,
    method: String,
    params: JsonObject,
): AppServerEvent? {
    val rawParams = params.toString()
    val messageChars = id.displayValue.length.toLong() + method.length.toLong() + rawParams.length.toLong()
    if (messageChars > MAX_APPROVAL_MESSAGE_CHARS) {
        requests.invalidate()
        return AppServerEvent.FatalProtocolError(
            "Remote approval request exceeded the safe size limit; disconnected without responding.",
        )
    }
    val request = CodexRpcClient.parseApprovalRequest(id, method, params, rawParams)
    if (request.kind == ApprovalKind.UNKNOWN) {
        requests.invalidate()
        return AppServerEvent.FatalProtocolError(
            "Remote sent an unsupported JSON-RPC server request; disconnected without guessing a response.",
        )
    }
    return when (
        requests.reserveOrReplay(
            id = id,
            threadId = request.threadId,
            retainedChars = request.retainedCharCount,
            replayIdentity = approvalReplayIdentity(method, rawParams),
        )
    ) {
        OutstandingApprovalRequests.Reservation.NEW -> AppServerEvent.Approval(request.threadId, request)
        OutstandingApprovalRequests.Reservation.EXACT_REPLAY -> null
        OutstandingApprovalRequests.Reservation.STALE_AFTER_RESOLUTION -> null
        OutstandingApprovalRequests.Reservation.REJECTED -> AppServerEvent.FatalProtocolError(
            "Remote reused an outstanding approval request ID with changed authorization context " +
                "or exceeded the safe tracking limit; disconnected without responding.",
        )
    }
}

private fun approvalReplayIdentity(method: String, rawParams: String): ByteArray =
    MessageDigest.getInstance("SHA-256").digest(
        buildString(method.length + rawParams.length + 1) {
            append(method)
            append('\u0000')
            append(rawParams)
        }.toByteArray(Charsets.UTF_8),
    )

internal fun trackedServerRequestResolvedEvent(
    requests: OutstandingApprovalRequests,
    params: JsonObject,
): AppServerEvent? {
    val threadId = params.strictNonBlankString("threadId")
    val requestId = params["requestId"]?.let(CodexRpcClient::parseRequestId)
    if (threadId == null || requestId == null) {
        requests.invalidate()
        return AppServerEvent.FatalProtocolError(
            "Remote sent an invalid or unexpected approval resolution; disconnected without responding.",
        )
    }
    return when (requests.resolveOrIgnore(requestId, threadId)) {
        OutstandingApprovalRequests.Resolution.RESOLVED -> AppServerEvent.ApprovalResolved(threadId, requestId)
        OutstandingApprovalRequests.Resolution.UNKNOWN -> null
        OutstandingApprovalRequests.Resolution.REJECTED -> AppServerEvent.FatalProtocolError(
            "Remote changed the ownership or state of an approval resolution; disconnected without responding.",
        )
    }
}


package com.codex.remote.domain

import org.junit.Assert.*
import org.junit.Test

class LiveFileReviewRetentionTest {
    @Test fun replacingAtCapacityKeepsIndependentCachesAndInvalidReviewRemovesOldSnapshot() {
        val rpcCache = mutableMapOf<ApprovalFileItemKey, TimelineItem>()
        val uiCache = mutableMapOf<ApprovalFileItemKey, TimelineItem>()
        repeat(200) { retainLiveFileApprovalSnapshot(rpcCache, "a", item("$it")) }
        val refused = retainLiveFileApprovalSnapshot(rpcCache, "a", item("overflow"))!!
        assertEquals(200, rpcCache.size)
        assertNull(refused.fileApprovalSnapshotOrNull())
        val original = item("0")
        retainLiveFileApprovalSnapshot(uiCache, "a", original)
        val replacement = original.copy(fileChanges = listOf(FileChangeSummary("renamed.kt", "update", "+changed", "old.kt")))
        retainLiveFileApprovalSnapshot(rpcCache, "a", replacement)
        assertEquals(200, rpcCache.size)
        assertEquals("renamed.kt", rpcCache.getValue(ApprovalFileItemKey("a", "turn", "0")).fileChanges.single().path)
        assertEquals("file.kt", uiCache.getValue(ApprovalFileItemKey("a", "turn", "0")).fileChanges.single().path)
        val oversized = replacement.copy(fileChanges = listOf(FileChangeSummary("file.kt", "update", "x".repeat(32_769))))
        val invalid = retainLiveFileApprovalSnapshot(rpcCache, "a", oversized)!!
        assertFalse(ApprovalFileItemKey("a", "turn", "0") in rpcCache)
        assertNull(invalid.fileApprovalSnapshotOrNull())
        assertEquals(1, uiCache.size)
    }

    @Test fun aggregateCapacityRejectsReplacementWithoutKeepingItsPriorAuthorizationMaterial() {
        val cache = mutableMapOf<ApprovalFileItemKey, TimelineItem>()
        retainLiveFileApprovalSnapshot(cache, "a", item("target"))
        repeat(128) { retainLiveFileApprovalSnapshot(cache, "a", item("large-$it", "x".repeat(32_768))) }
        assertTrue(cache.values.sumOf { it.approvalFileSnapshotRetainedCharCount } <= ApprovalQueue.MAX_RETAINED_CHARS)
        val rejected = retainLiveFileApprovalSnapshot(cache, "a", item("target", "x".repeat(32_768)))!!
        assertNull(rejected.fileApprovalSnapshotOrNull())
        assertFalse(ApprovalFileItemKey("a", "turn", "target") in cache)
        assertTrue(cache.values.sumOf { it.approvalFileSnapshotRetainedCharCount } <= ApprovalQueue.MAX_RETAINED_CHARS)
    }

    private fun item(id: String, diff: String = "+new") = TimelineItem(id = id, kind = TimelineKind.FILE_CHANGE,
        turnId = "turn", status = "inProgress", fileChangesComplete = true, fileChanges = listOf(FileChangeSummary("file.kt", "update", diff)))
}

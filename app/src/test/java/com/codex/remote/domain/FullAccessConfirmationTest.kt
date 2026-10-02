package com.codex.remote.domain

import org.junit.Assert.*
import org.junit.Test

class FullAccessConfirmationTest {
    @Test fun draftAuthorizationRequiresExactIdentityConnectionTrustAndProject() {
        val draft = FullAccessTarget.Draft(12, "/project")
        val confirmation = FullAccessConfirmation(4, "host\u0000trust", draft)
        assertTrue(confirmation.matches(4, "host\u0000trust", draft))
        assertFalse(confirmation.matches(5, "host\u0000trust", draft))
        assertFalse(confirmation.matches(4, "other-host\u0000trust", draft))
        assertFalse(confirmation.matches(4, "host\u0000other-trust", draft))
        assertFalse(confirmation.matches(4, null, draft))
        assertFalse(confirmation.matches(4, "host\u0000trust", draft.copy(selectionRevision = 13)))
        assertFalse(confirmation.matches(4, "host\u0000trust", draft.copy(projectPath = "/other")))
        assertFalse(confirmation.matches(4, "host\u0000trust", null))
        assertFalse(confirmation.matches(4, "host\u0000trust", FullAccessTarget.ExistingThread("12")))
    }

    @Test fun existingTaskAuthorizationCannotBecomeADraftGrant() {
        val task = FullAccessTarget.ExistingThread("thread")
        val confirmation = FullAccessConfirmation(4, "host", task)
        assertTrue(confirmation.matches(4, "host", task))
        assertFalse(confirmation.matches(4, "host", FullAccessTarget.ExistingThread("other")))
        assertFalse(confirmation.matches(4, "host", FullAccessTarget.Draft(4, "/thread")))
    }
}

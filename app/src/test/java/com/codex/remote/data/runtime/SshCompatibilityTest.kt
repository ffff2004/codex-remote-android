package com.codex.remote.data.runtime

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SshCompatibilityTest {
    @Test
    fun excludesCurve25519ThatAndroidCannotInstantiate() {
        val names = androidCompatibleSshConfig().keyExchangeFactories.map { it.name }

        assertFalse(names.any { it.contains("curve25519", ignoreCase = true) })
        assertTrue(names.any { it.contains("ecdh", ignoreCase = true) || it.contains("group14", ignoreCase = true) })
    }
}

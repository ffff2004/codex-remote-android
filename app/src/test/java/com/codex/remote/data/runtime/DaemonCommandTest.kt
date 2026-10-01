package com.codex.remote.data.runtime

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The remote command string is parsed by the SSH login shell, which is not guaranteed to be POSIX
 * (for example fish rejects `${VAR:-default}` with exit 127). These tests pin a shell-agnostic command.
 */
class DaemonCommandTest {
    @Test
    fun daemonCommandsDoNotDependOnTheRemoteLoginShellSyntax() {
        val commands = listOf(
            daemonStartCommand(),
            daemonProxyCommand("/run/user/1000/codex.sock"),
        )

        commands.forEach { command ->
            assertTrue("expected a POSIX login shell invocation: $command", command.startsWith("exec /bin/sh -lc "))
            assertFalse("command must not expand shell variables: $command", command.contains('$'))
        }
    }

    @Test
    fun daemonStartCommandRunsTheIdempotentDaemonStart() {
        assertEquals(
            "exec /bin/sh -lc 'exec codex app-server daemon start'",
            daemonStartCommand(),
        )
    }

    @Test
    fun daemonProxyCommandQuotesTheSocketPath() {
        assertEquals(
            "exec /bin/sh -lc 'exec codex app-server proxy --sock '\\''/run/user/1000/codex.sock'\\'''",
            daemonProxyCommand("/run/user/1000/codex.sock"),
        )
        assertEquals(
            "exec /bin/sh -lc 'exec codex app-server proxy --sock '\\''/tmp/a b.sock'\\'''",
            daemonProxyCommand("/tmp/a b.sock"),
        )
        assertTrue(daemonProxyCommand("/tmp/it's.sock").contains("'\\''"))
    }
}

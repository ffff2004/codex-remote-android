package com.codex.remote.data.runtime

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Test

class DaemonLifecycleTest {
    @Test
    fun parsesStartedLifecycle() {
        val info = DaemonLifecycle.parse(
            """{"status":"started","socketPath":"/tmp/codex-app-server.sock",""" +
                """"cliVersion":"0.42.0","appServerVersion":"0.42.1"}""",
        )

        assertEquals("started", info.status)
        assertEquals("/tmp/codex-app-server.sock", info.socketPath)
        assertEquals("0.42.0", info.cliVersion)
        assertEquals("0.42.1", info.appServerVersion)
    }

    @Test
    fun parsesAlreadyRunningLifecycleWithoutVersions() {
        val info = DaemonLifecycle.parse(
            """{"status":"alreadyRunning","socketPath":"/home/codex/.codex/app-server.sock"}""",
        )

        assertEquals("alreadyRunning", info.status)
        assertEquals("/home/codex/.codex/app-server.sock", info.socketPath)
        assertNull(info.cliVersion)
        assertNull(info.appServerVersion)
    }

    @Test
    fun ignoresUnknownFields() {
        val info = DaemonLifecycle.parse(
            """{"status":"started","socketPath":"/tmp/a.sock","pid":1234,""" +
                """"futureField":{"nested":true},"cliVersion":"1.0.0"}""",
        )

        assertEquals("started", info.status)
        assertEquals("/tmp/a.sock", info.socketPath)
        assertEquals("1.0.0", info.cliVersion)
    }

    @Test
    fun rejectsNonJsonLine() {
        assertThrows(AppServerException.IncompatibleDaemon::class.java) {
            DaemonLifecycle.parse("codex: command not found")
        }
    }

    @Test
    fun rejectsNonObjectJson() {
        assertThrows(AppServerException.IncompatibleDaemon::class.java) {
            DaemonLifecycle.parse("""["started","/tmp/a.sock"]""")
        }
    }

    @Test
    fun rejectsWrongStatus() {
        assertThrows(AppServerException.IncompatibleDaemon::class.java) {
            DaemonLifecycle.parse("""{"status":"stopped","socketPath":"/tmp/a.sock"}""")
        }
    }

    @Test
    fun rejectsBlankSocketPath() {
        assertThrows(AppServerException.IncompatibleDaemon::class.java) {
            DaemonLifecycle.parse("""{"status":"started","socketPath":"   "}""")
        }
    }

    @Test
    fun rejectsRelativeSocketPath() {
        assertThrows(AppServerException.IncompatibleDaemon::class.java) {
            DaemonLifecycle.parse("""{"status":"started","socketPath":"tmp/a.sock"}""")
        }
    }

    @Test
    fun posixQuoteEscapesSpacesAndSingleQuotes() {
        assertEquals("'/tmp/a b.sock'", DaemonLifecycle.posixQuote("/tmp/a b.sock"))
        assertEquals("'it'\\''s'", DaemonLifecycle.posixQuote("it's"))
        assertEquals("''", DaemonLifecycle.posixQuote(""))
        assertEquals("'/a \$HOME `whoami`'", DaemonLifecycle.posixQuote("/a \$HOME `whoami`"))
        assertEquals("'a'\\''b'\\''c'", DaemonLifecycle.posixQuote("a'b'c"))
    }
}

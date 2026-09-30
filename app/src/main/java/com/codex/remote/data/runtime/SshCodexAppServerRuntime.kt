package com.codex.remote.data.runtime

import android.content.Context
import com.codex.remote.domain.AuthType
import com.codex.remote.domain.ConnectionSecrets
import com.codex.remote.domain.SavedConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import net.schmizz.sshj.DefaultSecurityProviderConfig
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.common.Buffer
import net.schmizz.sshj.connection.channel.direct.Session
import net.schmizz.sshj.transport.verification.HostKeyVerifier
import java.io.File
import java.io.InputStream
import java.security.MessageDigest
import java.security.PublicKey
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * POSIX-only production runtime:
 *
 * `SSH connect + host-key verify` -> `codex app-server daemon start` (idempotent, one lifecycle JSON line) ->
 * `codex app-server proxy --sock <quoted socket>` -> RFC 6455 WebSocket over the proxy stdio pair.
 *
 * The remote daemon is shared and persistent: [AppServerSession.close] never stops it and there is no
 * automatic reconnect.
 */
class SshCodexAppServerRuntime(private val context: Context) : AppServerRuntime {
    override suspend fun open(
        connection: SavedConnection,
        secrets: ConnectionSecrets,
    ): AppServerSession = withContext(Dispatchers.IO) {
        var ssh: SSHClient? = null
        try {
            val client = authenticatedClient(connection, secrets)
            ssh = client
            val lifecycle = startDaemon(client)
            openProxy(client, lifecycle)
        } catch (error: Throwable) {
            closeSsh(ssh)
            throw error
        }
    }

    private fun authenticatedClient(
        connection: SavedConnection,
        secrets: ConnectionSecrets,
    ): SSHClient {
        val ssh = SSHClient(androidCompatibleSshConfig())
        var unknownFingerprint: String? = null
        var changedFingerprint: String? = null
        ssh.connectTimeout = CONNECT_TIMEOUT_MILLIS
        ssh.timeout = READ_TIMEOUT_MILLIS
        ssh.addHostKeyVerifier(object : HostKeyVerifier {
            override fun verify(hostname: String, port: Int, key: PublicKey): Boolean {
                val actual = sha256Fingerprint(key)
                val expected = connection.hostKeyFingerprint
                if (expected.isBlank()) {
                    unknownFingerprint = actual
                    return false
                }
                if (expected != actual) {
                    changedFingerprint = actual
                    return false
                }
                return true
            }

            override fun findExistingAlgorithms(hostname: String, port: Int): List<String> = emptyList()
        })

        try {
            ssh.connect(connection.host, connection.port)
            when (connection.authType) {
                AuthType.PASSWORD -> ssh.authPassword(connection.username, secrets.password)
                AuthType.PRIVATE_KEY -> authenticatePrivateKey(ssh, connection.username, secrets)
            }
            return ssh
        } catch (error: Throwable) {
            closeSsh(ssh)
            unknownFingerprint?.let { throw UnknownHostKeyException(it) }
            changedFingerprint?.let { throw HostKeyChangedException(connection.hostKeyFingerprint, it) }
            throw error
        }
    }

    private fun authenticatePrivateKey(
        ssh: SSHClient,
        username: String,
        secrets: ConnectionSecrets,
    ) {
        val keyFile = File.createTempFile("codex_remote_", ".key", context.cacheDir)
        try {
            keyFile.writeText(secrets.privateKey, Charsets.UTF_8)
            val provider = if (secrets.passphrase.isBlank()) {
                ssh.loadKeys(keyFile.absolutePath)
            } else {
                ssh.loadKeys(keyFile.absolutePath, secrets.passphrase.toCharArray())
            }
            ssh.authPublickey(username, provider)
        } finally {
            keyFile.writeText("")
            keyFile.delete()
        }
    }

    private fun startDaemon(ssh: SSHClient): DaemonLifecycle.Info {
        val session = ssh.startSession()
        var command: Session.Command? = null
        var drain: Thread? = null
        try {
            val started = session.exec(daemonStartCommand())
            command = started
            val stderrTail = StderrTail()
            drain = Thread(
                { drainStderr(started.errorStream, stderrTail::record) },
                "codex-daemon-start-stderr",
            ).apply {
                isDaemon = true
                start()
            }

            val line = try {
                started.inputStream.bufferedReader(Charsets.UTF_8).readLine()
            } catch (error: Throwable) {
                if (error is CancellationException) throw error
                throw AppServerException.DaemonStartFailed(
                    started.exitStatus ?: -1,
                    stderrTail.detail().ifBlank { error.message.orEmpty() },
                )
            }
            if (line == null) {
                started.join(DAEMON_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                runCatching { started.close() }
                drain.join(STDERR_DRAIN_JOIN_MILLIS)
                throw AppServerException.DaemonStartFailed(started.exitStatus ?: -1, stderrTail.detail())
            }
            return DaemonLifecycle.parse(line)
        } finally {
            runCatching { command?.close() }
            runCatching { session.close() }
            drain?.join(STDERR_DRAIN_JOIN_MILLIS)
        }
    }

    private fun openProxy(ssh: SSHClient, lifecycle: DaemonLifecycle.Info): AppServerSession {
        ssh.timeout = 0
        val session = ssh.startSession()
        var command: Session.Command? = null
        try {
            val proxied = session.exec(daemonProxyCommand(lifecycle.socketPath))
            command = proxied
            val webSocket = WebSocketOverStdio(proxied.inputStream, proxied.outputStream)
            webSocket.upgrade()
            return SshAppServerSession(
                ssh = ssh,
                session = session,
                command = proxied,
                webSocket = webSocket,
                version = CodexRuntimeVersion(
                    cli = lifecycle.cliVersion.orEmpty(),
                    appServer = lifecycle.appServerVersion.orEmpty(),
                ),
            )
        } catch (error: Throwable) {
            runCatching { command?.close() }
            runCatching { session.close() }
            throw error
        }
    }

    private fun sha256Fingerprint(key: PublicKey): String {
        val wireKey = Buffer.PlainBuffer().putPublicKey(key).compactData
        val digest = MessageDigest.getInstance("SHA-256").digest(wireKey)
        return "SHA256:" + Base64.getEncoder().withoutPadding().encodeToString(digest)
    }

    private fun androidCompatibleSshConfig() = DefaultSecurityProviderConfig().apply {
        keyExchangeFactories = keyExchangeFactories.filterNot {
            it.name.contains("curve25519", ignoreCase = true)
        }
    }

    private fun drainStderr(source: InputStream, record: (String) -> Unit) {
        runCatching {
            source.bufferedReader(Charsets.UTF_8).forEachLine(record)
        }
    }

    private fun closeSsh(ssh: SSHClient?) {
        ssh ?: return
        runCatching { ssh.disconnect() }
        runCatching { ssh.close() }
    }

    private class StderrTail {
        private val lock = Any()
        private var last: String = ""

        fun record(line: String) {
            if (line.isBlank()) return
            synchronized(lock) { last = line }
        }

        fun detail(): String = synchronized(lock) { last }
    }

    private companion object {
        const val CONNECT_TIMEOUT_MILLIS = 15_000
        const val READ_TIMEOUT_MILLIS = 30_000
        const val DAEMON_START_TIMEOUT_SECONDS = 15L
        const val STDERR_DRAIN_JOIN_MILLIS = 500L
    }
}

private class SshAppServerSession(
    private val ssh: SSHClient,
    private val session: Session,
    private val command: Session.Command,
    private val webSocket: WebSocketOverStdio,
    override val version: CodexRuntimeVersion,
) : AppServerSession {
    private val json = Json { ignoreUnknownKeys = true; explicitNulls = false }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val messageChannel = Channel<JsonObject>(Channel.UNLIMITED)
    private val diagnosticChannel = Channel<String>(Channel.BUFFERED)
    private val sendMutex = Mutex()
    private val closed = AtomicBoolean(false)

    override val messages: Flow<JsonObject> = messageChannel.receiveAsFlow()
    override val diagnostics: Flow<String> = diagnosticChannel.receiveAsFlow()

    init {
        scope.launch { pumpMessages() }
        scope.launch { pumpDiagnostics() }
    }

    override suspend fun send(message: JsonObject) {
        if (closed.get()) throw closedException()
        sendMutex.withLock {
            if (closed.get()) throw closedException()
            webSocket.sendText(json.encodeToString(JsonObject.serializer(), message))
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        scope.cancel()
        runCatching { webSocket.close() }
        runCatching { command.close() }
        runCatching { session.close() }
        runCatching { ssh.disconnect() }
        runCatching { ssh.close() }
    }

    private suspend fun pumpMessages() {
        try {
            webSocket.messages.collect { text ->
                val payload = runCatching { json.parseToJsonElement(text) }.getOrNull() as? JsonObject
                    ?: throw AppServerException.AppServerConnectionLost("app-server 返回了非法 JSON")
                messageChannel.send(payload)
            }
            messageChannel.close()
        } catch (cancelled: CancellationException) {
            messageChannel.close(cancelled)
            throw cancelled
        } catch (error: Throwable) {
            messageChannel.close(error)
        }
    }

    private suspend fun pumpDiagnostics() {
        try {
            command.errorStream.bufferedReader(Charsets.UTF_8).forEachLine { line ->
                if (line.isNotBlank()) diagnosticChannel.trySend(line)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            // Diagnostics are best-effort and must never fail the session.
        } finally {
            diagnosticChannel.close()
        }
    }

    private fun closedException() = AppServerException.AppServerConnectionLost("session 已关闭")
}

internal fun daemonStartCommand(): String =
    "exec \"\${SHELL:-/bin/sh}\" -lc " +
        DaemonLifecycle.posixQuote("exec codex app-server daemon start")

internal fun daemonProxyCommand(socketPath: String): String =
    "exec \"\${SHELL:-/bin/sh}\" -lc " +
        DaemonLifecycle.posixQuote(
            "exec codex app-server proxy --sock " + DaemonLifecycle.posixQuote(socketPath),
        )

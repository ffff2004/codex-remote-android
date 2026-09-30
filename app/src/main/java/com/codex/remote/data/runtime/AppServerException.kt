package com.codex.remote.data.runtime

sealed class AppServerException(message: String) : Exception(message) {
    class DaemonStartFailed(
        val exitStatus: Int,
        val stderrDetail: String,
    ) : AppServerException(
        "远端 codex app-server daemon 启动失败（exit=$exitStatus）" +
            if (stderrDetail.isBlank()) "" else "：$stderrDetail",
    )

    class IncompatibleDaemon(val detail: String) : AppServerException("远端 daemon 响应不兼容：$detail")

    class WebSocketHandshakeFailed(val detail: String) : AppServerException("WebSocket 握手失败：$detail")

    class AppServerConnectionLost(val detail: String) : AppServerException("远端 app-server 连接已断开：$detail")
}

/**
 * Host key exceptions live in the runtime module. [com.codex.remote.data.ssh.SshAppServerTransport] keeps
 * type aliases so the legacy SSH path and existing callers still compile during the migration.
 */
class HostKeyChangedException(
    expected: String,
    actual: String,
) : SecurityException("SSH 主机密钥已变更。已保存 $expected，当前为 $actual")

class UnknownHostKeyException(val fingerprint: String) :
    SecurityException("首次连接需要确认 SSH 主机指纹：$fingerprint")

package com.codex.remote.data.runtime

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Validated `codex app-server daemon start` lifecycle JSON.
 *
 * The contract is experimental, so parsing is strict about the fields that matter for safety (status and
 * absolute POSIX socket path) and tolerant about additive fields.
 */
object DaemonLifecycle {
    private const val STATUS_STARTED = "started"
    private const val STATUS_ALREADY_RUNNING = "alreadyRunning"

    private val json = Json { ignoreUnknownKeys = true; explicitNulls = false }

    data class Info(
        val status: String,
        val socketPath: String,
        val cliVersion: String?,
        val appServerVersion: String?,
    )

    /**
     * Parses one lifecycle line. Unknown fields are ignored; a blank, relative or non-object payload is rejected
     * with [AppServerException.IncompatibleDaemon].
     */
    fun parse(line: String): Info {
        val element = runCatching { json.parseToJsonElement(line) }.getOrNull()
            ?: throw AppServerException.IncompatibleDaemon("daemon start 未返回合法 JSON 行：$line")
        val payload = element as? JsonObject
            ?: throw AppServerException.IncompatibleDaemon("daemon start 返回的 JSON 不是对象")

        val status = payload.stringField("status")
            ?: throw AppServerException.IncompatibleDaemon("daemon 生命周期缺少 status")
        if (status != STATUS_STARTED && status != STATUS_ALREADY_RUNNING) {
            throw AppServerException.IncompatibleDaemon("不支持的 daemon status：$status")
        }

        val socketPath = payload.stringField("socketPath")
            ?: throw AppServerException.IncompatibleDaemon("daemon 生命周期缺少 socketPath")
        if (!socketPath.startsWith("/") || socketPath.contains('\u0000')) {
            throw AppServerException.IncompatibleDaemon("socketPath 不是合法的绝对 POSIX 路径：$socketPath")
        }

        return Info(
            status = status,
            socketPath = socketPath,
            cliVersion = payload.stringField("cliVersion"),
            appServerVersion = payload.stringField("appServerVersion"),
        )
    }

    /**
     * POSIX single-quote escaping: the value is wrapped in single quotes and every embedded `'` becomes `'\''`,
     * so remote JSON is never interpolated raw into a shell command.
     */
    fun posixQuote(value: String): String = "'" + value.replace("'", "'\\''") + "'"

    private fun JsonObject.stringField(key: String): String? {
        val primitive = this[key] as? JsonPrimitive ?: return null
        if (!primitive.isString) return null
        return primitive.content.takeIf { it.isNotBlank() }
    }
}

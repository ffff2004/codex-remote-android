package com.codex.remote.data.runtime

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.security.MessageDigest
import java.util.Base64
import java.util.Random

/**
 * Minimal RFC 6455 client over the blocking stdio pair of `codex app-server proxy`.
 *
 * The class is deliberately not a port: it is the internal transport seam used by
 * [SshCodexAppServerRuntime]. Blocking reads are expected to run on `Dispatchers.IO`; [upgrade] and the
 * message pump are synchronous, while [sendText] switches to IO itself.
 */
internal class WebSocketOverStdio(
    private val input: InputStream,
    private val output: OutputStream,
    private val random: Random = Random(),
) : Closeable {
    private val writeLock = Any()
    private val utf8Decoder = Charsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)

    @Volatile
    private var closed = false

    @Volatile
    private var closeFrameSent = false

    /** One emission per reassembled text message; collected once by the session message pump. */
    val messages: Flow<String> = flow {
        while (true) {
            val message = readMessage() ?: break
            emit(message)
        }
    }

    /**
     * Performs the fixed `ws://localhost/` Upgrade and validates status 101 plus `Sec-WebSocket-Accept`.
     * No extensions and no subprotocol are requested.
     */
    fun upgrade() {
        val keyBytes = ByteArray(WEBSOCKET_KEY_BYTES)
        random.nextBytes(keyBytes)
        val key = Base64.getEncoder().encodeToString(keyBytes)
        val request = buildString {
            append("GET ws://localhost/ HTTP/1.1\r\n")
            append("Host: localhost\r\n")
            append("Upgrade: websocket\r\n")
            append("Connection: Upgrade\r\n")
            append("Sec-WebSocket-Key: ").append(key).append("\r\n")
            append("Sec-WebSocket-Version: 13\r\n")
            append("\r\n")
        }
        try {
            synchronized(writeLock) {
                output.write(request.toByteArray(Charsets.US_ASCII))
                output.flush()
            }
        } catch (error: IOException) {
            throw AppServerException.WebSocketHandshakeFailed(error.message ?: "无法发送 WebSocket Upgrade 请求")
        }

        val response = try {
            readUpgradeResponse()
        } catch (error: IOException) {
            throw AppServerException.WebSocketHandshakeFailed(error.message ?: "无法读取 WebSocket Upgrade 响应")
        }
        if (response.statusCode != HTTP_SWITCHING_PROTOCOLS) {
            throw AppServerException.WebSocketHandshakeFailed("Upgrade 响应状态码为 ${response.statusCode}")
        }
        if (response.header("sec-websocket-accept") != acceptValue(key)) {
            throw AppServerException.WebSocketHandshakeFailed("Sec-WebSocket-Accept 校验失败")
        }
    }

    /** Sends one masked text frame. Throws after [close]. */
    suspend fun sendText(text: String) = withContext(Dispatchers.IO) {
        if (closed) throw AppServerException.AppServerConnectionLost("WebSocket 已关闭")
        synchronized(writeLock) {
            if (closed) throw AppServerException.AppServerConnectionLost("WebSocket 已关闭")
            writeFrameLocked(OPCODE_TEXT, text.toByteArray(Charsets.UTF_8), fin = true)
        }
    }

    /**
     * Reads frames until a complete text message is assembled, replying to pings along the way.
     * Returns null once the peer sends a close frame (terminal EOF).
     */
    internal fun readMessage(): String? {
        val fragments = ByteArrayOutputStream()
        var dataOpcode = -1
        while (true) {
            val frame = readFrame()
            when (frame.opcode) {
                OPCODE_CONTINUATION -> {
                    if (dataOpcode < 0) {
                        throw AppServerException.AppServerConnectionLost("收到没有起始帧的 WebSocket continuation 帧")
                    }
                    appendFragment(fragments, frame.payload)
                    if (frame.fin) {
                        val message = decodeMessage(dataOpcode, fragments.toByteArray())
                        if (message != null) return message
                        dataOpcode = -1
                    }
                }

                OPCODE_TEXT, OPCODE_BINARY -> {
                    if (dataOpcode >= 0) {
                        throw AppServerException.AppServerConnectionLost("WebSocket 分片消息未结束就开始了新消息")
                    }
                    if (frame.fin) {
                        val message = decodeMessage(frame.opcode, frame.payload)
                        if (message != null) return message
                    } else {
                        dataOpcode = frame.opcode
                        appendFragment(fragments, frame.payload)
                    }
                }

                OPCODE_PING -> sendControl(OPCODE_PONG, frame.payload)
                OPCODE_PONG -> Unit
                OPCODE_CLOSE -> {
                    sendCloseOnce()
                    return null
                }

                else -> throw AppServerException.AppServerConnectionLost("不支持的 WebSocket opcode：${frame.opcode}")
            }
        }
    }

    override fun close() {
        synchronized(writeLock) {
            if (closed) return
            if (!closeFrameSent) {
                closeFrameSent = true
                runCatching { writeFrameLocked(OPCODE_CLOSE, CLOSE_NORMAL, fin = true) }
            }
            closed = true
        }
        runCatching { output.close() }
        runCatching { input.close() }
    }

    private fun readUpgradeResponse(): UpgradeResponse {
        val bytes = ByteArrayOutputStream()
        var matched = 0
        while (matched < HEADER_TERMINATOR_SIZE) {
            val next = input.read()
            if (next < 0) throw AppServerException.WebSocketHandshakeFailed("Upgrade 响应提前结束")
            bytes.write(next)
            if (bytes.size() > MAX_UPGRADE_BYTES) {
                throw AppServerException.WebSocketHandshakeFailed("Upgrade 响应过大")
            }
            matched = when {
                next == '\r'.code && (matched == 0 || matched == 2) -> matched + 1
                next == '\n'.code && (matched == 1 || matched == 3) -> matched + 1
                else -> 0
            }
        }
        val lines = String(bytes.toByteArray(), Charsets.ISO_8859_1).split("\r\n")
        val statusLine = lines.firstOrNull().orEmpty()
        val statusCode = statusLine.split(' ').getOrNull(1)?.toIntOrNull()
            ?: throw AppServerException.WebSocketHandshakeFailed("无法解析 Upgrade 状态行：$statusLine")
        val headers = mutableMapOf<String, String>()
        lines.drop(1).forEach { line ->
            if (line.isBlank()) return@forEach
            val separator = line.indexOf(':')
            if (separator > 0) {
                headers[line.substring(0, separator).trim().lowercase()] = line.substring(separator + 1).trim()
            }
        }
        return UpgradeResponse(statusCode, headers)
    }

    private fun readFrame(): WebSocketFrame {
        val first = readByteOrThrow()
        val fin = first and 0x80 != 0
        if (first and 0x70 != 0) {
            throw AppServerException.AppServerConnectionLost("WebSocket 帧设置了不支持的 RSV 位")
        }
        val opcode = first and 0x0F
        val second = readByteOrThrow()
        if (second and 0x80 != 0) {
            throw AppServerException.AppServerConnectionLost("服务端 WebSocket 帧不得使用掩码")
        }
        var length = (second and 0x7F).toLong()
        when (length) {
            126L -> length = readUnsignedShort().toLong()
            127L -> length = readUnsignedLong()
        }
        if (length < 0 || length > MAX_MESSAGE_BYTES) {
            throw AppServerException.AppServerConnectionLost("WebSocket 帧长度非法：$length")
        }
        val payload = readFully(length.toInt())
        if (opcode >= OPCODE_CLOSE && (!fin || payload.size > MAX_CONTROL_PAYLOAD)) {
            throw AppServerException.AppServerConnectionLost("WebSocket 控制帧非法")
        }
        return WebSocketFrame(fin, opcode, payload)
    }

    private fun readByteOrThrow(): Int {
        val value = input.read()
        if (value < 0) throw AppServerException.AppServerConnectionLost("WebSocket 数据流已结束")
        return value
    }

    private fun readFully(length: Int): ByteArray {
        val payload = ByteArray(length)
        var read = 0
        while (read < length) {
            val count = input.read(payload, read, length - read)
            if (count < 0) throw AppServerException.AppServerConnectionLost("WebSocket 帧数据不完整")
            read += count
        }
        return payload
    }

    private fun readUnsignedShort(): Int = (readByteOrThrow() shl 8) or readByteOrThrow()

    private fun readUnsignedLong(): Long {
        var value = 0L
        repeat(Long.SIZE_BYTES) {
            value = (value shl 8) or readByteOrThrow().toLong()
        }
        return value
    }

    private fun appendFragment(sink: ByteArrayOutputStream, payload: ByteArray) {
        if (sink.size().toLong() + payload.size > MAX_MESSAGE_BYTES) {
            throw AppServerException.AppServerConnectionLost("WebSocket 消息超过大小上限")
        }
        sink.write(payload)
    }

    private fun decodeMessage(opcode: Int, payload: ByteArray): String? {
        if (opcode == OPCODE_BINARY) return null
        return try {
            utf8Decoder.reset()
            utf8Decoder.decode(ByteBuffer.wrap(payload)).toString()
        } catch (_: CharacterCodingException) {
            throw AppServerException.AppServerConnectionLost("WebSocket 文本帧不是合法 UTF-8")
        }
    }

    private fun sendControl(opcode: Int, payload: ByteArray) {
        if (payload.size > MAX_CONTROL_PAYLOAD) {
            throw AppServerException.AppServerConnectionLost("WebSocket 控制帧负载过大")
        }
        synchronized(writeLock) {
            if (closed) return
            writeFrameLocked(opcode, payload, fin = true)
        }
    }

    private fun sendCloseOnce() {
        synchronized(writeLock) {
            if (closed || closeFrameSent) return
            closeFrameSent = true
            runCatching { writeFrameLocked(OPCODE_CLOSE, CLOSE_NORMAL, fin = true) }
        }
    }

    private fun writeFrameLocked(opcode: Int, payload: ByteArray, fin: Boolean) {
        val frame = ByteArrayOutputStream()
        frame.write(if (fin) 0x80 or opcode else opcode)
        val maskKey = ByteArray(MASK_KEY_BYTES)
        random.nextBytes(maskKey)
        val length = payload.size
        when {
            length < 126 -> frame.write(0x80 or length)
            length <= 0xFFFF -> {
                frame.write(0x80 or 126)
                frame.write(length ushr 8)
                frame.write(length and 0xFF)
            }

            else -> {
                frame.write(0x80 or 127)
                for (shift in 7 downTo 0) {
                    frame.write(((length.toLong() ushr (8 * shift)) and 0xFF).toInt())
                }
            }
        }
        frame.write(maskKey)
        for (index in 0 until length) {
            frame.write(payload[index].toInt() xor maskKey[index % MASK_KEY_BYTES].toInt())
        }
        output.write(frame.toByteArray())
        output.flush()
    }

    private data class UpgradeResponse(
        val statusCode: Int,
        val headers: Map<String, String>,
    ) {
        fun header(name: String): String? = headers[name.lowercase()]
    }

    private data class WebSocketFrame(
        val fin: Boolean,
        val opcode: Int,
        val payload: ByteArray,
    )

    companion object {
        const val WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

        private const val HTTP_SWITCHING_PROTOCOLS = 101
        private const val WEBSOCKET_KEY_BYTES = 16
        private const val MASK_KEY_BYTES = 4
        private const val HEADER_TERMINATOR_SIZE = 4
        private const val MAX_UPGRADE_BYTES = 16 * 1024
        private const val MAX_CONTROL_PAYLOAD = 125
        private const val MAX_MESSAGE_BYTES = 16L * 1024 * 1024

        private const val OPCODE_CONTINUATION = 0
        private const val OPCODE_TEXT = 1
        private const val OPCODE_BINARY = 2
        private const val OPCODE_CLOSE = 8
        private const val OPCODE_PING = 9
        private const val OPCODE_PONG = 10

        private val CLOSE_NORMAL = byteArrayOf(0x03, 0xE8.toByte())

        fun acceptValue(key: String): String {
            val digest = MessageDigest.getInstance("SHA-1")
                .digest((key + WEBSOCKET_GUID).toByteArray(Charsets.US_ASCII))
            return Base64.getEncoder().encodeToString(digest)
        }
    }
}

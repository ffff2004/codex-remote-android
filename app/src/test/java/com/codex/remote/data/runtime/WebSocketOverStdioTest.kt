package com.codex.remote.data.runtime

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.Random

class WebSocketOverStdioTest {
    @Test
    fun upgradeSendsRfc6455RequestWithoutExtensionsOrSubprotocol() {
        val (socket, output) = socketFor()
        socket.upgrade()

        val request = String(output.toByteArray(), Charsets.US_ASCII)
        assertTrue(request.startsWith("GET ws://localhost/ HTTP/1.1\r\n"))
        assertTrue(request.contains("Host: localhost\r\n"))
        assertTrue(request.contains("Upgrade: websocket\r\n"))
        assertTrue(request.contains("Connection: Upgrade\r\n"))
        assertTrue(request.contains("Sec-WebSocket-Version: 13\r\n"))
        assertTrue(request.contains("Sec-WebSocket-Key: ${keyForSeed(SEED)}\r\n"))
        assertTrue(request.endsWith("\r\n\r\n"))
        assertFalse(request.contains("Sec-WebSocket-Extensions"))
        assertFalse(request.contains("Sec-WebSocket-Protocol"))
    }

    @Test
    fun upgradeRejectsWrongAcceptValue() {
        val (socket, _) = socketFor(accept = "not-the-right-accept")

        assertThrows(AppServerException.WebSocketHandshakeFailed::class.java) {
            socket.upgrade()
        }
    }

    @Test
    fun upgradeRejectsNonSwitchingStatus() {
        val (socket, _) = socketFor(status = 400)

        assertThrows(AppServerException.WebSocketHandshakeFailed::class.java) {
            socket.upgrade()
        }
    }

    @Test
    fun sendTextSetsMaskBitAndEncodesPayload() {
        val (socket, output) = socketFor()
        socket.upgrade()
        runBlocking { socket.sendText("hi") }

        val raw = output.toByteArray()
        val frameStart = headerEnd(raw)
        assertEquals(0x81, raw[frameStart].toInt() and 0xFF)
        assertTrue(raw[frameStart + 1].toInt() and 0x80 != 0)
        val frame = parseClientFrames(raw).single()
        assertTrue(frame.fin)
        assertEquals(OPCODE_TEXT, frame.opcode)
        assertEquals("hi", String(frame.payload, Charsets.UTF_8))
    }

    @Test
    fun reassemblesFragmentedMessages() {
        val frames = concat(
            serverFrame(OPCODE_TEXT, "Hel".toByteArray(), fin = false),
            serverFrame(OPCODE_CONTINUATION, "lo ".toByteArray(), fin = false),
            serverFrame(OPCODE_CONTINUATION, "world".toByteArray(), fin = true),
            serverFrame(OPCODE_CLOSE, closePayload()),
        )
        val (socket, _) = socketFor(serverBytes = frames)
        socket.upgrade()

        assertEquals(listOf("Hello world"), runBlocking { socket.messages.toList() })
    }

    @Test
    fun readsSevenSixteenAndSixtyFourBitPayloadLengths() {
        val small = "a".repeat(125)
        val medium = "b".repeat(200)
        val large = "c".repeat(70_000)
        val frames = concat(
            serverFrame(OPCODE_TEXT, small.toByteArray()),
            serverFrame(OPCODE_TEXT, medium.toByteArray()),
            serverFrame(OPCODE_TEXT, large.toByteArray()),
            serverFrame(OPCODE_CLOSE, closePayload()),
        )
        val (socket, _) = socketFor(serverBytes = frames)
        socket.upgrade()

        assertEquals(listOf(small, medium, large), runBlocking { socket.messages.toList() })
    }

    @Test
    fun writesSixteenAndSixtyFourBitPayloadLengths() {
        val (socket, output) = socketFor()
        socket.upgrade()
        val medium = "m".repeat(200)
        val large = "l".repeat(70_000)
        runBlocking {
            socket.sendText(medium)
            socket.sendText(large)
        }

        val frames = parseClientFrames(output.toByteArray())
        assertEquals(2, frames.size)
        assertEquals(medium, String(frames[0].payload, Charsets.UTF_8))
        assertEquals(large, String(frames[1].payload, Charsets.UTF_8))
    }

    @Test
    fun repliesToPingWithMaskedPong() {
        val frames = concat(
            serverFrame(OPCODE_PING, "ping!".toByteArray()),
            serverFrame(OPCODE_TEXT, "after".toByteArray()),
            serverFrame(OPCODE_CLOSE, closePayload()),
        )
        val (socket, output) = socketFor(serverBytes = frames)
        socket.upgrade()

        assertEquals(listOf("after"), runBlocking { socket.messages.toList() })
        val clientFrames = parseClientFrames(output.toByteArray())
        assertEquals(OPCODE_PONG, clientFrames[0].opcode)
        assertTrue(clientFrames[0].fin)
        assertEquals("ping!", String(clientFrames[0].payload, Charsets.UTF_8))
    }

    @Test
    fun closeFrameIsTerminalEofAndIsAnswered() {
        val frames = concat(
            serverFrame(OPCODE_TEXT, "bye".toByteArray()),
            serverFrame(OPCODE_CLOSE, closePayload()),
        )
        val (socket, output) = socketFor(serverBytes = frames)
        socket.upgrade()

        assertEquals(listOf("bye"), runBlocking { socket.messages.toList() })
        val clientFrames = parseClientFrames(output.toByteArray())
        assertEquals(OPCODE_CLOSE, clientFrames.last().opcode)
    }

    @Test
    fun rejectsInvalidUtf8TextFrame() {
        val frames = serverFrame(OPCODE_TEXT, byteArrayOf(0xC3.toByte(), 0x28))
        val (socket, _) = socketFor(serverBytes = frames)
        socket.upgrade()

        val error = runCatching { socket.readMessage() }.exceptionOrNull()

        assertTrue(error is AppServerException.AppServerConnectionLost)
    }

    @Test
    fun sendTextAfterCloseThrows() {
        val (socket, _) = socketFor()
        socket.upgrade()
        socket.close()

        assertThrows(AppServerException.AppServerConnectionLost::class.java) {
            runBlocking { socket.sendText("nope") }
        }
    }

    private fun socketFor(
        seed: Long = SEED,
        serverBytes: ByteArray = ByteArray(0),
        status: Int = 101,
        accept: String? = null,
    ): Pair<WebSocketOverStdio, ByteArrayOutputStream> {
        val response = upgradeResponse(status, accept ?: WebSocketOverStdio.acceptValue(keyForSeed(seed)))
        val input = ByteArrayInputStream(response.toByteArray(Charsets.ISO_8859_1) + serverBytes)
        val output = ByteArrayOutputStream()
        return WebSocketOverStdio(input, output, Random(seed)) to output
    }

    private fun upgradeResponse(status: Int, accept: String): String =
        "HTTP/1.1 $status ${if (status == 101) "Switching Protocols" else "Error"}\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: $accept\r\n" +
            "\r\n"

    private fun keyForSeed(seed: Long): String {
        val random = Random(seed)
        val bytes = ByteArray(16)
        random.nextBytes(bytes)
        return Base64.getEncoder().encodeToString(bytes)
    }

    private fun serverFrame(opcode: Int, payload: ByteArray, fin: Boolean = true): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(if (fin) 0x80 or opcode else opcode)
        val length = payload.size
        when {
            length < 126 -> out.write(length)
            length <= 0xFFFF -> {
                out.write(126)
                out.write(length ushr 8)
                out.write(length and 0xFF)
            }

            else -> {
                out.write(127)
                for (shift in 7 downTo 0) {
                    out.write(((length.toLong() ushr (8 * shift)) and 0xFF).toInt())
                }
            }
        }
        out.write(payload)
        return out.toByteArray()
    }

    private fun parseClientFrames(bytes: ByteArray): List<ClientFrame> {
        val frames = mutableListOf<ClientFrame>()
        var index = headerEnd(bytes)
        while (index < bytes.size) {
            val first = bytes[index].toInt() and 0xFF
            val second = bytes[index + 1].toInt() and 0xFF
            var length = second and 0x7F
            var cursor = index + 2
            when (length) {
                126 -> {
                    length = ((bytes[cursor].toInt() and 0xFF) shl 8) or (bytes[cursor + 1].toInt() and 0xFF)
                    cursor += 2
                }

                127 -> {
                    var value = 0L
                    repeat(8) { shift ->
                        value = (value shl 8) or (bytes[cursor + shift].toInt() and 0xFF).toLong()
                    }
                    length = value.toInt()
                    cursor += 8
                }
            }
            val masked = second and 0x80 != 0
            val mask = ByteArray(4)
            if (masked) {
                System.arraycopy(bytes, cursor, mask, 0, 4)
                cursor += 4
            }
            val payload = ByteArray(length) { offset ->
                (bytes[cursor + offset].toInt() xor (if (masked) mask[offset % 4].toInt() else 0)).toByte()
            }
            frames += ClientFrame(first and 0x80 != 0, first and 0x0F, payload)
            index = cursor + length
        }
        return frames
    }

    private fun headerEnd(bytes: ByteArray): Int {
        for (index in 0 until bytes.size - 3) {
            if (bytes[index] == '\r'.code.toByte() &&
                bytes[index + 1] == '\n'.code.toByte() &&
                bytes[index + 2] == '\r'.code.toByte() &&
                bytes[index + 3] == '\n'.code.toByte()
            ) {
                return index + 4
            }
        }
        error("Upgrade request header terminator not found")
    }

    private fun concat(vararg arrays: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        arrays.forEach(out::write)
        return out.toByteArray()
    }

    private fun closePayload() = byteArrayOf(0x03, 0xE8.toByte())

    private data class ClientFrame(val fin: Boolean, val opcode: Int, val payload: ByteArray)

    private companion object {
        const val SEED = 7L
        const val OPCODE_CONTINUATION = 0
        const val OPCODE_TEXT = 1
        const val OPCODE_CLOSE = 8
        const val OPCODE_PING = 9
        const val OPCODE_PONG = 10
    }
}

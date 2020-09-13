package im.toss.util.cache.spring.webmvc

import org.springframework.http.HttpHeaders
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import javax.servlet.ServletOutputStream
import javax.servlet.WriteListener
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponseWrapper

class CaptureHttpServletResponse(response: HttpServletResponse) : HttpServletResponseWrapper(response) {
    private val capture = ByteArrayOutputStream(response.bufferSize)
    private var output: ServletOutputStream? = null
    private var writer: PrintWriter? = null
    private val headers: HttpHeaders = HttpHeaders()
    private var captureMode = false

    val isCaptureMode: Boolean get() = captureMode

    fun activateCaptureMode() {
        check(writer == null) { "getWriter() has already been called on this response." }
        check(output == null) { "getOutputStream() has already been called on this response." }

        captureMode = true
    }

    override fun addHeader(name: String, value: String) {
        if (captureMode) {
            headers.add(name, value)
        }
        super.addHeader(name, value)
    }

    override fun getOutputStream(): ServletOutputStream {
        check(writer == null) { "getWriter() has already been called on this response." }
        return if (captureMode) {
            output ?: object : ServletOutputStream() {
                override fun write(b: Int) = capture.write(b)
                override fun flush() = capture.flush()
                override fun close() = capture.close()
                override fun isReady(): Boolean = false
                override fun setWriteListener(arg0: WriteListener) {}
            }.also { output = it }
        } else super.getOutputStream()
    }

    override fun getWriter(): PrintWriter {
        check(output == null) { "getOutputStream() has already been called on this response." }
        return if (captureMode) {
            if (writer == null) {
                writer = PrintWriter(OutputStreamWriter(capture, characterEncoding))
            }
            writer!!
        } else super.getWriter()
    }

    override fun flushBuffer() {
        if (captureMode) {
            writer?.flush()
            output?.flush()
        } else super.flushBuffer()
    }

    val capturedResponseBody: ByteArray by lazy {
        check(captureMode) { "capture mode is not activated" }
        writer?.close()
        output?.close()
        capture.toByteArray()
    }

    val capturedHeaders: HttpHeaders by lazy {
        check(captureMode) { "capture mode is not activated" }
        headers
    }
}
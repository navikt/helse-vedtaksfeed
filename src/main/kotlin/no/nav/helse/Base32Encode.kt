package no.nav.helse

import org.apache.commons.codec.binary.Base32
import java.nio.ByteBuffer
import java.util.*

private const val pad = '='
private const val padByte = '='.code.toByte()

internal fun UUID.base32Encode(): String {
    return Base32(padByte)
        .encodeAsString(this.byteArray())
        .replace(pad.toString(), "")
}

private fun UUID.byteArray() = ByteBuffer.allocate(Long.SIZE_BYTES * 2).apply {
    putLong(this@byteArray.mostSignificantBits)
    putLong(this@byteArray.leastSignificantBits)
}.array()

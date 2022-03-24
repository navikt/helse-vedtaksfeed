package no.nav.helse

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

internal class Base32EncodeTest {

    @Test
    fun `encode uuid til base32`() {
        val uuid = UUID.fromString("7b0e2b09-1100-4790-90e5-7b30d7a48a13")
        assertEquals("PMHCWCIRABDZBEHFPMYNPJEKCM", uuid.base32Encode())
    }
}

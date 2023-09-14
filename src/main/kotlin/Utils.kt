import io.netty.buffer.ByteBuf
import storage.ObjectIdentifier
import java.nio.charset.StandardCharsets

fun getTimeMillis():Long {
    return System.currentTimeMillis() - 1640995200000L // 1/1/2022
}


fun encodeUTF8(str: String, out: ByteBuf) {
    val stringBytes = str.toByteArray(StandardCharsets.UTF_8)
    out.writeInt(stringBytes.size)
    out.writeBytes(stringBytes)
}

fun decodeUTF8(buff: ByteBuf): String {
    val stringBytes = ByteArray(buff.readInt())
    buff.readBytes(stringBytes)
    return String(stringBytes, StandardCharsets.UTF_8)
}


fun serializeString(s: String, byteBuf: ByteBuf) {
    val byteArray = s.toByteArray(Charsets.UTF_8)
    byteBuf.writeInt(byteArray.size)
    byteBuf.writeBytes(byteArray)
}

fun deserializeString(byteBuf: ByteBuf): String {
    val size = byteBuf.readInt()
    val stringBytes = ByteArray(size)
    byteBuf.readBytes(stringBytes)
    return String(stringBytes, Charsets.UTF_8)
}
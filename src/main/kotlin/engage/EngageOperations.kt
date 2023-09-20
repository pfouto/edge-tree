package engage

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.network.ISerializer

data class ReadOperation(val partition: String, val key: String) : Operation(READ)

data class WriteOperation(
    val partition: String,
    val key: String,
    val value: ByteArray,
) : Operation(WRITE) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as WriteOperation

        if (partition != other.partition) return false
        if (key != other.key) return false
        return (!value.contentEquals(other.value))
    }

    override fun hashCode(): Int {
        var result = partition.hashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + value.contentHashCode()
        return result
    }

    override fun toString(): String {
        return "WriteOperation($partition:$key, value=${value.size}})"
    }

}

abstract class Operation(val type: Short) {

    companion object {
        const val READ: Short = 1
        const val WRITE: Short = 2
    }

    object Serializer : ISerializer<Operation> {
        override fun serialize(msg: Operation, out: ByteBuf) {
            out.writeShort(msg.type.toInt())
            when (msg) {
                is ReadOperation -> {
                    encodeUTF8(msg.partition, out)
                    encodeUTF8(msg.key, out)
                }

                is WriteOperation -> {
                    encodeUTF8(msg.partition, out)
                    encodeUTF8(msg.key, out)
                    out.writeInt(msg.value.size)
                    out.writeBytes(msg.value)
                }
            }
        }

        override fun deserialize(buff: ByteBuf): Operation {
            return when (val type = buff.readShort()) {
                READ -> {
                    val partition = decodeUTF8(buff)
                    val key = decodeUTF8(buff)
                    ReadOperation(partition, key)
                }

                WRITE -> {
                    val partition = decodeUTF8(buff)
                    val key = decodeUTF8(buff)
                    val valueSize = buff.readInt()
                    val value = ByteArray(valueSize)
                    buff.readBytes(value)
                    WriteOperation(partition, key, value)
                }

                else -> throw IllegalArgumentException("Unknown operation type: $type")
            }
        }
    }

}
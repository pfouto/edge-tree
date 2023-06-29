package proxy.utils

import decodeUTF8
import encodeUTF8
import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

data class ReadOperation(val partition: String, val key: String) : Operation(READ)

data class WriteOperation(
    val partition: String,
    val key: String,
    val value: ByteArray,
    val persistence: Short,
) : Operation(WRITE) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as WriteOperation

        if (partition != other.partition) return false
        if (key != other.key) return false
        if (!value.contentEquals(other.value)) return false
        return persistence == other.persistence
    }

    override fun hashCode(): Int {
        var result = partition.hashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + value.contentHashCode()
        result = 31 * result + persistence
        return result
    }

    override fun toString(): String {
        return "WriteOperation($partition:$key, value=${value.size}}, persistence=$persistence)"
    }

}

data class MigrationOperation(val hlc: HybridTimestamp, val path: List<Host>) : Operation(MIGRATION)

data class PartitionFetchOperation(val partition: String) : Operation(PARTITION_FETCH)

abstract class Operation(val type: Short) {

    companion object {
        const val READ: Short = 1
        const val WRITE: Short = 2
        const val MIGRATION: Short = 3
        const val PARTITION_FETCH: Short = 4
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
                    out.writeShort(msg.persistence.toInt())
                }

                is MigrationOperation -> {
                    HybridTimestamp.Serializer.serialize(msg.hlc, out)
                    out.writeInt(msg.path.size)
                    msg.path.forEach { Host.serializer.serialize(it, out) }
                }

                is PartitionFetchOperation -> {
                    encodeUTF8(msg.partition, out)
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
                    val persistence = buff.readShort()
                    WriteOperation(partition, key, value, persistence)
                }

                MIGRATION -> {
                    val hlc = HybridTimestamp.Serializer.deserialize(buff)
                    val pathSize = buff.readInt()
                    val path = mutableListOf<Host>()
                    repeat(pathSize) { path.add(Host.serializer.deserialize(buff)) }
                    MigrationOperation(hlc, path)
                }

                PARTITION_FETCH -> {
                    val partition = decodeUTF8(buff)
                    PartitionFetchOperation(partition)
                }

                else -> throw IllegalArgumentException("Unknown operation type: $type")
            }
        }
    }

}
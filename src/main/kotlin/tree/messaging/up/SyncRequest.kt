package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class SyncRequest(
    val upstream: Upstream,
    val partitions: Set<Pair<String, String>>,
) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 205
    }

    override fun toString(): String {
        return "SyncRequest($upstream, partitions=$partitions)"
    }

    object Serializer : ISerializer<SyncRequest> {
        override fun serialize(msg: SyncRequest, out: ByteBuf) {
            Upstream.Serializer.serialize(msg.upstream, out)
            out.writeInt(msg.partitions.size)
            for (item in msg.partitions) {
                val keyBytes = item.first.encodeToByteArray()
                out.writeInt(keyBytes.size)
                out.writeBytes(keyBytes)
                val valueBytes = item.second.encodeToByteArray()
                out.writeInt(valueBytes.size)
                out.writeBytes(valueBytes)
            }
        }

        override fun deserialize(buff: ByteBuf): SyncRequest {
            val upstream = Upstream.Serializer.deserialize(buff)
            val nPartitions = buff.readInt()
            val partitions = mutableSetOf<Pair<String, String>>()
            for (i in 0 until nPartitions) {
                val keyBytes = ByteArray(buff.readInt())
                buff.readBytes(keyBytes)
                val key = String(keyBytes)
                val valueBytes = ByteArray(buff.readInt())
                buff.readBytes(valueBytes)
                val value = String(valueBytes)
                partitions.add(Pair(key, value))
            }
            return SyncRequest(upstream, partitions)
        }
    }

}
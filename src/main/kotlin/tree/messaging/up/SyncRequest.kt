package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer

data class SyncRequest(
    val upstream: Upstream,
    val partitions: List<String>,
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
            for (s: String in msg.partitions) {
                val stringBytes = s.encodeToByteArray()
                out.writeInt(stringBytes.size)
                out.writeBytes(stringBytes)
            }
        }

        override fun deserialize(buff: ByteBuf): SyncRequest {
            val upstream = Upstream.Serializer.deserialize(buff)
            val nPartitions = buff.readInt()
            val list = mutableListOf<String>()
            for (i in 0 until nPartitions) {
                val stringBytes = ByteArray(buff.readInt())
                buff.readBytes(stringBytes)
                list.add(String(stringBytes))
            }
            return SyncRequest(upstream, list)
        }
    }

}
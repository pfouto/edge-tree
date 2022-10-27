package tree.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

data class SyncResponse(
    val stableTS: HybridTimestamp,
    val parents: List<Pair<Host, HybridTimestamp>>,
    val data: ByteArray,
) : ProtoMessage(ID) {

    companion object {
        const val ID: Short = 201
    }

    override fun toString(): String {
        return "SyncResponse(stableTS=$stableTS, parents=$parents, data=${data.size})"
    }

    object Serializer : ISerializer<SyncResponse> {
        override fun serialize(msg: SyncResponse, out: ByteBuf) {
            HybridTimestamp.Serializer.serialize(msg.stableTS, out)
            out.writeInt(msg.parents.size)
            for (p in msg.parents) {
                Host.serializer.serialize(p.first, out)
                HybridTimestamp.Serializer.serialize(p.second, out)
            }
            out.writeInt(msg.data.size)
            out.writeBytes(msg.data)
        }

        override fun deserialize(buff: ByteBuf): SyncResponse {
            val ts = HybridTimestamp.Serializer.deserialize(buff)
            val nParents = buff.readInt()
            val parents = mutableListOf<Pair<Host, HybridTimestamp>>()
            for (i in 0 until nParents) {
                val host = Host.serializer.deserialize(buff)
                val ts = HybridTimestamp.Serializer.deserialize(buff)
                parents.add(Pair(host, ts))
            }
            val data = ByteArray(buff.readInt())
            buff.readBytes(data)
            return SyncResponse(ts, parents, data)
        }
    }

}
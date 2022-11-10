package tree.utils.messaging

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

data class Downstream(val stableTS: HybridTimestamp, val parents: List<Pair<Host, HybridTimestamp>>) :
    ProtoMessage(ID) {

    companion object {
        const val ID: Short = 203
    }

    override fun toString(): String {
        return "Downstream(stableTS=$stableTS, parents=$parents)"
    }

    object Serializer : ISerializer<Downstream> {
        override fun serialize(msg: Downstream, out: ByteBuf) {
            HybridTimestamp.Serializer.serialize(msg.stableTS, out)
            out.writeInt(msg.parents.size)
            for (p in msg.parents) {
                Host.serializer.serialize(p.first, out)
                HybridTimestamp.Serializer.serialize(p.second, out)
            }
        }

        override fun deserialize(buff: ByteBuf): Downstream {
            val ts = HybridTimestamp.Serializer.deserialize(buff)
            val nParents = buff.readInt()
            val parents = mutableListOf<Pair<Host, HybridTimestamp>>()
            for (i in 0 until nParents) {
                val host = Host.serializer.deserialize(buff)
                val ts = HybridTimestamp.Serializer.deserialize(buff)
                parents.add(Pair(host, ts))
            }
            return Downstream(ts, parents)
        }
    }

}
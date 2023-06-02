package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host
import tree.utils.HybridTimestamp

class Reconfiguration(val grandparents: List<Host>, val timestamps: List<HybridTimestamp>) : ProtoMessage(ID){

    companion object {
        const val ID: Short = 207
    }

    override fun toString(): String {
        return "Reconfiguration($grandparents, $timestamps)"
    }

    object Serializer : ISerializer<Reconfiguration> {
        override fun serialize(msg: Reconfiguration, out: ByteBuf) {
            out.writeInt(msg.grandparents.size)
            for (p in msg.grandparents) {
                Host.serializer.serialize(p, out)
            }

            out.writeInt(msg.timestamps.size)
            for (ts in msg.timestamps) {
                HybridTimestamp.Serializer.serialize(ts, out)
            }
        }

        override fun deserialize(buff: ByteBuf): Reconfiguration {
            val nParents = buff.readInt()
            val parents = mutableListOf<Host>()
            for (i in 0 until nParents) {
                val host = Host.serializer.deserialize(buff)
                parents.add(host)
            }
            val nTimestamps = buff.readInt()
            val timestamps = mutableListOf<HybridTimestamp>()
            for (i in 0 until nTimestamps) {
                val ts = HybridTimestamp.Serializer.deserialize(buff)
                timestamps.add(ts)
            }
            return Reconfiguration(parents, timestamps)
        }
    }

}
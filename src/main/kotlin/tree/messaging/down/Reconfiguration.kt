package tree.messaging.down

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import pt.unl.fct.di.novasys.network.data.Host

class Reconfiguration(val grandparents: List<Host>, val downstream: DownstreamMetadata) : ProtoMessage(ID){

    companion object {
        const val ID: Short = 207
    }

    override fun toString(): String {
        return "Reconfiguration(grandparents=$grandparents, $downstream)"
    }

    object Serializer : ISerializer<Reconfiguration> {
        override fun serialize(msg: Reconfiguration, out: ByteBuf) {
            out.writeInt(msg.grandparents.size)
            for (p in msg.grandparents) {
                Host.serializer.serialize(p, out)
            }
            DownstreamMetadata.Serializer.serialize(msg.downstream, out)
        }

        override fun deserialize(buff: ByteBuf): Reconfiguration {
            val nParents = buff.readInt()
            val parents = mutableListOf<Host>()
            for (i in 0 until nParents) {
                val host = Host.serializer.deserialize(buff)
                parents.add(host)
            }
            val downstream = DownstreamMetadata.Serializer.deserialize(buff)
            return Reconfiguration(parents, downstream)
        }
    }

}
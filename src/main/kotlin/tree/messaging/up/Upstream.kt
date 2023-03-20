package tree.messaging.up

import io.netty.buffer.ByteBuf
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage
import pt.unl.fct.di.novasys.network.ISerializer
import tree.utils.HybridTimestamp

data class Upstream(val ts: HybridTimestamp): ProtoMessage(ID) {
    companion object {
        const val ID: Short = 206
    }

    override fun toString(): String {
        return "Upstream(ts=$ts)"
    }
    object Serializer : ISerializer<Upstream> {
        override fun serialize(obj: Upstream, buffer: ByteBuf) {
            HybridTimestamp.Serializer.serialize(obj.ts, buffer)
        }

        override fun deserialize(buffer: ByteBuf): Upstream {
            val ts = HybridTimestamp.Serializer.deserialize(buffer)
            return Upstream(ts)
        }
    }
}